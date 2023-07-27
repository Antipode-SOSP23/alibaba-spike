from datetime import datetime
from pathlib import Path
from pprint import pprint as pp
import sys
import uuid
import random
import yaml
import os
import sys
import tempfile
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from helpers import *

#--------------
# CONSTANTS
#--------------
SPARK_MASTER = sys.argv[1]
APP_NAME = sys.argv[2]
DATASET_PATH = Path(sys.argv[3])
STATEFUL_RPCTYPES = [
  'mc', # Caches
  'db', # Databases
  'mq', # Message Queues
]
PERCENTILES=[0.20, 0.25, 0.30, 0.40, 0.5, 0.60, 0.75, 0.80, 0.90, 0.95, 0.99]

#--------------
# HELPER
#--------------
def _describe_to_hash(df, col):
  return { r['summary'] : float(r[col]) for r in [ row.asDict() for row in df.describe(col).collect() ] }

#--------------
# SPARK SESSION
#--------------
# start spark session
spark = SparkSession.builder.master(SPARK_MASTER).appName(APP_NAME).getOrCreate()
spark.sparkContext.setLogLevel('WARN')

#--------------
# MAIN
#--------------
print("[INFO] Loading dataset")
# load dataset and rename columns
schema = T.StructType()\
  .add('index', T.StringType(), True)\
  .add('traceid', T.StringType(), True)\
  .add('timestamp', T.LongType(), True)\
  .add('rpcid', T.StringType(), True)\
  .add('um', T.StringType(), True)\
  .add('rpctype', T.StringType(), True)\
  .add('dm', T.StringType(), True)\
  .add('interface', T.StringType(), True)\
  .add('rt', T.StringType(), True)
# Remove duplicates according to: https://github.com/alibaba/clusterdata/issues/124
df = spark.read.option('header', True).schema(schema).csv(str(DATASET_PATH))\
  .drop(*('index', 'interface', 'rt'))\
  .dropDuplicates()
print(f"[INFO] Current count: {df.count()}")

stats = dict()

#--------------
# STATEFULL CHAINS
#--------------
def _chain_size(rpcid,rpcids):
  # sort rpcids by length
  rpcids = sorted(rpcids, key=lambda k: len(k.split('.')), reverse=False)
  # will be atleast one since its the same
  return sum([ 1 if r in rpcid else 0 for r in rpcids ])

# filter all stateful operations and the roots
sf_sequence_df = df.filter(F.col('rpctype').isin(STATEFUL_RPCTYPES))\
  .select('traceid','rpcid').repartition('traceid','rpcid')\
  .withColumn('rpcids', F.collect_list('rpcid').over(Window.partitionBy('traceid').orderBy('rpcid')))\
  .withColumn('sf_chain_size', F.udf(lambda rpcid,rpcids: _chain_size(rpcid, rpcids), T.IntegerType())('rpcid','rpcids'))

# describe stats for chain
stats['sf_chain'] = _describe_to_hash(sf_sequence_df, 'sf_chain_size')

# histogram of those sequences
sf_sequence_hist_df = sf_sequence_df.select('sf_chain_size')\
  .groupBy('sf_chain_size')\
  .agg(F.count('sf_chain_size').alias('count'))\
  .sort('sf_chain_size')

stats['sf_chain_hist'] = [ r.asDict() for r in sf_sequence_hist_df.collect() ]
stats['sf_chain_percentiles'] = dict(zip(PERCENTILES, sf_sequence_df.select(F.percentile_approx('sf_chain_size', PERCENTILES, 1000000).alias('percentiles')).collect()[0]['percentiles']))

#--------------
# SERVICES
#--------------
print("[INFO] Gathering services stats ...")
services_per_df = df.select('traceid','rpctype','dm')\
  .withColumn('meta_rpctype', F.udf(lambda t: 'sf' if t in STATEFUL_RPCTYPES else 'sl')('rpctype'))

###
# count dm per
def _untie_metarpctype(meta_rpctypes):
  if len(meta_rpctypes) > 1:
    if any([t in meta_rpctypes for t in STATEFUL_RPCTYPES]):
      # returns statefull if at least one is
      return 'sf'
    else:
      # only returns stateless if every rpctype was a stateless
      return 'sl'
  else:
    # only one entry return that one
    return meta_rpctypes[0]

count_meta_df = services_per_df.select('dm','meta_rpctype')\
  .distinct()\
  .groupBy('dm')\
  .agg(F.collect_list('meta_rpctype').alias('meta_rpctype'))\
  .withColumn('meta_rpctype', F.udf(lambda l: _untie_metarpctype(l))('meta_rpctype'))\
  .groupBy('meta_rpctype')\
  .agg(F.count('dm').alias('count'))

stats['count_meta'] = { i['meta_rpctype']:i['count'] for i in [ r.asDict() for r in count_meta_df.collect() ] }

###
# unique meta per trace
unique_meta_per_trace_df = services_per_df.select('traceid','meta_rpctype','dm')\
  .distinct()\
  .groupBy('traceid')\
  .agg(
    F.count(F.when(F.col('meta_rpctype') == 'sf', True)).alias('sf_count'),
    F.count(F.when(F.col('meta_rpctype') == 'sl', True)).alias('sl_count'),
  )
stats['unique_meta_per_trace_sf'] = _describe_to_hash(unique_meta_per_trace_df, 'sf_count')
stats['unique_meta_per_trace_sl'] = _describe_to_hash(unique_meta_per_trace_df, 'sl_count')
# hist
unique_sf_per_trace_df = unique_meta_per_trace_df.select('traceid', 'sf_count')\
  .withColumnRenamed('sf_count','count_bin')\
  .groupBy('count_bin')\
  .agg(F.count('count_bin').alias('count'))\
  .sort('count_bin')
stats['unique_sf_per_trace_hist'] = [ r.asDict() for r in unique_sf_per_trace_df.collect() ]
stats['unique_sf_per_trace_percentiles'] = dict(zip(PERCENTILES, unique_meta_per_trace_df.select(F.percentile_approx('sf_count', PERCENTILES, 1000000).alias('percentiles')).collect()[0]['percentiles']))
unique_sl_per_trace_df = unique_meta_per_trace_df.select('traceid', 'sl_count')\
  .withColumnRenamed('sl_count','count_bin')\
  .groupBy('count_bin')\
  .agg(F.count('count_bin').alias('count'))\
  .sort('count_bin')
stats['unique_sl_per_trace_hist'] = [ r.asDict() for r in unique_sl_per_trace_df.collect() ]
stats['unique_sl_per_trace_percentiles'] = dict(zip(PERCENTILES, unique_meta_per_trace_df.select(F.percentile_approx('sl_count', PERCENTILES, 1000000).alias('percentiles')).collect()[0]['percentiles']))

###
# meta per trace
meta_per_trace_df = services_per_df.select('traceid','meta_rpctype','dm')\
  .groupBy('traceid')\
  .agg(
    F.count(F.when(F.col('meta_rpctype') == 'sf', True)).alias('sf_count'),
    F.count(F.when(F.col('meta_rpctype') == 'sl', True)).alias('sl_count'),
  )
stats['meta_per_trace_sf'] = _describe_to_hash(meta_per_trace_df, 'sf_count')
stats['meta_per_trace_sl'] = _describe_to_hash(meta_per_trace_df, 'sl_count')
# hist
sf_per_trace_df = meta_per_trace_df.select('traceid', 'sf_count')\
  .withColumnRenamed('sf_count','count_bin')\
  .groupBy('count_bin')\
  .agg(F.count('count_bin').alias('count'))\
  .sort('count_bin')
stats['sf_per_trace_hist'] = [ r.asDict() for r in sf_per_trace_df.collect() ]
stats['sf_per_trace_percentiles'] = dict(zip(PERCENTILES, meta_per_trace_df.select(F.percentile_approx('sf_count', PERCENTILES, 1000000).alias('percentiles')).collect()[0]['percentiles']))
sl_per_trace_df = meta_per_trace_df.select('traceid', 'sl_count')\
  .withColumnRenamed('sl_count','count_bin')\
  .groupBy('count_bin')\
  .agg(F.count('count_bin').alias('count'))\
  .sort('count_bin')
stats['sl_per_trace_hist'] = [ r.asDict() for r in sl_per_trace_df.collect() ]
stats['sl_per_trace_percentiles'] = dict(zip(PERCENTILES, meta_per_trace_df.select(F.percentile_approx('sl_count', PERCENTILES, 1000000).alias('percentiles')).collect()[0]['percentiles']))

#--------------
# EXPORT
#--------------
# debug
df.printSchema()
df.show()
pp(stats)

print("[INFO] Exporting to YAML ...")
stats_path = Path(f'/tmp/{APP_NAME}.yml')
with open(stats_path, 'w') as outfile:
  yaml.dump(stats, outfile, default_flow_style=False)
print(f"[INFO] Stats exported to {stats_path}")