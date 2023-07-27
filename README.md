# Alibaba Spike

In this repo you will find found some data extraction and analysis over [Alibaba's 2021 microservice calls dataset](https://github.com/alibaba/clusterdata/tree/master/cluster-trace-microservices-v2021).

We used Apache Spark and HDFS to load the dataset onto a cluster of physical nodes.
Then we ran a script that gathers some key stats, and with those stats we plot some relevant information.


## Prerequisites

You need to install the following dependencies before runnning:
- Python 3.7+
- The requisites for our `maestro` script. Simply run `pip install -r requirements.txt`
- Pull submodules: `git submodule update --init --recursive`

Prerequesites for Spark+Hadoop cluster:
- In general you want a Spark cluster with enough capacity for Alibaba's dataset and with enough computing power to run queries on. As a reference, our Spark cluster consisted of 8 machines with between 12 and 16 cores and 128GB RAM each, and a total disk space of 4TB between them.
- Although we provide scripts to provision Spark+Hadoop from scracth (see below) we recommend using an existing tested cluster.

Alibaba Dataset:
Upload the [Alibaba dataset](https://github.com/alibaba/clusterdata/tree/master/cluster-trace-microservices-v2021) to the Hadoop folder on the `/dataset` folder.
    - Use the [Alibaba script](https://github.com/alibaba/clusterdata/blob/master/cluster-trace-microservices-v2021/fetchData.sh) to download the `MSCallGraph` dataset.
    - Unzip all dataset with: `find ./MSCallGraphCSV/ -name 'MSCallGraph_*.tar.gz' | xargs -n1 tar zxvf`
    - On the Hadoop master node, upload dataset to Hadoop: `hadoop fs -put ./MSCallGraphCSV/*.csv /dataset/`


## Usage

Start by setting your variables and inventory file. You can follow the reference for our cluster in `config/gsd-inventory.yml` and `config/gsd-vars.yml`.

Then call maestro to provision the application:
```zsh
./maestro --gsd deploy -inventory configs/gsd-inventory.yml -vars configs/gsd-vars.yml
```
If Spark+Hadoop is not available on the node already consider adding the `-provision` flag.

Then you can gather the key stats from Alibaba dataset using:
```zsh
./maestro --gsd stats
```

With stats in hand you can clean your deployment with:
```zsh
./maestro --gsd clean
```
If you want to stop the cluster use `-stop` and if you want to completely decomission the stack use `-strong`.


## Paper References

João Loff, Daniel Porto, João Garcia, Jonathan Mace, Rodrigo Rodrigues\
Antipode: Enforcing Cross-Service Causal Consistency in Distributed Applications\
To appear.\
[Download]()


Shutian Luo, Huanle Xu, Chengzhi Lu, Kejiang Ye, Guoyao Xu, Liping Zhang, Yu Ding, Jian He, Chengzhong Xu\
Characterizing Microservice Dependency and Performance: Alibaba Trace Analysis\
SoCC'21\
[Download](http://cloud.siat.ac.cn/pdca/socc2021-AlibabaTraceAnalysis.pdf)