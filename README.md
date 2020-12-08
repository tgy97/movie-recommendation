# movie-recommendation


## 下载镜像
```
docker pull wavey/moive-recommendation:latest
```
## 启动多个容器及内部的hdfs与spark
```
sh start_contrainer.sh
```
## 进入hadoop-node1 container后，运行代码
```
spark-submit --master spark://Hadoop-node1:7077 ../test/code.py
```