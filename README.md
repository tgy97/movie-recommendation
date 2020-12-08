# movie-recommendation

```
docker pull wavey/moive-recommendation:latest
```
 
```
sh start_contrainer.sh
```
# (进入hadoop-node1 container后)
```
spark-submit --master spark://Hadoop-node1:7077 ../test/code.py
```