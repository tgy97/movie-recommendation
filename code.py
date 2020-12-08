#from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS
import time

def recommend_for_user(model, user, movieTitle, n = 5):
    recommandP = model.recommendProducts(user, n)
    for p in recommandP:
        print('Recommand moive: <<'+ movieTitle[str(p[1])].encode('utf-8') + '>> for user ' + str(p[0]))

def recommend_for_moive(model, movie, movieTitle, n = 5):
    recommandP = model.recommendUsers(movie, n)
    for p in recommandP:
        print('Recommand moive: <<'+ movieTitle[str(p[1])].encode('utf-8') + '>> for user ' + str(p[0]))
if __name__ == "__main__":
    use_hdfs = True
    dataset = '1m'
    # spark = SparkSession\
    #         .builder\
    #         .appName("movie_recommendation")\
    #         .master("spark://hadoop-node1:7077") \
    #         .getOrCreate()

    # #spark.conf.set("spark.executor.memory", "500M")
    # sc = spark.sparkContext
    # #sc.setLogLevel("ERROR")
    # # print(sc.textFile("hdfs://hadoop-node1:9000/input/NOTICE.txt").count())
    sc = SparkContext("spark://hadoop-node1:7077")
    if use_hdfs:
        if dataset == '1m':
            text = sc.textFile("hdfs://hadoop-node1:9000/ml-latest-small/ratings.csv")
            movies = sc.textFile("hdfs://hadoop-node1:9000/ml-latest-small/movies.csv")
            split_dot = ","
        elif dataset == '10m':
            text = sc.textFile("hdfs://hadoop-node1:9000/ml-10M100K/ratings.dat")
            movies = sc.textFile("hdfs://hadoop-node1:9000/ml-10M100K/movies.dat")
            split_dot = "::"
    else:
        text = sc.textFile("file:///usr/local/test/ml-latest-small/ratings.csv")
        movies = sc.textFile("file:///usr/local/test/ml-latest-small/movies.csv")
    text = text.filter(lambda x: "movieId" not in x)    

    text_train, text_test = text.randomSplit((0.9,0.1))

    movieRatings = text_train.map(lambda x: x.split(split_dot)[:3])


    ground = text_test.map(lambda x: (x.split(split_dot)[:2],x.split(split_dot)[2]))

    
    print('Begin training')
    t = time.time()
    model = ALS.train(movieRatings, 10, 5, 0.01)
    print('Finished training')
    print('Train time cost: ' + str(time.time()-t) + 's')


    movies = movies.filter(lambda x: "movieId" not in x)
    movieTitle = movies.map(lambda x: x.split(",")[: 2]).collectAsMap()
    print('Search movie for user 1')
    recommend_for_user(model, user = 1, movieTitle = movieTitle, n = 5)
    print('Search user for movie 10')
    recommend_for_moive(model, movie = 10, movieTitle = movieTitle, n = 5)

    #pred = model.predictAll(text_test.map(lambda x: x.split(split_dot)[0:2]))
    # pred = model.predictAll(ground.map(lambda x: x[0]))


    # print(pred.take(2))
    # pred = pred.map(lambda x : ([x[0],x[1]],x[2]))
    # print(pred.take(2))
    # print(ground.map(lambda x: x[0]).take(2))

    # pred = pred.join(ground)
    # print(pred.take(2))





    # recommandP = model.recommendProducts(1, 5)
    # print('finished recommendation')
    # movies = movies.filter(lambda x: "movieId" not in x)
    # movieTitle = movies.map(lambda x: x.split(",")[: 2]).collectAsMap()

    # for p in recommandP:
    #     print('Recommand moive: '+ str(movieTitle[str(p[1])]) + ' for user ' + str(p[0]))





                                                                             
