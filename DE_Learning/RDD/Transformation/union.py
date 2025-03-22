from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE").setMaster("local[*]").set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)

data1 = [
    {"id": 1, "name": "SON"},
    {"id": 2, "name": "Long"}
]
rdd1 = sc.parallelize(data1)
rdd2 = sc.parallelize([1,2,3,4,5,6,7])
data3 = rdd1.union(rdd2)
print(data3.collect())