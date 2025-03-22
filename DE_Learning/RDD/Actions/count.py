from pyspark import SparkConf, SparkContext

sc = SparkContext("local[2]", "DE")
data = [
    {"id": 1, "name": "SON"},
    {"id": 2, "name": "Long"}
]
rdd = sc.parallelize(data)

print(rdd.count())