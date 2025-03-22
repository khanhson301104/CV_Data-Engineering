from pyspark import SparkConf, SparkContext

sc = SparkContext("local[2]", "DE")
data = [1,2,3,4,5,7]
rdd = sc.parallelize(data)

print(rdd.take(2))