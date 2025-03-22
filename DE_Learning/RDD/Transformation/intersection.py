from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE").setMaster("local[*]").set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)

data1 = sc.parallelize(["one", 1,2, 3, "two","three", "two", 2,3 ])
data2 = sc.parallelize([1,2,3,4,5,7,"two", "three"])

data3 = data2.intersection(data1)
print(data3.collect())