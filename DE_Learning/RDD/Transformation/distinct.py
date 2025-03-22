from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE").setMaster("local[*]").set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)

data = sc.parallelize(["one", 1,2, 3, "two","three", "two", 2,3 ])
print(data.distinct().collect())