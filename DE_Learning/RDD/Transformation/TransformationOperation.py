from pyspark import SparkContext, SparkConf
conf = SparkConf().setMaster("local[*]").setAppName("new").set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)

data = [1,2,3,4,5,6,7,8,9,10]

rdd = sc.parallelize(data)

square = rdd.map(lambda x: (x,x*2))
print(square.collect())