from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE").setMaster("local[*]").set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)


rdd_data = sc.textFile(r"D:\CV_Data Engineering\DE_Learning\Data\temp_data.txt")
print(rdd_data.collect())