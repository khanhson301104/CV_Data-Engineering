from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE").setMaster("local[*]").set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)

fileRDD = sc.textFile(r"D:\CV_Data Engineering\DE_Learning\Data\temp_data.txt")

wordRDD = fileRDD.flatMap(lambda char: char.split(" "))
print(wordRDD.collect())