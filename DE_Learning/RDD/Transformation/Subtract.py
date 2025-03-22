from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE").setMaster("local[*]").set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)

word = sc.parallelize(["Chi nhE CaI rAnG cua cHi sat rA Chi nGhIem TuC cho Toi"])\
    .flatMap(lambda char: char.split(" "))\
    .map(lambda char: char.lower())

dropWord = sc.parallelize(["nhe cai rang con me may ra nghiem cho"])\
            .flatMap(lambda char: char.split(" "))

finalRdd = word.subtract(dropWord)

print(finalRdd.collect())