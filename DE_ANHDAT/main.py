import time

from pyspark import SparkContext, SparkConf
from random import Random

conf = SparkConf().setAppName("Dat dep zai").setMaster("local[*]").set("spark.executor.memory","4g")

sc = SparkContext(conf=conf)

data = ["Dat", "Golden", "Heu kkk", "Sami"]
# dat:123123, golden:1231233
rdd = sc.parallelize(data)

# def numsPartition(iterator):
#     # create 1 nums for map Partition data
#     rand = Random(int(time.time()*1000) + Random().randint(0,1000))
#     return [f"{item}:{rand.randint(0,1000)}" for item in iterator]

result = rdd.mapPartitions(
    lambda item: map(
        lambda l: f"{l}:{Random(int(time.time()*1000)+Random().randint(0,1000)).randint(0,1000)}", item)
)
print(result.collect())