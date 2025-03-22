from random import Random
import time
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE").setMaster("local[*]").set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)

data = ["ToanNguyen-debt", "Hieu-debt", "Duy-debt", "DucAnh-debt"]

rdd1 = sc.parallelize(data, 2)

# def partition(iterator):
#     rand = Random(int(time.time() * 1000) + Random().randint(0,1000))
#     return [f"{item}: {rand.randint(0,1000)}" for item in iterator]
#

result2 = rdd1.mapPartitions(
    lambda item: map(
        lambda l: f"{l}: {Random(int(time.time() * 1000) + Random().randint(0,1000)).randint(0,1000)}",
        item
    )
)

print(result2.collect())