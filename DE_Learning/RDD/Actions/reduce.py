from pyspark import SparkConf, SparkContext

sc = SparkContext("local[*]", "DE")

numbers = sc.parallelize([3,4,65,754,61,3,5,6,65,34],8)

def sum(v1: int, v2: int) -> int:
    print(f"v1:{v1}, v2:{v2} ->({v1+v2})")
    return v1 + v2

print(numbers.glom().collect())