from pyspark.sql.functions import col
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("DE") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()


data = spark.read.json(r"C:\Users\Lenovo\Downloads\2015-03-01-17.json")

from pyspark.sql.functions import col

data.select(
    col("id"),
    col("type"),
    col("actor.login"),
    col("repo.name"),
    col("payload.issue.title"),
    col("payload.issue.state"),
    col("payload.issue.created_at"),
    col("actor.id").alias("actor_id")
).show()
# from pyspark.sql.functions import length
#
# data.select("id").filter(length(col("id"))<11).show()

