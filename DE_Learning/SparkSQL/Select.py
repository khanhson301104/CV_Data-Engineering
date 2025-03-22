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
    col("actor.login").alias("actor_login"),
    col("repo.name").alias("repository"),
    col("payload.issue.title").alias("issue_title"),
    col("payload.issue.state").alias("issue_state"),
    col("payload.issue.created_at").alias("issue_created_at")
).show(truncate=False)
