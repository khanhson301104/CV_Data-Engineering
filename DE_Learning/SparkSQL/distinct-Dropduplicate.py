from pyspark.sql.functions import col
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("DE") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()


data = spark.read.json(r"C:\Users\Lenovo\Downloads\2015-03-01-17.json")

# data.select(col("payload.issue.state")).distinct().show()
#
data.select(col("payload.issue.state")).distinct().selectExpr("count(*) as count").show()

# data.select(col("payload.issue.state").alias("state")).dropDuplicates(["state"]).show()


