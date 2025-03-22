from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql.functions import struct
# Initialize SparkSession
spark = SparkSession.builder \
    .appName("DE") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()


# data = spark.read.json(r"C:\Users\Lenovo\Downloads\2015-03-01-17.json")

# data.withColumn("id2", lit("datdepzai")).select(col("id"), col("id2")).show()
# data.withColumn("actor.id2", lit("kson")).select(col("id"), col("actor.id2")).show()

# jsondata = data.withColumn(
#     "actor",
#     struct(
#         col("actor.id").alias("id"),
#         col("actor.login").alias("login"),
#         col("actor.gravatar_id").alias("gravatar_id"),
#         col("actor.url").alias("url"),
#         col("actor.avatar_url").alias("avatar_url"),
#         lit("kson").alias("id2")
#     )
# ).select(col("actor.id"), col("actor.id2"))
#
# def kson(data):
#     return data+3
#
# # function UDF: user define: nguoi dung xac dinh
from pyspark.sql.functions import udf
#
# kson2 = udf(kson)
#
# df = data.withColumn("helo", kson2(col("actor.id")))
# df.select(col("actor.id"), col("helo")).show()

def transform_data(data):
    new_data = data.replace(".", "/").replace("-", "/").split("/")
    if len(new_data[0]) == 4:
        year, month, day = new_data
    else:
        day, month, year = new_data

    return day, month, year
data2 = [("11/12/2025",), ("27/02.2014",), ("2023.01.09",), ("28-12-2025",)]
df = spark.createDataFrame(data2, ["date"])
rows = df.collect()
lst_day = []
lst_month = []
lst_year = []
for row in rows:
    day, month, year = transform_data(row["date"])
    lst_day.append(day)
    lst_month.append(month)
    lst_year.append(year)

new_data = list(zip(lst_day, lst_month, lst_year))  # Convert lists into tuples
df_new = spark.createDataFrame(new_data, ["day", "month", "year"])
df_new.show()