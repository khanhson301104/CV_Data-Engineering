from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder.getOrCreate()

# MySQL Connection Properties
jdbc_url = "jdbc:mysql://localhost:3306/etl_data"
table_name = "employees"
db_properties = {
    "user": "root",
    "password": "1",
    "driver": "com.mysql.cj.jdbc.Driver"
}
# Read MySQL Table into Spark DataFrame
df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=db_properties)

# Show Data
df.show()
