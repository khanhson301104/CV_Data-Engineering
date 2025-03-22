from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder.appName("MySQL_Connector").getOrCreate()

# Define JDBC connection parameters
url = "jdbc:mysql://localhost:3306/etl_data"
driver = "com.mysql.cj.jdbc.Driver"
user = "root"
password = "1"

# Fix: Use a proper subquery with parentheses in `dbtable`
sql = '(SELECT id as job_id, company_id, group_id, campaign_id FROM job) A'
jobs = spark.read.format('jdbc').options(url = url , driver = driver , dbtable = sql , user=user , password = password).load()
jobs.show()