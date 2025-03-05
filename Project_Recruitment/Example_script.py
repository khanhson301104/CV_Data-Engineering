import findspark
findspark.init()

from pyspark.sql import SparkSession
from cassandra.util import datetime_from_uuid1
from cassandra.cqltypes import TimeUUIDType
import uuid
from uuid import UUID
import time_uuid
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, col
import pyspark.sql.functions as sf

spark = SparkSession.builder \
    .appName("CassandraTest") \
    .getOrCreate()

# Load data from Cassandra
data = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="tracking", keyspace="logs") \
    .load()

def process_df(df):
    # UDF to convert TimeUUID to datetime string
    @udf(returnType=StringType())
    def to_datetime_str(x):
        return time_uuid.TimeUUID(bytes=UUID(x).bytes).get_datetime().strftime('%Y-%m-%d %H:%M:%S')
    # Apply the UDF to create a new column with the converted datetime
    df_with_time = df.withColumn('normal_time', to_datetime_str(col('create_time')))
    # Select required columns and collect the result
    time_data = df_with_time.select('create_time', 'normal_time').collect()
    # Create lists using list comprehensions
    spark_timeuuid = [row.create_time for row in time_data]
    normal_time = [row.normal_time for row in time_data]
    # Create DataFrame from TimeUUID and normal time
    time_data_df = spark.createDataFrame(zip(spark_timeuuid, normal_time), ['create_time', 'ts'])
    # Join original DataFrame with time_data_df
    result = df.join(time_data_df, ['create_time'], 'inner').drop(df.ts)
    # Select required columns
    result = result.select('create_time', 'ts', 'job_id', 'custom_track', 'bid', 'campaign_id', 'group_id', 'publisher_id')
    return result

df = process_df(data)
df= df.cache()

data = df

click_data = data.filter(data.custom_track == 'click')
click_data.createTempView('clickdata')
click_data = spark.sql("""select date(ts) as date,hour(ts) as hour ,job_id,publisher_id,campaign_id,group_id,round(avg(bid),2) as bid_set , sum(bid) as spend_hour , count(*) as click from clickdata group by date(ts),
hour(ts),job_id,publisher_id,campaign_id,group_id""")

#process conversion data
conversion_data = data.filter(data.custom_track == 'conversion')
conversion_data.createTempView('conversiondata')
conversion_data = spark.sql("""select date(ts) as date,hour(ts) as hour ,job_id,publisher_id,campaign_id,group_id, count(*) as conversion from conversiondata group by date(ts),
hour(ts),job_id,publisher_id,campaign_id,group_id""")
#process qualified data
qualified_data = data.filter(data.custom_track == 'qualified')
qualified_data.createTempView('qualified')
qualified_data = spark.sql("""select date(ts) as date,hour(ts) as hour ,job_id,publisher_id,campaign_id,group_id, count(*) as qualified from qualified group by date(ts),
hour(ts),job_id,publisher_id,campaign_id,group_id""")
#process unqualified data
unqualified_data = data.filter(data.custom_track == 'unqualified')
unqualified_data.createTempView('unqualified')
unqualified_data = spark.sql("""select date(ts) as date,hour(ts) as hour ,job_id,publisher_id,campaign_id,group_id, count(*) as unqualified from unqualified group by date(ts),
hour(ts),job_id,publisher_id,campaign_id,group_id""")
#filter null data
click_data = click_data.filter(click_data.date.isNotNull())
conversion_data = conversion_data.filter(conversion_data.date.isNotNull())
qualified_data = qualified_data.filter(qualified_data.date.isNotNull())
unqualified_data = unqualified_data.filter(unqualified_data.date.isNotNull())

#finalize output full join
result = (click_data.
    join(conversion_data, on = ['date','hour','job_id','publisher_id','campaign_id','group_id'],how='full').\
        join(qualified_data,on = ['date','hour','job_id','publisher_id','campaign_id','group_id'],how='full').\
            join(unqualified_data,on = ['date','hour','job_id','publisher_id','campaign_id','group_id'],how='full'))



click_data.show()

# url = 'jdbc:mysql://' + 'localhost' + ':' + '3306' + '/' + 'etl_data'
# driver = "com.mysql.cj.jdbc.Driver"
# user = 'root'
# password = '1'
# sql = '(SELECT id as job_id, company_id, group_id, campaign_id FROM job) A'
# jobs = spark.read.format('jdbc').options(url = url , driver = driver , dbtable = sql , user=user , password = password).load()
# result.show()