import uuid
import findspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import TimestampType, StringType
from cassandra.util import datetime_from_uuid1
from cassandra.cqltypes import TimeUUIDType
import time_uuid
import datetime

findspark.init()

# Create a UDF to convert UUID to timestamp
def uuid_to_timestamp(uuid_str):
    try:
        uuid_obj = uuid.UUID(uuid_str)
        # Get timestamp as a datetime object
        timestamp_seconds = time_uuid.TimeUUID(bytes=uuid_obj.bytes).get_timestamp()
        # Convert to Python datetime (handles the tzinfo issue)
        dt = datetime.datetime.fromtimestamp(timestamp_seconds)
        return dt
    except Exception as e:
        print(f"Error converting UUID: {e}")
        return None

# Register the UDF with the correct return type
uuid_to_timestamp_udf = udf(uuid_to_timestamp, TimestampType())

spark = SparkSession.builder.getOrCreate()

data = spark.read.format("org.apache.spark.sql.cassandra").options(table="tracking", keyspace="logs").load()

data = data.withColumn("ts", uuid_to_timestamp_udf(col("create_time")))
data = data.select("ts", 'job_id', 'custom_track', 'bid', 'campaign_id', 'group_id', 'publisher_id')

click_data = data.filter(col("custom_track") == "click")
click_data.createOrReplaceTempView("clickdata")
click_data = spark.sql("Select date(ts) as date, hour(ts) as hour, job_id, campaign_id, group_id, publisher_id, round(avg(bid), 2) as average, sum(bid) as click_result, count(*) as total_click from clickdata group by date(ts), hour(ts),job_id, custom_track, campaign_id, group_id, publisher_id ")

unqualified_data = data.filter(col("custom_track") == "unqualified")
unqualified_data.createOrReplaceTempView("unqualifieddata")
unqualified_data = spark.sql("select date(ts) as date, hour(ts) as hour, job_id, campaign_id, group_id, publisher_id, count(*) as total_unqualified from unqualifieddata group by date(ts), hour(ts),job_id, campaign_id, group_id, publisher_id ")

qualified_data = data.filter(col("custom_track") == "qualified")
qualified_data.createOrReplaceTempView("qualifieddata")
qualified_data = spark.sql("select date(ts) as date, hour(ts) as hour, job_id, campaign_id, group_id, publisher_id, count(*) as total_qualified from qualifieddata group by date(ts), hour(ts),job_id, campaign_id, group_id, publisher_id")

conversion_data = data.filter(col("custom_track") == "conversion")
conversion_data.createOrReplaceTempView("conversiondata")
conversion_data = spark.sql("select date(ts) as date, hour(ts) as hour, job_id, campaign_id, group_id, publisher_id, count(*) as total_conversion from conversiondata group by date(ts), hour(ts),job_id, campaign_id, group_id, publisher_id")

click_data = click_data.filter(col("date").isNotNull())
qualified_data = qualified_data.filter(col("date").isNotNull())
unqualified_data = unqualified_data.filter(col("date").isNotNull())
conversion_data = conversion_data.filter(col("date").isNotNull())

result = (click_data.
          join(qualified_data, on=["date", "hour", "job_id", "campaign_id", "group_id", "publisher_id"], how="full")
          .join(unqualified_data, on=["date", "hour", "job_id", "campaign_id", "group_id", "publisher_id"], how="full")
          .join(conversion_data, on=["date", "hour", "job_id", "campaign_id", "group_id", "publisher_id"], how="full"))

result.show()
