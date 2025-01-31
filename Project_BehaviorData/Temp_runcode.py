from pyspark.sql import SparkSession
import os
import pyspark.sql.functions as f
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

path = "D:\\log_search\\"

def retrieve_data_search(path,list_file):
    schema = StructType([
        StructField("datetime", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("keyword", StringType(), True)])
    data = spark.createDataFrame([],schema = schema)
    for i in list_file:
        df = spark.read.parquet(path+i)
        df = df.select('datetime','user_id','keyword')
        data = data.union(df)
        print('Read data {}'.format(i))
    data = data.filter(data.user_id.isNotNull())
    data = data.filter(data.keyword.isNotNull())
    print('Data read finished')
    return data

def processing_most_search(data):
    most_search = data.select('user_id','keyword')
    most_search = most_search.groupBy('user_id','keyword').count()
    most_search = most_search.withColumnRenamed('count','TotalSearch')
    most_search = most_search.orderBy('user_id',ascending=False)
    window = Window.partitionBy('user_id').orderBy(f.col('TotalSearch').desc())
    most_search = most_search.withColumn('Row_Number',f.row_number().over(window))
    most_search = most_search.filter(most_search.Row_Number == 1)
    most_search = most_search.select('user_id','keyword', 'TotalSearch')
    most_search = most_search.withColumnRenamed('keyword','Most_Search')
    return most_search

def main_task(path):
    list_t6 = os.listdir(path)[0:14]
    list_t7 = os.listdir(path)[14:]
    data_t7 = retrieve_data_search(path,list_t7)
    data_t6 = retrieve_data_search(path,list_t6)
    most_search_t6 = processing_most_search(data_t6)
    most_search_t7 = processing_most_search(data_t7)
    most_search_t6 = most_search_t6.withColumnRenamed('Most_Search','Most_Search_T6')
    most_search_t7 = most_search_t7.withColumnRenamed('Most_Search','Most_Search_T7')
    result = most_search_t6.join(most_search_t7,'user_id','inner')
    return result

result = main_task(path)
result.show(20)