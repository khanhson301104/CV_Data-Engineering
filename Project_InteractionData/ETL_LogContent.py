import findspark
import pyspark.sql.functions as f
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta

findspark.init()

path = "D:\\log_content\\"

spark = SparkSession.builder.getOrCreate()

def calculate_date_range(firstday, lastday):
    firstday = datetime.strptime(str(firstday), "%Y%m%d")
    lastday = datetime.strptime(str(lastday), "%Y%m%d")
    total_day = lastday - firstday
    date_range = []
    for i in range(total_day.days + 1):
        day = firstday + timedelta(days=i)
        day = day.strftime("%Y%m%d")
        date_range.append(day)
    return date_range

def read_data(filepath):
    data = spark.read.json(filepath)
    return data

# def check_distinct_appname(filepath):
#     data = read_data(filepath)
#     data = data.select("_source.*")
#     distinct_values = data.select("AppName").distinct().collect()
#     return distinct_values

#      Phai doc het 1 lan 30 ngay roi moi chon distinct appname

def etl_1_day(filepath, file_date):
    data = read_data(filepath)
    data = data.select("_source.*")
    data = data.withColumn("Category",
                           when((col("AppName") == "KPLUS") | (col("AppName") == "DHSD") | (col("AppName") == "KPlus") | (col("AppName") == "CHANNEL"), "Truyen Hinh")
                            .when((col("AppName") == "VOD") | (col("AppName") == "FIMS_RES") | (col("AppName") == "BHD_RES") | (col("AppName") == "VOD_RES") | (col("AppName") == "FIMS") | (col("AppName") == "BHD") | (col("AppName") == "DANET"), "Phim Bo")
                            .when(col("AppName") == "RELAX", "GameShow")
                            .when(col("AppName") == "CHILD", "Kenh Thieu Nhi")
                            .when(col("AppName") == "SPORT", "The Thao")
                            .otherwise("ERROR!")
                           )
    data = data.select("Contract", "TotalDuration", "Category")
    data = data.filter((data.Contract != '0') & (data.Category != "ERROR!"))
    data = data.groupBy("Contract", "Category").sum("TotalDuration")
    data = data.groupBy("Contract").pivot("Category").sum("sum(TotalDuration)")
    data = data.withColumn("Date", lit(file_date))
    return data

def calculate_most_watch(result):
    result_most_watch = result.withColumn("Most Watch", greatest(col("GameShow"), col("Kenh Thieu Nhi"), col("Phim Bo"), col("The Thao"), col("Truyen Hinh")))
    result_most_watch = result_most_watch.withColumn("Most Watch",
                                                     when((col("Most Watch") == col("GameShow")), "GameShow")
                                                     .when((col("Most Watch") == col("Kenh Thieu Nhi")), "Kenh Thieu Nhi")
                                                     .when((col("Most Watch") == col("Phim Bo")), "Phim Bo")
                                                     .when((col("Most Watch") == col("The Thao")), "The Thao")
                                                     .when((col("Most Watch") == col("Truyen Hinh")), "Truyen Hinh")
                                                     )
    return result_most_watch


def calculate_active_days(result):
    windowspec = Window.partitionBy('Contract')
    result = result.withColumn("Active_Days", f.count('Date').over(windowspec))
    return result

def main_task():
    firstday = input("Nhap ngay dau tien: ")
    lastday = input("Nhap ngay cuoi cung: ")
    date_range =  calculate_date_range(firstday, lastday)
    result = etl_1_day(path + date_range[0] + '.json', date_range[0])
    for i in date_range[1:]:
        result_2 = etl_1_day(path + i + '.json', i)
        result = result.union(result_2)
    result = calculate_most_watch(result)
    result = calculate_active_days(result)
    return result

def import_to_mysql(result):
    url = 'jdbc:mysql://' + 'localhost' + ':' + '3306' + '/' + 'data_warehouse'
    driver = "com.mysql.cj.jdbc.Driver"
    user = 'root'
    password = ''
    (result.write.format('jdbc')
     .option('url',url)
     .option('driver',driver)
     .option('dbtable','summary_behavior_data')
     .option('user',user)
     .option('password',password).
     mode('append').save())
    return print("Data Import Successfully")

result = main_task()
import_to_mysql(result)