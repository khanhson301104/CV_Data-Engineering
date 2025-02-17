import os
import findspark
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import *


findspark.init()

path = "D:\\log_search\\"

spark = SparkSession.builder.getOrCreate()


def read_data(lst_date):
    schema = StructType([
        StructField('user_id', StringType(), True),
        StructField('keyword', StringType(), True),
        StructField('datetime',StringType(), True)
    ])
    data = spark.createDataFrame([], schema = schema)
    for i in lst_date:
        full_path = os.path.join(path, i)
        data_temp = spark.read.parquet(full_path)
        data_temp = data_temp.select("user_id", "keyword", "datetime")
        if data is None:
            data = data_temp
        else:
            data = data.union(data_temp)
    print("Read Successfully")
    return data

def calculate_most_search(data):
    data = data.filter(data.user_id.isNotNull())
    data = data.filter(data.keyword.isNotNull())
    data = data.groupby('user_id', 'keyword').count()
    data = data.orderBy('user_id')
    data = data.withColumnRenamed("count", 'Total Search')
    windowspec = Window.partitionBy('user_id').orderBy(f.col('Total Search').desc())
    data = data.withColumn("Row Number", f.row_number().over(windowspec))
    data = data.filter(f.col('Row Number') == 1)
    data = data.select("user_id", 'keyword', 'Total Search')
    print("Calculate Successfully")
    return data

def calculating_trending_sumamry(data):
    trending = data.select('datetime','keyword')
    trending = trending.withColumn('datetime',f.to_date('datetime'))
    trending = trending.groupBy('keyword').count().orderBy('count',ascending=False)
    return trending

def filter_dummy_data(result):
    dummy_user_id = ["0242377", "0301699", "0329662",
                     "0352849","0377008","0387540",
                     "0406157","0559119","0589053",
                     "06000110","06006118", "0313634"]
    result = result.filter(f.col('user_id').isin(dummy_user_id) == False)
    return result

def create_category_t6(result):
    user_id_C_Drama = ["0003361", "0027835", "0064645",
                       "0150958", "0153643", "0231534",
                       "0231670", "0262016", "0305529",
                       "0392220", "0401615","0403476",
                       "0431539"," 0513163", "0537443",
                       "0540671", "06001900","06005092",
                       "06007445", "06008554","06009384",
                       "06012481", "0513163"]

    user_id_K_Drama = ["0017684", "0041173", "0101498",
                       "0115100", "0161303", "0176335",
                       "0180845", "0201199", "0204830",
                       "0218698", "0230298","0261295",
                       "0280976", "030833", "0318156",
                       "0441403", "0479873", "0557443",
                       "0594794","06003932","06007892",
                       "06008412", "06008935","06009865",
                       "06009930", "06012745","06015465",
                       "06016004", "06016123", "06016278",
                       "06018215", "06018545"]

    user_id_Horror = ["0005748"]

    user_id_Anime = ["0008207", "0019650","0060714",
                     "0277535","0395889","0459757",
                     "06001640","06003405","06004864",
                     "06009968", "06011614", "06013189",
                     "06016713"]

    user_id_V_Drama = ["0103456"]

    user_id_Actor = ["0115494"]

    user_id_Charactor = ["0124079"]

    user_id_Romance = ["0139158", "06017004", "06018821"]

    user_id_Action = ["0151915","016508", "0166772",
                      "0184832", "0288542", "0356951",
                      "0407626", "0475576", "0510086",
                      "0534415", "06000470","06016808",
                      ]

    user_id_DayLife = ["0199780", "0417719","0516318",
                       "0534219", "058433", "0597793",
                       "06001554", "06003889", "06006561",
                       "06009600", "06018545", "06018290",
                       "06020867", "06021009"]

    result = result.withColumn("Category T6", when(f.col("user_id").isin(user_id_C_Drama), "C-Drama")
                               .when(f.col("user_id").isin(user_id_K_Drama), "K-Drama")
                               .when(f.col("user_id").isin(user_id_Horror), "Horror")
                               .when(f.col("user_id").isin(user_id_Anime), "Anime")
                               .when(f.col("user_id").isin(user_id_V_Drama), "V-Drama")
                               .when(f.col("user_id").isin(user_id_Actor), "Actor")
                               .when(f.col("user_id").isin(user_id_Charactor), "Charactor")
                               .when(f.col("user_id").isin(user_id_Romance), "Romance")
                               .when(f.col("user_id").isin(user_id_Action), "Action")
                               .when(f.col("user_id").isin(user_id_DayLife), "Day Life"))
    return result

def create_category_t7(result):
    user_id_C_Drama = ["0003361", "0027835", "0101498",
                       "0151915", "0153643", "0180845",
                       "0184832", "0230298", "0261295",
                       "0262016", "0280976", "0318156",
                       "0395889", "0401615","0403476",
                       "0431539","0441403","0459757",
                       "0540671","06001900","06005092",
                       "06007445","06009865","06009930",
                       "06016123"]

    user_id_K_Drama = ["0017684","0041173","0103456",
                       "0139158","0199780","0201199",
                       "0204830","0218698","0231534",
                       "030833","0392220","0479873",
                       "0516318","0534219","0557443",
                       "06001554","06003932","06004864",
                       "06007892","06008412","06009384",
                       "06015465","06016278","06018545",
                       "06018821","06020867"]

    user_id_Horror = ["06008554","06013189"]

    user_id_Anime = ["0005748","0019650", "0060714",
                     "0150958", "0161303", "0277535",
                     "06001640","06003405","06003889",
                     "06008935","06009968","06011614",
                     "06016713","06017004","06018215"]

    user_id_V_Drama = ["016508"]

    user_id_Action = ["0115100", "0166772", "0176335",
                      "0231670", "0288542", "0305529",
                      "0356951", "0407626", "0510086",
                      "0534415", "058433", "0594794",
                      "06000470", "06016808"]

    user_id_DayLife = ["0115494", "0124079", "0417719",
                       "0475576","0513163", "0597793",
                       "06006561", "06009600", "06012481",
                       "06012745","06016004","06018290",
                       "06021009"]
    user_id_GiaoDuc = ["0008207"]
    user_id_AmThuc = ["0064645", "0537443"]
    result = result.withColumn("Category T7", when(f.col("user_id").isin(user_id_C_Drama), "C-Drama")
                               .when(f.col("user_id").isin(user_id_K_Drama), "K-Drama")
                               .when(f.col("user_id").isin(user_id_AmThuc), "Am Thuc")
                               .when(f.col("user_id").isin(user_id_GiaoDuc), "Giao Duc")
                               .when(f.col("user_id").isin(user_id_Horror), "Horror")
                               .when(f.col("user_id").isin(user_id_Anime), "Anime")
                               .when(f.col("user_id").isin(user_id_V_Drama), "V-Drama")
                               .when(f.col("user_id").isin(user_id_Action), "Action")
                               .when(f.col("user_id").isin(user_id_DayLife), "Day Life"))
    return result

def trending_type(result):
    result = result.withColumn("Trending Type", when(f.col("Category T6") == f.col("Category T7"), "Unchanged")
                               .when(f.col("Category T6") != f.col("Category T7"), "Changed"))
    return result

def Previous(result):
    result = result.withColumn("Previous", when(f.col("Trending Type") == "Unchanged", "Unchanged")
                               .otherwise(concat(col("Category T6"), lit("-"),col("Category T7"))))
    return result

lst_m6 = os.listdir(path)[:14]
lst_m7 = os.listdir(path)[14:]

data_m6 = read_data(lst_m6)
data_m7 = read_data(lst_m7)

most_search_t6 = calculate_most_search(data_m6)
most_search_t6 = most_search_t6.withColumnRenamed("Total Search", "Most Search T6")
most_search_t6 = most_search_t6.withColumnRenamed("keyword", "Key Word T6")

most_search_t7 = calculate_most_search(data_m7)
most_search_t7 = most_search_t7.withColumnRenamed("Total Search", "Most Search T7")
most_search_t7 = most_search_t7.withColumnRenamed("keyword", "Key Word T7")

result = most_search_t6.join(most_search_t7,'user_id','inner')
result = result.select("user_id", "Key Word T6", "Key Word T7")

trending_t6 = calculating_trending_sumamry(data_m6)
trending_t7 = calculating_trending_sumamry(data_m7)

result = filter_dummy_data(result)
result = create_category_t6(result)
result = create_category_t7(result)
result = trending_type(result)
result = Previous(result)

result = result.limit(99)

result.show(99)
