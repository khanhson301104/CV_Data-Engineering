import time
import requests
import re
import datetime
import pandas as pd
import mysql.connector

def crawl_data_realtime():
    res = requests.get('https://www.binance.com/en/trade/BTC_USDT?_from=markets&theme=dark&type=spot')
    reg_ex = float(re.findall(r'"close":"\d+.\d+"', res.text)[0].split(":")[1].split('"')[1])
    now = datetime.datetime.now()
    dt_string = now.strftime("%Y-%m-%d %H:%M:%S")
    btc_total = {"Name":"Bitcoin", "Time": dt_string, "Price": reg_ex}
    btc_price = pd.DataFrame(btc_total, index=[0])
    return btc_price

def import_to_mysql(btc_price):
    my_db = mysql.connector.connect(
        host= "localhost",
        user = "root",
        password = '',
        database= "realtime_bitcoin"
    )
    mycursor = my_db.cursor()
    SQL_Query = "INSERT INTO data (Name,Time,Price) VALUES (%s,%s,%s)"
    values = (btc_price["Name"].iloc[0], btc_price["Time"].iloc[0], btc_price["Price"].iloc[0])
    mycursor.execute(SQL_Query,values)
    my_db.commit()
    print("Commit Succesful")
    mycursor.close()
    my_db.close()

# while True:
#     btc_price = crawl_data_realtime()
#     import_to_mysql(btc_price)
#     time.sleep(1000)
#

res = requests.get('https://www.binance.com/en/trade/BTC_USDT?_from=markets&theme=dark&type=spot')
print(res.text)
