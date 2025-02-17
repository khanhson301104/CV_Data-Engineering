# import mysql.connector
# import pymongo
# from abc import ABC, abstractmethod
#
# class DatabaseConnector(ABC):
#     @abstractmethod
#     def connect(self) -> None:
#         pass
#
#     @abstractmethod
#     def get_data(self)-> list[dict[str, any]]:
#         pass
#
# class MySQLConnector(DatabaseConnector):
#     def __init__(self, config: dict[str, str]):
#         self.config = config
#         self.connection = None
#     def connect(self) -> None:
#         self.connection = mysql.connector.connect(**self.config)
#     def get_data(self) -> list[dict[str, any]]:
#         cursor = self.connection.cursor(dictionary = True)
#         cursor.execute("Select * from socom1")
#         return cursor.fetcall()
#
# class MongoDBConnector(DatabaseConnector):
#     def __init__(self, config: dict[str, str]):
#         self.config = config
#         self.connection = None
#     def connect(self) -> None:
#         self.connection = pymongo.MongoClient(self.config['uri'])
#     def get_data(self) -> list[dict[str, any]]:
#         db = self.connection[self.config["database"]]
#         collection = db[self.config["collection"]]
#         return list(collection.find().limit(10))
#
# class ConnectorFactory(ABC):
#     @abstractmethod
#     def createConnector(self) -> DatabaseConnector:
#         pass
#
# class MySQLFactory(ConnectorFactory):
#     def createConnector(self) -> DatabaseConnector:
#         config = {
#             'host' : 'localhost',
#             'user': 'root',
#             'password': '',
#             'database': 'socom1'
#         }
#         return MySQLConnector(config)
# class MongoDBFactory(ConnectorFactory):
#     def createConnector(self) -> DatabaseConnector:
#         config = {
#             'uri' : 'mongodb://localhost:27017',
#             'database': 'temp_database',
#             'collection': 'temp_collection'
#         }
#         return MongoDBConnector(config)
# def main():
#     factories = [MySQLFactory(), MongoDBFactory()]
#     for factory in factories:
#         connector = factory.createConnector()
#         connector.connect()
#         data = connector.get_data()
#         print(data)

import pymongo
import mysql.connector
from typing import Dict, Any


class MySQLExtractor:
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def read_data(self):
        connection = mysql.connector.connect(**self.config)
        try:
            cursor = connection.cursor(dictionary=True)
            cursor.execute("SELECT * FROM socom1 LIMIT 10")
            return cursor.fetchall()
        finally:
            cursor.close()
            connection.close()


class MongoDBExtractor:
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def read_data(self):
        client = pymongo.MongoClient(self.config['uri'])
        try:
            db = client[self.config['database']]
            collection = db[self.config['collection']]
            return list(collection.find().limit(10))
        finally:
            client.close()


def main():
    mysql_config = {
        "host": "localhost",
        "user": "root",
        "password": '',
        "database": "socom1"
    }

    mongo_config = {
        'uri': 'mongodb://localhost:27017',
        'database': 'ETL_DEAN',
        'collection': 'Wealth-AccountSeries'
    }

    mysql_extractor = MySQLExtractor(mysql_config)
    mongo_extractor = MongoDBExtractor(mongo_config)

    mysql_data = mysql_extractor.read_data()
    mongo_data = mongo_extractor.read_data()

    print("MySQL data:", mysql_data)
    print("MongoDB data:", mongo_data)


if __name__ == "__main__":
    main()