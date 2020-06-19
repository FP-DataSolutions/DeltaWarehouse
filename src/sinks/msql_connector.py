from pyspark.sql import DataFrame
from utils import Config,SparkSpawner


class MsqlConnector:
    def __init__(self):
        self.__url = ('jdbc:sqlserver://{0}:{1};database={2};encrypt={3};trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;'
                      + 'loginTimeout=30;').format(Config.DATABASE_HOSTNAME, Config.DATABASE_PORT, Config.DATABASE_NAME,
                                                   Config.DATABASE_ENCRYPT)
        if not Config.IsSparkDataBrick:
            self.__url = ('jdbc:sqlserver://{0}:{1};database={2};loginTimeout=30;').format(Config.DATABASE_HOSTNAME, Config.DATABASE_PORT, Config.DATABASE_NAME)
        self.__properties = {
            'user': Config.DATABASE_USERNAME,
            'password': Config.DATABASE_PASSWORD,
            'driver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
        }

    def insert_with_overwrite(self, df: DataFrame, table):
        return df.write.jdbc(url=self.__url, table=table, mode='overwrite', properties=self.__properties)

    def run_read_query(self, query) -> DataFrame:
        return SparkSpawner.get_spark().read.jdbc(url=self.__url, table=query, properties=self.__properties)

