from pyspark.sql import DataFrame
from utils.spark_spawner import SparkSpawner

class DataSourceSql:
    def __init__(self, sql_data_object):
        self.__spark__ = SparkSpawner().get_spark()
        self.__sql_data_object=sql_data_object

    def load_data(self) -> DataFrame:
        sql="SELECT * FROM {}".format(self.__sql_data_object)
        return self.__spark__.sql(sql)

