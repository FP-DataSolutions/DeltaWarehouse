from pyspark.sql import DataFrame
from .url_path import UrlPath
from .spark_spawner import SparkSpawner

class DataSource:
    def __init__(self, base_path, data_path):
        self.__datapath__ = UrlPath().combine(base_path, data_path)
        self.__spark__ = SparkSpawner().get_spark()

    def load_data(self) -> DataFrame:
        df = self.__spark__.read.parquet(self.__datapath__)
        return df
