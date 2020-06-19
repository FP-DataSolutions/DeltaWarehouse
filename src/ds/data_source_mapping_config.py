from ds.data_source_mapper import DataSourceMapper
from utils.spark_spawner import SparkSpawner
from utils.url_path import UrlPath
from pyspark.sql.types import StructType, StructField, TimestampType, DoubleType, IntegerType, StringType

class DataSourceMappingConfig:
    """description of class"""
    CONFIG_SCHEMA = StructType([
        StructField('DataSourcePath', StringType()),
        StructField('DataSourceFormat', StringType()),
        StructField('DataSourceOptions', StringType()),            
        StructField('TableName', StringType())            
    ])

    def __init__(self, base_path, config_path):
        self.__config_path = UrlPath().combine(base_path, config_path)
        self.__spark = SparkSpawner().get_spark()
        self.__data_source_configurations = list()
        self.__base_path = base_path

    def load_config(self) -> list:
        self.__data_source_configurations.clear()
        cfg = self.__spark.read.csv(self.__config_path, header=True, sep=";", schema=self.CONFIG_SCHEMA).rdd.collect();
        for row in cfg:
            self.__data_source_configurations.append(
                DataSourceMapper(
                    self.__base_path,
                    row['DataSourcePath'],
                    row['DataSourceFormat'],
                    row['DataSourceOptions'],
                    row['TableName'],
                ))
        return self.__data_source_configurations





