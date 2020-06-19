from utils.spark_spawner import SparkSpawner
from .etl_table_configuration import EtlTableConfiguration
from utils.url_path import UrlPath
from pyspark.sql.types import StructType, StructField, TimestampType, DoubleType, IntegerType, StringType


class ETLConfig:
    CONFIG_SCHEMA = StructType([
        StructField('TableName', StringType()),
        StructField('TableDataSource', StringType()),
        StructField('TableUniqueKeys', StringType()),
        StructField('TableRowKeyName', StringType()),
        StructField('RowHashExcludedColumns', StringType())
    ])
    UNIQUE_KEY_SEP = ','

    def __init__(self, base_path, config_path):
        self.__config_path = UrlPath().combine(base_path, config_path)
        self.__spark = SparkSpawner().get_spark()
        self.__table_configurations = list()
        self.__base_path = base_path

    def load_config(self) -> list:
        ##Add structure
        self.__table_configurations.clear()
        cfg = self.__spark.read.csv(self.__config_path, header=True, sep=";", schema=self.CONFIG_SCHEMA).rdd.collect();
        for row in cfg:
            excluded_cols = row['RowHashExcludedColumns']
            if not excluded_cols is None:
                excluded_cols = row['RowHashExcludedColumns'].split(self.UNIQUE_KEY_SEP)
            else:
                excluded_cols = list()
            self.__table_configurations.append(
                EtlTableConfiguration(
                    row['TableName'],
                    row['TableDataSource'],
                    row['TableUniqueKeys'].split(self.UNIQUE_KEY_SEP),
                    row['TableRowKeyName'],
                    excluded_cols
                ))
        return self.__table_configurations

    def get_base_path(self) -> str:
        return self.__base_path
