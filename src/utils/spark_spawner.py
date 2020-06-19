from pyspark import SparkContext
from pyspark.sql import SparkSession


class SparkSpawner:
    @classmethod
    def get_spark(self) -> SparkSession:
        spark_builder = SparkSession.builder.appName('BigDW')\
            .enableHiveSupport() \
            .config("spark.jars.packages","io.delta:delta-core_2.11:0.5.0")  \
            .config("spark.jars.packages", "com.microsoft.sqlserver:mssql-jdbc:7.0.0.jre8,io.delta:delta-core_2.11:0.5.0")
            #.config("hive.metastore.warehouse.dir","d:/AppData/Metastore/")
            #.config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
        spark = spark_builder.getOrCreate()
        
        return spark

