from utils.spark_spawner import SparkSpawner
from utils.url_path import UrlPath
from pyspark.sql.functions import col
from utils.logger import Logger


class DataWarehouse(object):
    """description of class"""
    CREATE_DW_SQL = "CREATE DATABASE IF NOT EXISTS {}"
    CREATE_DW_SQL_LOCATION = " LOCATION '{}'"
    USE_DW = " USE {} "
    SHOW_TABLES_SQL = "SHOW TABLES"
    DESCRIBE_SQL = "DESCRIBE DATABASE {}"
    TABLE_NAME_COL = "tableName"
    DROP_TABLE_SQL = "DROP TABLE IF EXISTS {}"
    DROP_VIEW_SQL = "DROP VIEW IF EXISTS {}"
    VIEW_PREFIX ="vw"

    def __init__(self, base_path, dw_path, dw_name):
        self.__dw_path__ = UrlPath().combine(base_path, dw_path)
        self.__dw_name__ = dw_name.lower()
        self.__spark__ = SparkSpawner().get_spark()
        self.__logger = Logger(__name__)

    def open(self):
        sql = self.CREATE_DW_SQL.format(self.__dw_name__)
        if len(self.__dw_path__) > 0:
            sql = sql + self.CREATE_DW_SQL_LOCATION.format(self.__dw_path__)
        self.__logger.trace(sql)
        self.__spark__.sql(sql)
        self.__spark__.sql(self.USE_DW.format(self.__dw_name__))
        self.__reload_tables()

    def is_table_exists(self, table_name):
        self.__reload_tables()
        return bool(self.__tables__.where(col(self.TABLE_NAME_COL)==table_name.lower()).count())

    def run_sql(self,sql_command):
        self.__spark__.sql(self.USE_DW.format(self.__dw_name__))
        return self.__spark__.sql(sql_command)

    def drop_all_tables(self):
        self.__logger.trace("Dropping all tables...")
        self.__reload_tables()     # tableName
        tables = self.__tables__.toPandas()[self.TABLE_NAME_COL]
        for table in tables:
            sql=""
            if table.startswith("vw"):
                sql = self.DROP_VIEW_SQL.format(table)
            else:
                sql = self.DROP_TABLE_SQL.format(table)
            self.__logger.trace(sql)
            self.__spark__.sql(sql)
        self.__logger.trace("Tables have been successfully dropped")

    def __reload_tables(self):
        self.__tables__ = self.__spark__.sql(self.SHOW_TABLES_SQL)

    def show_tables(self):
        self.__reload_tables()
        self.__tables__.show()

    def get_tables(self):
        self.__reload_tables()
        return self.__tables__


    def describe_dw(self):
        self.__spark__.sql(self.DESCRIBE_SQL.format(self.__dw_name__)).show()

    def get_spark(self):
        return self.__spark__




