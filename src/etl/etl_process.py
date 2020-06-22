from dw.data_warehouse import DataWarehouse
from dw.dw_table import DWTable
from etl.etl_config import ETLConfig
from utils.data_source import DataSource
from dw.dw_sql_data_source import DataSourceSql
from ds.data_source_mapping_manager import DataSourceMappingManager
from etl.etl_table_configuration import EtlTableConfiguration
from time import time 
from utils.logger import Logger
from utils.config import Config
from sinks.msql_connector import MsqlConnector

class ETLProcess:

    def __init__(self, base_path,data_sources_config_path, etl_config_path, dw_path, dw_name):
        self.__config = ETLConfig(base_path, etl_config_path)
        self.__dw = DataWarehouse(base_path, dw_path, dw_name)
        self.__stage_ds = DataSourceMappingManager(base_path,data_sources_config_path)
        self.__etl_tables = list()
        self.__etl_process_id = int(time())
        self.__logger = Logger(__name__)
        self.__dwsink = MsqlConnector()

    def init(self):
        self.__logger.trace("Checking connection to sink...")
        #self.__dwsink.con
        self.__logger.trace('Loading configuration...')
        configuration = self.__config.load_config()
        for  cfg  in configuration:
           self.__etl_tables.append(DWTable(
                self.__etl_process_id,
                self.__dw,
                DataSourceSql(
                    cfg.get_table_datasource()
                    ),
                  cfg.get_table_name(),
                  cfg.get_unique_keys(),
                  cfg.get_row_key_name(),
                  cfg.get_row_hash_excluded_columns()
               ))
           self.__logger.trace('Openning data warehouse...')
           self.__dw.open()

    def get_tables(self):
          return self.__etl_tables

    def get_dw(self) -> DataWarehouse:
         return self.__dw

    def register_data_sources(self):
        self.__logger.trace('Registering stage data sources...')
        ddls = self.__stage_ds.get_sql_ddl()
        for ddl in ddls:
            self.__logger.trace(ddl)
            self.__dw.run_sql(ddl)
        self.__logger.trace('Stage data sources have been successfully registered :)')

    def run(self):
        for etl_table in self.__etl_tables:
            etl_table.run_etl_process()
            changes = etl_table.get_changes()
            self.__dwsink.insert_with_overwrite(changes,"stage."+etl_table.get_sink_table_name())

    def run_dimensions(self):
        self.__logger.trace('Loading dimensions ...')
        for etl_table in self.__etl_tables:
            table_name = etl_table.get_sink_table_name().lower()
            if table_name.startswith("dim"):
                self.__process_table(etl_table)
            else:
                self.__logger.trace('Skipping {} - not a dimension...'.format(table_name))

    def run_facts(self):
        self.__logger.trace('Loading  facts ...')
        for etl_table in self.__etl_tables:
            table_name = etl_table.get_sink_table_name().lower()
            if table_name.startswith("fact"):
                self.__process_table(etl_table)
            else:
                 self.__logger.trace('Skipping {} - not a fact...'.format(table_name))

    def __process_table(self,table:DWTable):
        table.run_etl_process()
        if Config.LoadIntoSink:
             changes = table.get_changes()
             self.__dwsink.insert_with_overwrite(changes,"stage."+table.get_sink_table_name())