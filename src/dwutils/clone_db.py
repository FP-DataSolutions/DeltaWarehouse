from utils.logger import Logger
from dw.data_warehouse import DataWarehouse

class CloneDB(object):
    """description of class"""
    TABLE_NAME_COL = "tableName"
    DDL_MIGRATION = "CREATE TABLE IF NOT EXISTS {} USING {} {} AS SELECT * FROM {}.{}"
    def __init__(self,base_path,from_db_path, from_db_name,to_db_path,to_db_name):
        self.__logger = Logger(__name__)
        self.__from_db_name = from_db_name
        self.__to_db_name = to_db_name
        self.__to_db_path = to_db_path
        self.__dw = DataWarehouse(base_path,from_db_path, from_db_name)
        self.__dw_to = DataWarehouse(base_path,to_db_path, to_db_name)

    def clone(self):
        self.__dw.open()
        ddls = self.__create_migration_script()
        self.__dw_to.open()
        for ddl in ddls:
            print(ddl)
            self.__dw_to.run_sql(ddl)

    def __create_migration_script(self):
          ddls = list()
          self.__dw.open()
          tables = self.__dw.get_tables().toPandas()[self.TABLE_NAME_COL]
          for table in tables:
               ddl = self.__create_migration_table_ddl(table)
               if ddl!='':
                   ddls.append(ddl)
          return ddls

    def __create_migration_table_ddl(self,table_name):
        sql = "DESCRIBE FORMATTED {}".format(table_name)
        td = self.__dw.run_sql(sql).toPandas()
        is_part_info = False
        part_columns = []
        provider =''
        for index, row in td.iterrows():
            key = row['col_name']
            value= row['data_type']
            if is_part_info == True :
                if not key.startswith('#') and key != '':
                    part_columns.append(key)
                if key=='':
                    is_part_info = False

            if key == 'Type' and not value=='MANAGED' :
                return ''

            if key=='Provider':
                provider =  value
            if key=="# Partition Information":
                is_part_info = True
        partition_by=''
        if len(part_columns)>0:
            separator =','
            partition_by='PARTITIONED BY ({})'.format(separator.join(part_columns))

        return self.DDL_MIGRATION.format(table_name,provider,partition_by,self.__from_db_name,table_name)  

