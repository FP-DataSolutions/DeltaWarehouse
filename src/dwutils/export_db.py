from utils.spark_spawner import SparkSpawner
from utils.logger import Logger
from dw.data_warehouse import DataWarehouse

class ExportDb(object):
    
    TABLE_NAME_COL = "tableName"
    DDL_MIGRATION = "CREATE TABLE IF NOT EXISTS {} USING {} {} AS SELECT * FROM {}"
    def __init__(self,base_path,dw_path, dw_name, desc_dw_name):
        self.__logger = Logger(__name__)
        self.__dw = DataWarehouse(base_path,dw_path, dw_name)
        self.__desc_dw_name = desc_dw_name
        
    def generate_export_script(self):
        export_ddls = self.__create_export_script()
        print("--------------------------------------------------------------------------")
        print("--EXPORT DW SCRIPT -PLEASE RUN THIS SCRIPT IN NEW DW (START)...")
        print("USE {};".format(self.__desc_dw_name))
        for ddls in export_ddls:
            for ddl in ddls:
                print(ddl)
        print("--EXPORT DW SCRIPT (END)...")
        print("--------------------------------------------------------------------------")

    def __create_export_script(self):
        ddls = list()
        self.__dw.open()
        tables = self.__dw.get_tables().toPandas()[self.TABLE_NAME_COL]
        for table in tables:
            ddl = self.__create_export_table_ddl(table)
            if len(ddl)>0:
                ddls.append(ddl)
        return ddls

    def __create_export_table_ddl(self, table_name):
        sql = "DESCRIBE FORMATTED {}".format(table_name)
        table_export_script=[]
        is_part_info = False
        part_columns = []
        location=''
        provider =''
        td = self.__dw.run_sql(sql).toPandas()
        for index, row in td.iterrows():
            key = row['col_name']
            value= row['data_type']
            if is_part_info == True :
                if not key.startswith('#') and key != '':
                    part_columns.append(key)
                if key=='':
                    is_part_info = False
            if key == 'Type' and not value=='MANAGED' :
                return []
            if key=="# Partition Information":
                is_part_info = True
            if key=='Location':
                location = " LOCATION '{}'".format(value)
            if key=='Provider':
                provider =  value
        
        partition_by=''
        if len(part_columns)>0:
            separator =','
            partition_by='PARTITIONED BY ({})'.format(separator.join(part_columns))

        ddl_sql = "SHOW CREATE TABLE {} ".format(table_name)
        external_table_name = table_name+"_external"
        table_ddl = self.__dw.run_sql(ddl_sql).collect()[0][0]
        table_ddl_external = table_ddl.replace(table_name, external_table_name) +location
        table_export_script.append(table_ddl_external+";")
        table_export_script.append(self.DDL_MIGRATION.format(table_name,provider,partition_by,external_table_name)+";")
        table_export_script.append("DROP TABLE IF EXISTS {} ;".format(external_table_name))
        return table_export_script  



