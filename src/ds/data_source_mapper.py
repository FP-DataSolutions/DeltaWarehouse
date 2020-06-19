from utils.url_path import UrlPath

class DataSourceMapper:
    """description of class"""
    CREATE_TABLE_DDL ="CREATE TABLE {} USING {} {} LOCATION '{}'"

    def __init__(self,base_path,
                ds_path,
                data_format,
                data_options,
                table_name):
        self._base_path = base_path
        self._ds_path = ds_path
        self._data_format = data_format
        self._table_name = table_name
        self._data_options = data_options
    
    def get_mapping_ddl(self):
        ddl = list()
        sql_options=""
        if not( self._data_options is None):                   
            sql_options="OPTIONS ({})".format(self._data_options)
        table_name ="stage_"+self._table_name
        path= UrlPath.combine(self._base_path,self._ds_path)
        ddl.insert(0,"DROP TABLE IF EXISTS {}".format(table_name))
        ddl.insert(1,DataSourceMapper.CREATE_TABLE_DDL.format(table_name,
                                                        self._data_format,
                                                        sql_options,
                                                        path))
        return ddl


