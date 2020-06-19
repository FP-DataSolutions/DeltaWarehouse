class EtlTableConfiguration(object):
    """description of class"""
     
    def __init__(self, tableName :str , tableDataSource :str , tableUniqueKeys:list,tableRowKeyName,
                 row_hash_excluded_columns):
         self.__tableName = tableName
         self.__tableDataSource = tableDataSource
         self.__tableUniqueKeys = tableUniqueKeys
         self.__tableRowKeyName=tableRowKeyName
         self.__row_hash_excluded_columns = row_hash_excluded_columns

    def get_unique_keys(self) -> list:
         return self.__tableUniqueKeys

    def get_table_name(self) -> str:
         return self.__tableName

    def get_table_datasource(self) -> str:
         return self.__tableDataSource

    def get_row_key_name(self)->str:
        return self.__tableRowKeyName

    def get_row_hash_excluded_columns(self) -> list:
         return self.__row_hash_excluded_columns

