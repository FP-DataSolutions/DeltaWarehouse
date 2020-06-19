from ds.data_source_mapping_config import DataSourceMappingConfig
from ds.data_source_mapper import DataSourceMapper
class DataSourceMappingManager:
    """description of class"""
    def __init__(self, base_path, mapping_config):
        self.__cfg = DataSourceMappingConfig(base_path,
                                             mapping_config)

    def get_sql_ddl(self) ->list:
        sql_ddl =list()
        mappings = self.__cfg.load_config()
        for mapping_cfg in mappings:
            ds_ddl = mapping_cfg.get_mapping_ddl()
            sql_ddl.append(ds_ddl[0])
            sql_ddl.append(ds_ddl[1])
        return sql_ddl
