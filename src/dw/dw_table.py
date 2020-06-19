from dw.data_warehouse import DataWarehouse
from pyspark.sql.functions import sha2, concat_ws, lit, col,current_timestamp,unix_timestamp,date_add
from pyspark.sql import DataFrame
from utils.data_source import DataSource
from dw.dw_sql import DWSql
from dw.dw_sql_data_source import DataSourceSql
from utils.logger import Logger
from utils.config import Config
from datetime import datetime 
from delta.tables import *

class DWTable:

    def __init__(self,elt_proc_id:int,
               dw: DataWarehouse,
              ds : DataSourceSql,
             table_name :str,
            unique_keys: list,
            row_key_name:str,
            row_hash_excluded_columns : list 
            ):
        self.__dw = dw
        self.__ds = ds
        self.__table_name = table_name.lower()
        self.__table_name_ds = table_name.lower()+"_ds_temp"
        self.__etlhist_table_name = self.__table_name+'_etl_hist'
        self.__join_condition = DWSql.create_join_condition(unique_keys)
        self.__elt_proc_id ="{} AS elt_proc_id".format(elt_proc_id)
        self.__elt_proc_id_value = elt_proc_id
        self.__pkeys = unique_keys
        self.__row_hash_excluded_columns = row_hash_excluded_columns
        self.__elt_proc_oper="'{}'"
        self.__row_key_name=row_key_name.lower()
        self.__logger = Logger(__name__)


    def get_sink_table_name(self):
        return self.__table_name

    def test(self):
        self.__enrich_data__()
        self.__init_table(self.__etlhist_table_name,"DELTA",True)
        self.__init_table(self.__table_name,"DELTA",False)
        self.load_data()

    def load_data(self):
        
        self.__logger.trace('Loading new data (history)...')
        cols = self.__get_columns__()
        partition_by=" PARTITION(row_elt_proc_id) "
        if not Config.IsSparkDataBrick:
            partition_by =""
        
        
        self.__load_data_from_ds_to_elt_hist(cols,partition_by)
        self.__logger.trace('Loading changes (history)...')

        self.__logger.trace('Updating {} ...'.format(self.__table_name))
        etl_now = datetime.now()
        self.__load_data_changes_from_ds_to_etl_hist(cols,partition_by,etl_now)
        #add inserts  to delta   history
        self.__load_new_data_changes_from_ds_to_etl_hist(cols,partition_by,etl_now)

        self.__logger.trace('Loading data from history to sink...')
        merge = DWSql.create_merge_sql(self.__table_name,
                                   self.__etlhist_table_name,
                                   self.__elt_proc_id_value,
                                   self.__row_key_name,
                                   cols)
        self.__logger.trace(merge)
        if Config.IsSparkDataBrick:
            self.__dw.run_sql(merge)

        #self.__logger.trace('Loading data from history to sink...')
        #add_cols = cols
        #insert = DWSql.create_insert_inserted_sql(
        #    self.__table_name,
        #    self.__etlhist_table_name,
        #    add_cols,
        #    self.__elt_proc_id_value
        #    )
        #self.__logger.trace(insert)
        #self.__dw.run_sql(insert)
        #self.__logger.trace('Updating sink table...')
        #update = DWSql.create_update_changed_sql(
        #    self.__table_name,
        #    self.__etlhist_table_name,
        #    self.__elt_proc_id_value,
        #    self.__row_key_name
        #)
        #self.__logger.trace(update)
        #if Config.IsSparkDataBrick:
        #    self.__dw.run_sql(update)



    def run_etl_process(self):
        self.__enrich_data__()
        self.__init_table(self.__etlhist_table_name,"DELTA",True)
        self.__init_table(self.__table_name,"DELTA",False)
        self.load_data()

    def get_changes(self) -> DataFrame:
        self.__logger.trace("Getting changes ...")
        sql=DWSql.create_get_changes_sql(self.__etlhist_table_name,self.__elt_proc_id_value)
        self.__logger.trace(sql)
        return self.__dw.run_sql(sql)

    def __get_columns__(self):
        cols = self.__dw.run_sql(DWSql.SHOW_COLUMNS.format(self.__table_name))
        return ','.join(list(cols.toPandas()['col_name']))

    def __enrich_data__(self):
        df = self.__ds.load_data()
        cols = df.columns
        if len(self.__row_hash_excluded_columns) > 0:
            cols =list(set(cols) - set(self.__row_hash_excluded_columns))
        if len(cols) == 0 : 
            cols.append(self.__pkeys)

        df = df.withColumn(self.__row_key_name,lit("0"))\
            .withColumn("row_sha2", sha2(concat_ws("||", *cols), 256)) \
            .withColumn("row_version",lit(self.__elt_proc_id_value)) \
            .withColumn("row_datetime",lit(current_timestamp())) \
            .withColumn("row_iscurrent",lit(True))\
            .withColumn("row_startdate",lit('2000-01-01').cast('timestamp')) \
            .withColumn("row_enddate",lit('9999-12-31').cast('timestamp')) \
            .withColumn("row_elt_proc_id",lit(self.__elt_proc_id_value))
        df.createOrReplaceTempView(self.__table_name_ds)

    def __init_table(self, table_name, format, add_oper_info = False):
        self.__logger.trace('Initializating table {} ...'.format(table_name))
        sql=""
        tddl = "*"
        partitionBy =""
       
        if add_oper_info:
            tddl = tddl+",'INSERT' AS row_elt_proc_oper "
            if Config.IsSparkDataBrick:
                partitionBy=" PARTITIONED BY (row_elt_proc_id)"
        if not self.__dw.is_table_exists(table_name):
          self.__logger.trace('Table does not exist...')
          self.__logger.trace('Creating new table {} format: {}'.format(table_name,format))
          sql = DWSql.CREATE_TABLE_SQL.format(
            table_name,
            format,
            partitionBy,
            tddl,
            self.__table_name_ds)
          sql = sql + " WHERE 1 <> 1"
          self.__logger.trace(sql)
          self.__dw.run_sql(sql)
        else:
            self.__logger.trace('Table {} already exists...'.format(table_name))

    def __load_data_from_ds_to_elt_hist(self,cols,partition_by):
        self.__logger.trace('Loading new data from {} into {}'.format(self.__table_name_ds,self.__etlhist_table_name))
        values = cols.replace(self.__row_key_name,"uuid()") 
        values = values.replace("row_elt_proc_id","row_elt_proc_id,'INSERT'")
        insert = DWSql.create_insert_new_sql(
            self.__etlhist_table_name,
            values,
            self.__table_name_ds,
            self.__join_condition,
            partition_by
            )
        self.__logger.trace(insert)
        self.__dw.run_sql(insert)

    def __load_data_changes_from_ds_to_etl_hist(self,cols,partition_by,etl_date):
        self.__logger.trace('Loading data changes from {} into {}'.format(self.__table_name_ds,self.__etlhist_table_name))
        values = cols.replace("row_iscurrent,row_startdate,row_enddate","false,row_startdate,'{}'".format(etl_date))
        #values = values.replace(self.__row_key_name,"uuid()")
        values = values.replace("row_elt_proc_id","{},'UPDATE'".format(self.__elt_proc_id_value))
        insert_changed = DWSql.create_insert_changed_sql(
            self.__etlhist_table_name + partition_by,
            values,
            self.__table_name,
            self.__table_name_ds,
            self.__join_condition
            )
        self.__logger.trace(insert_changed)
        self.__dw.run_sql(insert_changed)

    def __load_new_data_changes_from_ds_to_etl_hist(self,cols,partition_by,etl_date):
        self.__logger.trace('Loading data changes from {} into {}'.format(self.__table_name_ds,self.__etlhist_table_name))
        values = cols.replace("row_iscurrent,row_startdate,row_enddate","true,'{}','9999-12-31'".format(etl_date))
        values = values.replace(self.__row_key_name,"uuid()")
        values = values.replace("row_elt_proc_id","row_elt_proc_id,'INSERT'")
        insert_changed = DWSql.create_insert_changed_sql(
            self.__etlhist_table_name + partition_by,
            values,
            self.__table_name_ds,
            self.__table_name,
            self.__join_condition
            )
        self.__logger.trace(insert_changed)
        self.__dw.run_sql(insert_changed)



        
