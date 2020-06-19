class DWSql:
    """description of class"""
    CREATE_TABLE_SQL ="CREATE TABLE {} USING {} {} AS SELECT {} FROM {}"
    SHOW_COLUMNS = "SHOW COLUMNS IN {}"
    DROP_TABLE_SQL = "DROP TABLE IF EXISTS {}"
    JOIN_SQL_AND = " AND "
    JOIN_CONDITION = "et.{} = lt.{} " + JOIN_SQL_AND
    INSERT_NEW_SQL = "INSERT INTO {} {} SELECT {} FROM {} AS lt LEFT ANTI JOIN {} AS et ON {}"
    INSERT_CHANGED_SQL = "INSERT INTO {} SELECT {} FROM {} AS lt LEFT SEMI JOIN {} AS et ON {} AND et.row_iscurrent = 1 AND lt.row_iscurrent = 1 AND lt.row_sha2 <> et.row_sha2"
    UPDATE_OLD ="UPDATE {} AS et SET row_enddate = current_timestamp(), row_iscurrent = 0, row_elt_proc_id = {} WHERE et.row_iscurrent = 1 \
    AND EXISTS (SELECT {} FROM {} AS lt WHERE lt.row_elt_proc_id ={} AND lt.row_elt_proc_oper ='UPDATE' AND {})"
    INSERT_UPDATED = "INSERT INTO {} SELECT {} FROM {} WHERE row_elt_proc_oper ='UPDATE' AND row_elt_proc_id = {} "
    INSERT_INSERTED = "INSERT INTO {} SELECT {} FROM {} WHERE row_elt_proc_oper ='INSERT' AND row_elt_proc_id = {} "
    SELECT_MAX_KEY="SELECT MAX(key) AS key FROM {}"
    GET_CHANGES="SELECT * FROM {} WHERE row_elt_proc_id = {}"
    MERGE_CHANGES="MERGE INTO {} AS trg    \
                   USING \
                    (SELECT * FROM {} WHERE row_elt_proc_id={}) AS src  \
                        ON  src.{} = trg.{} AND src.row_elt_proc_oper='UPDATE' \
                    WHEN MATCHED THEN \
                        UPDATE SET row_iscurrent =src.row_iscurrent, row_startdate= src.row_startdate, row_enddate = src.row_enddate, row_elt_proc_id= src.row_elt_proc_id \
                    WHEN NOT MATCHED  AND src.row_elt_proc_oper='INSERT' THEN   \
                    INSERT({}) \
                    VALUES({})"

    @staticmethod
    def create_merge_sql(dest_table_name,socure_table_name,elt_proc_id,row_key,column_list):
        return DWSql.MERGE_CHANGES.format(dest_table_name,socure_table_name,elt_proc_id,
                                          row_key,row_key,
                                          column_list,
                                          column_list)

    @staticmethod
    def create_join_condition(pkeys):
        join_condition =""
        for pk in pkeys:
            join_condition += DWSql.JOIN_CONDITION.format(pk,pk)
        join_condition = join_condition[:-len(DWSql.JOIN_SQL_AND)]
        return join_condition

    @staticmethod
    def create_insert_new_sql(desc_table_name, add_columns, source_table, join_condition, partitionby):
        return DWSql.INSERT_NEW_SQL.format(desc_table_name,partitionby, add_columns,source_table,desc_table_name,join_condition)

    @staticmethod
    def create_insert_changed_sql(desc_table_name, add_columns, source_table,existing_table, join_condition):
        return DWSql.INSERT_CHANGED_SQL.format(desc_table_name,add_columns,source_table,existing_table,join_condition)

    @staticmethod
    def create_update_changed_sql(desc_table_name,delta_table,elt_proc_id,row_key):
        join_condition = "et.{} = lt.{}".format(row_key,row_key)
        return DWSql.UPDATE_OLD.format(desc_table_name,elt_proc_id, row_key, delta_table,elt_proc_id,join_condition)

    @staticmethod
    def create_insert_updated_sql(desc_table_name, source_table,values,elt_proc_id):
        return DWSql.INSERT_UPDATED.format(desc_table_name,values, source_table,elt_proc_id)

    @staticmethod
    def create_insert_inserted_sql(desc_table_name, source_table,values,elt_proc_id):
        return DWSql.INSERT_INSERTED.format(desc_table_name,values, source_table,elt_proc_id)
    
    @staticmethod
    def create_get_changes_sql(delta_table,elt_proc_id):
        return DWSql.GET_CHANGES.format(delta_table,elt_proc_id)
