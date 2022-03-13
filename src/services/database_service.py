
from pangres import upsert
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine


class DatabaseService:
    def __init__(self, conn_string, logger):
        self.logger = logger
        self.conn_string = conn_string
        self.db = create_engine(conn_string)

    def save_dimention_table(self, inp_dim_df: pd.DataFrame, target_table: str, target_schema: str = 'public', dim_table_prefix: str = 'dim_'):
        dim_key = target_table[len(dim_table_prefix):]
        original_id_coulmn = f'{dim_key}_original_id'
        id_coulmn = f'{dim_key}_id'
        now = datetime.now()
        r_suf = '_target'

        inp_dim_df = inp_dim_df.drop_duplicates([original_id_coulmn])

        # get current dim records
        dim_df = self.read_table(target_table, target_schema)
        dim_df = dim_df.astype({original_id_coulmn: str})

        # get entirely new dim records
        new_dims = pd.merge(inp_dim_df, dim_df, on=original_id_coulmn,
                            how='left', indicator='match', suffixes=('', r_suf))
        new_dims = new_dims.loc[new_dims['match'] != 'both']

        # get matching existing dim records
        exs_dims = pd.merge(inp_dim_df, dim_df[dim_df['end_date'].isnull(
        )], on=original_id_coulmn, how='inner', suffixes=('', r_suf))

        columns_to_compare = [
            col for col in exs_dims.columns if col.endswith(r_suf)]
        filter = None
        for column in columns_to_compare:
            original_column = column[:-len(r_suf)]
            if filter is None:
                filter = (exs_dims[original_column] != exs_dims[column]) & (
                    ~exs_dims[original_column].isnull())
            else:
                filter = filter | ((exs_dims[original_column] != exs_dims[column]) & (
                    ~exs_dims[original_column].isnull()))

        # get only chenged existing dim records
        upd_dims = exs_dims.loc[filter]

        # get new changed dim records
        ins_dims = upd_dims.assign(start_date=now)
        ins_dims = ins_dims[dim_df.columns.intersection(ins_dims.columns)]
        ins_dims = ins_dims.drop(id_coulmn, 1)

        # update changed dim records end_date
        upd_dims = upd_dims.assign(end_date=now)

        # select only needed columns
        upd_columns = [id_coulmn, original_id_coulmn, 'start_date', 'end_date']
        new_columns = upd_columns.copy()
        columns_to_rename = {}
        if len(columns_to_compare) > 0:
            for column in columns_to_compare:
                original_column = column[:-len(r_suf)]
                columns_to_rename[column] = original_column
                upd_columns.append(column)
                new_columns.append(original_column)

        new_dims = new_dims[new_columns]
        new_dims = new_dims.drop(id_coulmn, 1)
        new_dims = new_dims.assign(start_date=now)

        upd_dims = upd_dims[upd_columns]
        if len(columns_to_rename) > 0:
            upd_dims.rename(columns=columns_to_rename, inplace=True)
        upd_dims.set_index([id_coulmn], inplace=True)

        with self.db.begin() as conn:
            tran = conn.begin()
            try:
                self.update_table(upd_dims, target_table, conn=conn)
                self.write_table(ins_dims, target_table, conn=conn)
                self.write_table(new_dims, target_table, conn=conn)
                tran.commit()
            except Exception as e:
                tran.rollback()
                self.logger.error(
                    f'Error while ingesting {target_table}: \n{e}')
                raise e

    def save_fact_table(self, inp_fact_df: pd.DataFrame, target_table: str, target_schema: str = 'public', fact_table_prefix: str = 'fact_', dim_table_prefix: str = 'dim_'):
        fact_key = target_table[len(fact_table_prefix):]
        original_id_coulmn = f'{fact_key}_original_id'
        id_coulmn = f'{fact_key}_id'
        now = datetime.now()
        r_suf = '_target'
        fc_suf = '_original_id'

        # get matching dim records
        matched_dims_df = inp_fact_df
        columns_to_join = [col for col in matched_dims_df.columns if col.endswith(
            fc_suf) and fact_key not in col]
        for join_column in columns_to_join:
            dim_table = f'{dim_table_prefix}{join_column[:-len(fc_suf)]}'
            id_column = f'{join_column[:-len(fc_suf)]}_id'
            # get current dim records
            dim_df = self.read_table(dim_table, target_schema)
            dim_df = dim_df.astype({join_column: str})
            dim_df = dim_df[dim_df['end_date'].isnull()]
            dim_df = dim_df[[join_column, id_column]]

            # get matching existing dim records
            matched_dims_df = pd.merge(
                matched_dims_df, dim_df, on=join_column, how='inner')

        # get current dim records
        fact_df = self.read_table(target_table, target_schema)
        fact_df = fact_df.astype({original_id_coulmn: str})

        # get entirely new dim records
        new_facts_df = pd.merge(matched_dims_df, fact_df, on=original_id_coulmn,
                                how='left', indicator='match', suffixes=('', r_suf))
        new_facts_df = new_facts_df.loc[new_facts_df['match'] != 'both']
        new_facts_df = new_facts_df[new_facts_df.columns.intersection(
            fact_df.columns)]
        new_facts_df = new_facts_df.drop(id_coulmn, 1)

        self.write_table(new_facts_df, target_table)

    def read_table(self, table_name: str, schema_name: str):
        return pd.read_sql(f'select * from {schema_name}.{table_name}', con=self.db)

    def write_table(self, df: pd.DataFrame, table_name: str, schema_name: str = None, if_exists: str = 'append', conn=None):
        target_conn = conn or self.db.connect()
        df.to_sql(table_name, con=target_conn, if_exists=if_exists,
                  index=False, schema=schema_name)
        self.logger.debug(f'Table {table_name}: {df.shape[0]} rows inserterd')

    def update_table(self, df: pd.DataFrame, table_name: str, schema_name: str = None, if_row_exists='update', conn=None):
        target_conn = conn or self.db.connect()
        upsert(con=target_conn, df=df, table_name=table_name,
               if_row_exists=if_row_exists)
        self.logger.debug(f'Table {table_name}: {df.shape[0]} rows updated')
