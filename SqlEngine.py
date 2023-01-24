from Config import URI_DB
from sqlalchemy import create_engine, delete, select, Table
import pandas as pd


class SqlEngine(object):
    _instance = None
    _con = None

    def __init__(self):
        raise RuntimeError("Call instance() instead.")

    @classmethod
    def instance(cls):
        if cls._instance is None:
            cls._instance = cls.__new__(cls)
            cls.engine = create_engine(URI_DB, echo=False)
            cls._con = cls.engine.connect()
        return cls._instance

    def execute(self, query: str):
        return self.engine.execute(query)

    def df_read_sql(self, query: str):
        df_result = pd.read_sql(query, con=self._con)
        return df_result

    def select_table(self, table: Table = None, meta_columns: list = None, query: str = ""):
        if str.strip(query) == "":
            query = select(table)
            if not meta_columns is None:
                query = query.with_only_columns(meta_columns)
        df_result = self.df_read_sql(query)
        return df_result

    def delete_table(self, table: Table, query: str = ""):
        if str.strip(query) == "":
            query = delete(table)
        row_count = self.execute(query)
        return row_count

    def insert_df_to_db(self, df: pd.DataFrame, table: Table, include_index: bool = True):
        if df is None:
            return 0
        df.to_sql(table.name, con=self._con, if_exists="append", index=include_index)
        row_count = df.shape[0]
        return row_count

    def display_sample(self, table: Table, order: dict, limit: int = 10):
        print("+" * 50 + f"\nTABLE: {table.name}")
        query = select(table)
        for column, asc_desc in order.items():
            if str.lower(asc_desc) == "desc":
                query = query.order_by(column.desc())
            else:
                query = query.order_by(column.asc())
        query = query.limit(limit)

        df_sample = sql().df_read_sql(query)
        # Drop redundant columns
        if "record_time" in df_sample:
            df_sample.drop(["record_time"], axis=1, inplace=True)
        print(f"{df_sample}\n")


def sql():
    return SqlEngine.instance()
