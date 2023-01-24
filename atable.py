from sqlalchemy import create_engine, MetaData, Table
from Config import URI_DB


class atable:
    def __init__(self, table_name: str):
        self.name = table_name
        self.meta = Table(
            table_name,
            MetaData(),
            autoload=True,
            autoload_with=create_engine(URI_DB, echo=False),
        )
        self.c_meta_list = self.__get_meta_columns()
        self.columns = self.__get_column_names()
        self.pk = self.__get_pk_names()

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name

    def __get_meta_columns(self):
        return [col for col in self.meta.columns if col.key != "record_time"]

    def __get_column_names(self):
        return [col.name for col in self.meta.columns if col.key != "record_time"]

    def __get_pk_names(self):
        return [pk.name for pk in self.meta.primary_key.columns.values()]
