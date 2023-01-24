from Config import START_MARKET_DATE, DATE_FORMAT_INT, DATE_FORMAT_STR
from datetime import datetime
from atable import atable
from sqlalchemy import select, func
from SqlEngine import sql
import pandas as pd

# Table models
MARKET_DATE = atable("P_MARKET_DATE")
STOCK_DATA = atable("T_STOCK_DATA")

# Market date class
class adate:
    date = date_no = date_row = None
    # Constants for class
    __DF_MARKET_CALENDAR = pd.DataFrame()

    def __init__(self, date: datetime.date):
        self.get_market_calendar()
        self.date = self.__correct_date(date=date)
        self.date_no = self.date_to_int(self.date)
        self.date_row = self.get_date_row(self.date)

    def __str__(self):
        return f"'{self.date}'"

    def __repr__(self):
        return f"'{self.date}'"

    @classmethod
    def from_date_no(cls, date_no: int):
        return cls(date=cls.int_to_date(date_no))

    @classmethod
    def __correct_date(cls, date: str = None, date_no: int = None, past: bool = True):
        date_no = date_no or cls.date_to_int(date)
        date = (
            cls.get_market_calendar().query("date_no <= @date_no")["date"].max()
            if past
            else cls.get_market_calendar().query("date_no >= @date_no")["date"].min()
        )
        return cls.str_to_date(date)

    def get_next(self, n: int = 1):
        p_date = self.get_date(date_row=self.date_row + n)
        return adate(p_date)

    def get_past(self, n: int = 1):
        p_date = self.get_date(date_row=self.date_row - n)
        return adate(p_date)

    @classmethod
    def market_start(cls):
        return cls(START_MARKET_DATE)

    @classmethod
    def market_last(cls):
        today = cls.date_to_int(datetime.today().date())
        last_date_no = cls.get_market_calendar().query("date_no <= @today")["date_no"].max()
        return cls.from_date_no(last_date_no)

    @classmethod
    def load_last(cls):
        query = select(STOCK_DATA.meta, func.max(STOCK_DATA.meta.c.date_no).label("max_date_no"))
        last_date_no = sql().df_read_sql(query)["max_date_no"][0]
        return cls.from_date_no(last_date_no)

    @classmethod
    def correct(cls, date: str = None, date_no: int = None, past: bool = True):
        return cls(date=cls.__correct_date(date, date_no, past))

    @staticmethod
    def date_to_int(date: datetime):
        if isinstance(date, pd.Series):
            r_date_no = pd.to_datetime(date).dt.strftime(DATE_FORMAT_INT).astype(int)
        else:
            r_date_no = int(pd.to_datetime(date).date().strftime(DATE_FORMAT_INT))
        return r_date_no

    @staticmethod
    def int_to_date(date_no: int):
        if isinstance(date_no, pd.Series):
            r_date = pd.to_datetime(date_no.astype("str"), format=DATE_FORMAT_INT)
        else:
            r_date = pd.to_datetime(str(date_no), format=DATE_FORMAT_INT).date()
        return r_date

    @staticmethod
    def str_to_date(date: str):
        if isinstance(date, pd.Series):
            r_date = pd.to_datetime(date.astype("str"), format=DATE_FORMAT_STR)
        else:
            r_date = pd.to_datetime(str(date), format=DATE_FORMAT_STR).date()
        return r_date

    @staticmethod
    def check_date_no(date_no: str):
        try:
            _ = pd.to_datetime(date_no, format=DATE_FORMAT_INT).date()
            return True
        except:
            return False

    @classmethod
    def get_market_calendar(cls):
        if cls.__DF_MARKET_CALENDAR.empty:
            list_columns = [
                MARKET_DATE.meta.c.date,
                MARKET_DATE.meta.c.date_no,
                MARKET_DATE.meta.c.date_row,
            ]
            query = select(MARKET_DATE.meta).with_only_columns(list_columns)
            query = query.order_by(MARKET_DATE.meta.c.date_no.desc())
            cls.__DF_MARKET_CALENDAR = sql().df_read_sql(query)
        return cls.__DF_MARKET_CALENDAR

    @classmethod
    def get_date_row(cls, date: str = None, date_no: int = None):
        if date != None:
            date_no = cls.date_to_int(date)
        try:
            df_one_day = cls.get_market_calendar().query("date_no == @date_no")
            df_one_day.reset_index(drop=True, inplace=True)
            date_row = df_one_day["date_row"].iloc[0]
            return date_row
        except Exception:
            raise Exception(f"{date_no} not in market calendar!")

    @classmethod
    def get_date_no(cls, date_row: int):
        try:
            df_one_day = cls.get_market_calendar().query("date_row == @date_row")
            df_one_day.reset_index(drop=True, inplace=True)
            date_no = df_one_day["date_no"].iloc[0]
            return date_no
        except Exception:
            raise Exception(f"{date_row} not in market calendar!")

    @classmethod
    def get_date(cls, date_row: int):
        return cls.int_to_date(cls.get_date_no(date_row=date_row))
