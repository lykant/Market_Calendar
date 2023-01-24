from adate import adate
from SqlEngine import sql
from adate import MARKET_DATE
from datetime import datetime
from workalendar.europe import Turkey
from Config import DATE_FORMAT_STR
import pandas as pd
import warnings

warnings.filterwarnings("ignore")

# Constants
FIRST_DATE = datetime(2000, 1, 1).date()
LAST_DATE = datetime(2049, 12, 31).date()


def load_market_date(start_date, end_date):
    df_holiday = pd.DataFrame(columns=["date", "holiday"])
    calendar_tr = Turkey()
    for hyear in range(start_date.year, end_date.year):
        df_year = pd.DataFrame(calendar_tr.holidays(hyear), columns=["date", "holiday"])
        df_holiday = df_holiday.append(df_year.query("holiday != 'New Year shift'"), ignore_index=True)

    df_date = pd.DataFrame(index=pd.date_range(start_date, end_date))
    df_date.index.name = "date"
    df_date.drop(df_holiday["date"], inplace=True)

    dict_day_name = {
        i: name
        for i, name in enumerate(
            [
                "Monday",
                "Tuesday",
                "Wednesday",
                "Thursday",
                "Friday",
                "Saturday",
                "Sunday",
            ]
        )
    }

    df_date["day"] = df_date.index.dayofweek.map(dict_day_name.get)
    df_date["day_of_week"] = df_date.index.dayofweek + 1
    df_date["week"] = df_date.index.week
    df_date["month"] = df_date.index.month
    df_date["quarter"] = df_date.index.quarter
    df_date["year_half"] = df_date.index.month.map(lambda mth: 1 if mth < 7 else 2)
    df_date["year"] = df_date.index.year
    df_date.query("day_of_week <= 5", inplace=True)

    df_date.reset_index(inplace=True)
    df_date["date"] = df_date["date"].dt.strftime(DATE_FORMAT_STR)
    df_date["date_no"] = adate.date_to_int(df_date["date"])
    df_date.index += 1
    df_date.index.name = "date_row"

    sql().delete_table(MARKET_DATE.meta)
    sql().insert_df_to_db(df_date, MARKET_DATE.meta)


load_market_date(FIRST_DATE, LAST_DATE)
