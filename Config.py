import os

# **************************************
# DB and file
DIR_BASE = os.path.dirname(os.path.abspath(__file__))
DB_STOCK = "BIST.db"
PATH_DB = os.path.join(DIR_BASE, DB_STOCK)
URI_DB = f"sqlite:///{PATH_DB}"

# **************************************
# Common
DATE_FORMAT_INT = "%Y%m%d"
DATE_FORMAT_STR = "%Y-%m-%d"
