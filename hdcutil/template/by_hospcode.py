
import os
import pandas as pd
import numpy as np
import pyarrow as pa
from math import ceil
from glob import glob
import json

from datetime import datetime, date
from pandas import DataFrame, Series, Index
from configparser import ConfigParser
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


from hdcutil import (
    get_conf, conf_s3,
    path_tmpdb, get_budget_year,
    read_lookup, read_tmpdb, read_person_db, read_person_cid, read_tmpdb_all,
    datediff, ParquetWR,
    check_mod11, df_trim_space,
    ErrorDataFrameEmpty, dataframe_empty
)

import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)
pd.set_option('use_inf_as_na', True)


__processing_time__ = datetime.now()

# %%
## for global variables
DEV_MODE: bool = os.environ.get("DEV", "False").title() == "True"
_PROVINCE_CODE: str = os.environ.get("PROVINCE_CODE", "")

_CONFIG_FILE: str = os.environ.get("CONFIG_FILE", "./config.ini")
_PROCESS_DATE: str = os.environ.get("PROCESS_DATE", date.today().isoformat())

conf:ConfigParser = get_conf(_CONFIG_FILE, force=True)
if _PROVINCE_CODE == "":
    _PROVINCE_CODE:str = conf.get("default", "province_code")
_BUDGET_YEAR: str = os.environ.get("BUDGET_YEAR", conf.get("default", "budget_year"))

print(f"DEV_MODE: {DEV_MODE}")
print(f"CONFIG_FILE: {_CONFIG_FILE}")
print(f"PROVINCE_CODE: {_PROVINCE_CODE}")
print(f"BUDGET_YEAR: {_BUDGET_YEAR}")
print(f"PROCESS_DATE: {_PROCESS_DATE}")


def fill_column(df: DataFrame ) -> DataFrame:
    df.columns = [c.upper() for c in  df.columns]
    col: Index[str] = df.columns
    if not "AREACODE" in col:
        if "VHID" in col:
            df = df.rename(columns={"VHID": "AREACODE"})
        if "HAREA" in col:
            df = df.rename(columns={"HAREA": "AREACODE"})
    if not "D_COM" in col:
        df["D_COM"] = datetime.now().isoformat()
    if not "B_YEAR" in col:
        df["B_YEAR"] = _BUDGET_YEAR
    if not "HOSPCODE" in col:
        if "HOSCODE" in col:
            df = df.rename(columns={"HOSCODE": "HOSPCODE"})
        elif "HCODE" in col:
            df = df.rename(columns={"HCODE": "HOSPCODE"})
    # remove field
    col = df.columns.to_list()
    if "VHID" in col:
        df = df.drop(columns=["VHID"])
    if "HAREA" in col:
        df = df.drop(columns=["HAREA"])
    # order column
    cols:list[str] = ["HOSPCODE","AREACODE", "D_COM", "B_YEAR"]
    cols += set(df.columns.to_list()).difference(cols)
    df.columns = cols
    return df


def verify_df(df: DataFrame)-> bool:
    if df.empty:
        return False
    col :Index[str] = df.columns
    if not "HOSPCODE" in col:
        return False
    if not "B_YEAR" in col:
        return False
    if not "D_COM" in col:
        return False
    if not "AREACODE" in col:
        return False
    return True

# os.error
class DataFrameEmpty(Exception):
    def __init__(self, message="Dataframe is empty."):
        self.message: str = message
        super().__init__(self.message)

# set variables
budget_year:str = str(_BUDGET_YEAR)
b_year: int = int(_BUDGET_YEAR)
process_date: str = _PROCESS_DATE
province_code:str = _PROVINCE_CODE
b_date_start = date(int(budget_year) - 1, 10, 1)
b_date_end = date(int(budget_year), 9, 30)

output_filename: str = "filename_not_define"

# ---

__PYSCRIPT_PARAMETERS__ = None

# --- 



# procssing variables
process_summary = []
process_error = []

# list hospcode
df_hospital: DataFrame = read_lookup("chospital", columns=["STATUS", "HOSPCODE", "CHW_CODE"])
list_hospcode:list[str]= df_hospital.loc[(df_hospital["STATUS"] == 1) & (df_hospital["CHW_CODE"] == province_code), "HOSPCODE"].tolist() 
len_hospcode: int = len(list_hospcode)

## with write parquet one file 

_pathfile = path_tmpdb(output_filename, '_all_', budget_year)
_result_dfs: list[DataFrame] = []
for i, hospcode in enumerate(list_hospcode):
    i += 1
    _start_procsss_dt: datetime = datetime.now()
    percent: int = ceil((i / len_hospcode) * 90)
    st_procss: datetime = datetime.now()
    try:
        
        # processing operation
        df: DataFrame = DataFrame()
        
        # ----
        
        __PROCESSING_CODE__: str = ""
        
        # ----

        # if process successful
        if isinstance(df, DataFrame) and not df.empty:
            delta = datetime.now() - _start_procsss_dt
            msg: str = f"[{datetime.now().isoformat():26}][{str(delta):14}][{hospcode:05}] Success "
            print(msg)
            print(f"{percent}%")
            process_summary.append(msg)
        _result_dfs.append(df.copy())

    except DataFrameEmpty:
        continue
    except ErrorDataFrameEmpty:
        continue
    except Exception as e:
        delta = datetime.now() - _start_procsss_dt
        msg = f"[{datetime.now().isoformat():26}][{str(delta):14}][{hospcode:05}] Error: {str(e)}"
        print(msg)
        process_error.append(msg)

df = pd.concat(_result_dfs, ignore_index=True)
df = fill_column(df)
if not verify_df(df):
    raise Exception("Dataframe is not correct.")
pqwr: ParquetWR = ParquetWR(_pathfile)
pqwr.write(df)
pqwr.close()


print("92%")

print("------------ Summary Processing -------------")

print("Summary:", len(process_summary))
print("SummaryDetail:", json.dumps(process_summary))
print("Error:", len(process_error))
print("ErrorDetail:", json.dumps(process_error))



filepath: str = path_tmpdb(output_filename, '_all_', budget_year)
if not os.path.exists(filepath):
    raise Exception("File not found: ", filepath)
df:DataFrame = read_tmpdb_all(output_filename, budget_year)

print("ProcessDate:", process_date)
print("ProcessTimestamp:", __processing_time__.isoformat())
print("ProvinceCode:", province_code)
print("BudgetYear:", budget_year)
print("Filename:", output_filename)
print("Filesize:", "{:.2f}MB".format(os.stat(filepath).st_size / 1024 / 1024))
print("Columns:", ",".join(df.columns.tolist()))
print("Recordtotal:", f"{len(df):,}")
print("ProcssedTime:", datetime.now() - __processing_time__)

print("100%")