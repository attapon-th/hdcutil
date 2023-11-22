
import os
import pandas as pd
import numpy as np
import pyarrow as pa
from math import ceil
from glob import glob

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
    check_mod11, df_trim_space
)

import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)
pd.set_option('use_inf_as_na', True)


# %%
## for global variables
DEV_MODE: bool = os.environ.get("DEV", "True").title() == "True"
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
    

# set variables
ErrDataframeEmpty = Exception("Dataframe is empty.")
budget_year:str = str(_BUDGET_YEAR)
process_date: str = _PROCESS_DATE
province_code:str = _PROVINCE_CODE
budget_start_date = date(int(budget_year) - 1, 10, 1)
budget_ended_date = date(int(budget_year), 9, 30)

output_filename: str = "filename_not_define"
# ---
__PYSCRIPT_PARAMETERS__ = None
# --- 


# procssing variables
process_summary = []
process_error = []

# list hospcode
df_hospital: DataFrame = read_lookup("chospital")
list_hospcode:list[str]= df_hospital.loc[(df_hospital["CHW_CODE"] == province_code), "HOSCODE"].tolist()
len_hospcode: int = len(list_hospcode)

## with write parquet one file 
pqwr: ParquetWR = ParquetWR(path_tmpdb(output_filename, '_all_', budget_year))
for i, hospcode in enumerate(list_hospcode):
    i += 1
    percent = ceil((i / len_hospcode) * 100)
    print(f"{percent}%")
    try:
        # print("Starting procass hospcode: ", hospcode, f"{i}/{len(list_hospcode)}")
        
        # processing operation
        df: DataFrame = DataFrame()
        
        # ----
        __PROCESSING_CODE__: str = ""
        # ----
        df = fill_column(df)
        if not verify_df(df):
            raise Exception("Dataframe is not correct.")
        pqwr.write(df)

        # if process successful
        if isinstance(df, DataFrame) and not df.empty:
            msg: str = f"Success {hospcode:05} - {len(df):10,} - timestamp: {datetime.now().isoformat()}"
            print(msg)
            process_summary.append(msg)

    except Exception as e:
        if not str(e) == str(ERR_IS_DATAFRAME_EMPTY):
            print("Error processing hospital: {} - {}".format(hospcode, str(e)))
            process_error.append(f"{hospcode} - {e}")

## close ParquetWR
pqwr.close()        

print("\n------")



# %% [markdown]
# ## Print Summary process

# %%
print("Summary")
for line in process_summary:
    print(line)
print("\n------")
print("Error")
if len(process_error) == 0:
    print("No error")
else:
    for line in process_error:
        print(line)
print("Finish.")
print("100%")


filepath = path_tmpdb(output_filename, '_all_', budget_year)
if not os.path.exists(filepath):
    raise Exception("File not found: ", filepath)

df:DataFrame = read_tmpdb_all(output_filename, budget_year)
print("Filename:", output_filename)
print("Columns:", ",".join(df.columns.tolist()))
print("Filesize: {:.2f} MB".format(os.stat(filepath).st_size / 1024 / 1024))
print(f"Total: {len(df):,}")
