import os
import pandas as pd
import numpy as np
import pyarrow as pa
from math import ceil
from glob import glob

from datetime import datetime, date
from pandas import DataFrame, Series, set_option
from configparser import ConfigParser
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import json


from dacutil import (
    get_config,
    Addict,
    datediff,
    check_mod11,
    df_strip,
    worker,
)

from hdcutil import (
    init_pandas_options,
    CoLookup,
    HDCFiles,
    ALL_HOSPCODE,
    IgnoreEmptyDataFrame,
    EmptyDataFrame,
)

init_pandas_options()


## for global variables
conf: Addict = get_config(os.getenv("CONFIG_URI", "config.ini"))
conf.unfreeze()
# conf


# # auto set variables
current_b_year: int = (
    date.today().month > 9 and date.today().year - 1 or date.today().year
)

# get config from enveronment
conf.BUDGET_YEAR = str(os.environ.get("BUDGET_YEAR", "2023"))
conf.PROVINCE_CODE = os.environ.get("PROVINCE_CODE", "14")
conf.PROCESS_DATETIME = datetime.now()
conf.PROCESS_DATE = date.today()


b_year = int(conf.BUDGET_YEAR)
b_date_start = datetime(b_year - 1, 10, 1, 0, 0, 0)
b_date_end = datetime(b_year, 9, 30, 23, 59, 59, microsecond=999999)
conf.BETWEEB_BUDGET_DATETIME = (b_date_start, b_date_end)
conf.freeze(True)


print(f"PROVINCE_CODE       : {conf.PROVINCE_CODE}")
print(f"BUDGET_YEAR         : {conf.BUDGET_YEAR}")
print(f"PROCESS_DATETIME    : {conf.PROCESS_DATETIME}")
print(
    f"BETWEEN_BUDGET      : {conf.BETWEEB_BUDGET_DATETIME[0]} - {conf.BETWEEB_BUDGET_DATETIME[1]}"
)


# setup read data and read lookup
hdcfile = HDCFiles(conf.storage.base, conf.BUDGET_YEAR)
colookup = CoLookup(conf.s3_lookup.dsn)


output_filename: str = "filename_not_define"

# ---

__PYSCRIPT_PARAMETERS__ = None

# ---


# procssing variables
process_summary = []
process_error = []

# list hospcode
df_hospital: DataFrame = colookup.get_chospital(
    province_code=conf.PROVINCE_CODE, columns=["STATUS", "HOSPCODE", "CHW_CODE"]
)
list_hospcode: list[str] = df_hospital.loc[
    (df_hospital["STATUS"] == 1) & (df_hospital["CHW_CODE"] == conf.PROVINCE_CODE),
    "HOSPCODE",
].tolist()
len_hospcode: int = len(list_hospcode)

## with write parquet one file

_pathfile: str = hdcfile.get_path(
    output_filename,
    ALL_HOSPCODE,
)
_result_dfs: list[DataFrame] = []
percent: int = 0
for i, hospcode in enumerate(list_hospcode):
    i += 1
    _start_procsss_dt: datetime = datetime.now()
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
            msg: str = f"[{i:4}][{datetime.now().isoformat():26}][{str(delta):14}][{hospcode:05}] Success "
            print(msg)
            process_summary.append(msg)
        _result_dfs.append(df.copy())

    except IgnoreEmptyDataFrame:
        pass
    except EmptyDataFrame:
        pass
    except Exception as e:
        delta = datetime.now() - _start_procsss_dt
        msg = f"[{i:4}][{datetime.now().isoformat():26}][{str(delta):14}][{hospcode:05}] Error: {str(e)}"
        print(msg)
        process_error.append(msg)

df = pd.concat(_result_dfs, ignore_index=True)
df = hdcfile.fill_column(df)
if not hdcfile.verify_df(df):
    raise Exception("Dataframe is not correct.")

df.to_parquet(_pathfile, engine="pyarrow", compression="snappy", index=False)


print("------------ Summary Processing -------------")

print("SummaryDetail:", json.dumps(process_summary))
print("ErrorDetail:", json.dumps(process_error))
print("Summary:", len(process_summary))
print("Error:", len(process_error))


filepath: str = hdcfile.get_path(output_filename, "_all_")
if not os.path.exists(filepath):
    raise Exception("File not found: ", filepath)
df: DataFrame = hdcfile.read_data(output_filename, ALL_HOSPCODE)

print("ProcessDate:", conf.PROCESS_DATE.isoformat())
print("ProcessTimestamp:", conf.PROCESS_DATETIME.isoformat())
print("ProvinceCode:", conf.PROVINCE_CODE)
print("BudgetYear:", conf.BUDGET_YEAR)
print("Filename:", output_filename)
print("Filesize:", "{:.2f}MB".format(os.stat(filepath).st_size / 1024 / 1024))
print("Columns:", ",".join(df.columns.tolist()))
print("RecordTotal:", f"{len(df):,}")
print("ProcessedTime:", datetime.now() - conf.PROCESS_DATETIME)
