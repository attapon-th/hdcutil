from __future__ import print_function, division, absolute_import
from datetime import datetime, date, time
from typing import Union, Optional
import pandas as pd
from pandas import DataFrame, Series
import os
import numpy as np
from configparser import ConfigParser
from .config import get_conf


def get_budget_year(current_date: Union[datetime, date, None] = None) -> int:
    """
    Return budget year based on current date

    Args:
        current_date (Union[datetime, date, None]): current date (default: None)

    Returns:
        int: budget year
    """
    if current_date is None:
        current_date = datetime.now()
    if current_date.month < 10:
        return current_date.year
    else:
        return current_date.year + 1


def check_mod11(cid: str) -> bool:
    """
    Validate Thai citizen ID is Mod11

    Args:
        cid (str): Thai citizen ID

    Returns:
        bool: True if valid else False
    """
    if pd.isna(cid) or len(cid) != 13 or not cid.isnumeric():  # ถ้า pid ไม่ใช่ 13 ให้คืนค่า False
        return False

    cid12 = cid[0:12]  # ตัวเลขหลักที่ 1 - 12 ของบัตรประชาชน
    cid13 = cid[12]  # ตัวเลขหลักที่ 13 ของบัตรประชาชน
    sum_num: int = 0  # ผลรวม
    for i, num in enumerate(cid12):  # วนลูปเช็คว่า pid มีตัวอักษรอยู่ในตำแหน่งไหน
        sum_num += int(num) * (13 - i)  # นำตัวเลขที่เจอมาคูณกับ 13 - i

    digit13 = sum_num % 11  # หาเศษจากผลรวมที่ได้จากการคูณด้วย 11
    digit13 = (11 - digit13) % 10
    return int(cid13) == digit13


def read_lookup(name: str, *, columns: list[str] = None) -> DataFrame:
    """
    Read lookup file from s3

    Args:
        name (str): name of lookup file (Example: chospital)
        columns (list[str]): list of columns (default: None)

    Returns:
        df (DataFrame): DataFrame of lookup
    """
    conf = get_conf()
    s3_obj = dict(conf.items("s3_lookup"))
    bucket = s3_obj.pop("bucket").strip("/")
    prefix = s3_obj.pop("prefix").strip("/")
    s3_obj['anon'] = s3_obj['anon'].lower() == 'true'
    s3_file: str = f"s3://{bucket}/{prefix}/{name}.parquet"

    try:

        df: DataFrame = pd.read_parquet(
            s3_file, engine="pyarrow", dtype_backend="pyarrow", storage_options=s3_obj, columns=columns)
        df.columns = df.columns.str.upper()
        return df
    except Exception as e:
        return pd.DataFrame()


def cal_birth_with_date(df: DataFrame, date_cal: np.datetime64(), col_birth: str, scalar: str = "Y") -> Series:
    """
    Calculate age from  column birth and static date (date_cal)

    Args:
        df (DataFrame): DataFrame
        date_cal (np.datetime64): date for calculate age
        col_birth (str): column name of birth
        scalar (str): scalar (default: "Y") (Y, M, D, W, h, m, s, ms, us, ns)

    Returns:
        sr (Series): Series of age
    """

    # check type of column date and column birth
    dtype_name = df[col_birth].dtype.name
    if not dtype_name.startswith('datetime64') or dtype_name.endswith("[pyarrow]"):
        df[col_birth] = pd.to_datetime(df[col_birth],  errors='coerce')

    df = df.loc[~df[col_birth].isna()]
    df = df.loc[(df[col_birth] <= date_cal)]
    df = df.loc[(df[col_birth] > np.datetime64('1900-01-01'))]

    sr = ((date_cal - df[col_birth]) / np.timedelta64(1, scalar))
    sr = sr.fillna(-9999).astype("int64")
    return sr


def cal_birth_with_column_date(df: DataFrame, col_date: str, col_birth: str, scalar: str = "Y") -> Series:
    """
    Calculate age from column birth and column date

    Args:
        df (DataFrame): DataFrame
        col_date (str): column name of date
        col_birth (str): column name of birth
        scalar (str): scalar (default: "Y") (Y, M, D, W, h, m, s, ms, us, ns)

    Returns:
        sr (Series): Series of age
    """

    # check type of column date and column birth
    dtype_name = df[col_birth].dtype.name
    if not dtype_name.startswith('datetime64') or dtype_name.endswith("[pyarrow]"):
        df[col_birth] = pd.to_datetime(df[col_birth])

    # check type of column date and column for calculate age
    dtype_name = df[col_date].dtype.name
    if not dtype_name.startswith('datetime64') or dtype_name.endswith("[pyarrow]"):
        df[col_date] = pd.to_datetime(df[col_date])

    df = df.loc[~df[col_birth].isna()]

    sr = ((df[col_date] - df[col_birth]) / np.timedelta64(1, scalar))
    sr = sr.fillna(-9999).astype("int64")
    return sr


def df_trim_space(df: DataFrame) -> DataFrame:
    """
    Trim space in string column

    Args:
        df (DataFrame): DataFrame

    Returns:
        df (DataFrame): DataFrame
    """
    cols = df.select_dtypes(['string[pyarrow]', 'string', 'object']).columns
    df[cols] = df[cols].apply(lambda x: x.str.strip())
    return df
