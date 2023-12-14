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
    if (
        pd.isna(cid) or len(cid) != 13 or not cid.isnumeric()
    ):  # ถ้า pid ไม่ใช่ 13 ให้คืนค่า False
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
        df (DataFrame): DataFrame of lookup if erorr return dataframe is empty
    """
    conf: ConfigParser = get_conf()
    s3_obj = dict(conf.items("s3_lookup"))
    bucket = s3_obj.pop("bucket").strip("/")
    prefix = s3_obj.pop("prefix").strip("/")
    s3_obj["anon"] = s3_obj["anon"].lower() == "true"
    s3_file: str = f"s3://{bucket}/{prefix}/{name}.parquet"

    try:
        df: DataFrame = pd.read_parquet(
            s3_file,
            engine="pyarrow",
            dtype_backend="pyarrow",
            storage_options=s3_obj,
            columns=columns,
        )
        df.columns = df.columns.str.upper()
        return df
    except Exception as e:
        return pd.DataFrame()


def df_trim_space(df: DataFrame) -> DataFrame:
    """
    Trim space in string column


    Args:
        df (DataFrame): DataFrame

    Returns:
        df (DataFrame): DataFrame
    """
    cols = df.select_dtypes(["string[pyarrow]", "string", "object"]).columns
    df[cols] = df[cols].apply(lambda x: x.str.strip())
    return df
