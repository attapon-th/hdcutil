from __future__ import print_function, division, absolute_import
from datetime import datetime, date, time
from typing import Union, Optional
import pandas as pd
from pandas import DataFrame, Series
import numpy as np
import pyarrow as pa


def cal_birth_with_date(
    df: DataFrame, date_cal: np.datetime64(), col_birth: str, scalar: str = "Y"
) -> Series:
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
    if not dtype_name.startswith("datetime64") or dtype_name.endswith("[pyarrow]"):
        df[col_birth] = pd.to_datetime(df[col_birth], errors="coerce")

    df = df.loc[~df[col_birth].isna()]
    df = df.loc[(df[col_birth] <= date_cal)]
    df = df.loc[(df[col_birth] > np.datetime64("1900-01-01"))]

    sr = (date_cal - df[col_birth]) / np.timedelta64(1, scalar)
    sr = sr.fillna(-9999).astype("int64")
    return sr


def cal_birth_with_column_date(
    df: DataFrame, col_date: str, col_birth: str, scalar: str = "Y"
) -> Series:
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
    if not dtype_name.startswith("datetime64") or dtype_name.endswith("[pyarrow]"):
        df[col_birth] = pd.to_datetime(df[col_birth])

    # check type of column date and column for calculate age
    dtype_name = df[col_date].dtype.name
    if not dtype_name.startswith("datetime64") or dtype_name.endswith("[pyarrow]"):
        df[col_date] = pd.to_datetime(df[col_date])

    df = df.loc[~df[col_birth].isna()]

    sr = (df[col_date] - df[col_birth]) / np.timedelta64(1, scalar)
    sr = sr.fillna(-9999).astype("int64")
    return sr


def datediff(
    from_series: Series,
    to_dt: Union[Series, np.datetime64, date, datetime],
    scalar: str = "Y",
) -> Series:
    """
    Calculate age from column birth and column date

    Args:
        scalar (str): scalar (default: "Y") (Y, M, D, W, h, m, s, ms, us, ns)

    Returns:
        sr (Series): Series of age
    """

    fr_dt: pa.Array = pa.array(from_series, type=pa.timestamp("s"))
    if isinstance(to_dt, datetime):
        to_dt = pa.scalar(to_dt, type=pa.timestamp(fr_dt.type.unit))
    elif isinstance(to_dt, date):
        to_dt = pa.scalar(
            datetime.fromisoformat(date.isoformat()), type=pa.timestamp(fr_dt.type.unit)
        )
    else:
        to_dt = pa.array(to_dt, type=pa.timestamp(fr_dt.type.unit))

    if scalar == "Y":
        sr: Series = pa.compute.years_between(fr_dt, to_dt)
    elif scalar == "M":
        sr: Series = pa.compute.months_between(fr_dt, to_dt)
    elif scalar == "D":
        sr: Series = pa.compute.days_between(fr_dt, to_dt)
    elif scalar == "W":
        sr: Series = pa.compute.weeks_between(fr_dt, to_dt)
    elif scalar == "h":
        sr: Series = pa.compute.hours_between(fr_dt, to_dt)
    elif scalar == "m":
        sr: Series = pa.compute.minutes_between(fr_dt, to_dt)
    elif scalar == "s":
        sr: Series = pa.compute.seconds_between(fr_dt, to_dt)
    elif scalar == "ms":
        sr: Series = pa.compute.milliseconds_between(fr_dt, to_dt)
    else:
        return Series()
    return sr.to_pandas().astype("int64[pyarrow]")
