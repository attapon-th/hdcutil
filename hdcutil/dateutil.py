from __future__ import print_function, division, absolute_import
from datetime import datetime, date, time
from typing import Union, Optional
import pandas as pd
from pandas import DataFrame, Series
import numpy as np
import pyarrow as pa


def datediff(
    from_series: Series,
    to_dt: Union[Series, np.datetime64, date, datetime],
    scalar: str = "Y",
) -> Series:
    """
    Calculate age from column birth and column date

    Args:
        from_series (Series): Series of birth (Ex: df["birth"])
        to_dt (Union[Series, np.datetime64, date, datetime]): Series or scalar of date (Ex: df["date"] or datetime.now())
        scalar (str): scalar (default: "Y") (Y, M, D, W, h, m, s, ms)

    Returns:
        sr (Series): Series of age (type int64)
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
