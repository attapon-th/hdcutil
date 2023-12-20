from __future__ import absolute_import, division, print_function

from datetime import date, datetime
from typing import Union

import numpy as np
import pandas as pd
import pyarrow as pa
from pandas import Series

_T_DATETIME = Union[Series, np.datetime64, date, datetime]


def datediff(
    from_series: Series,
    to_dt: _T_DATETIME,
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
    pa_type = pa.timestamp("ns")
    from_series = from_series.astype(pd.ArrowDtype(pa_type))
    fr_dt: pa.Array = pa.array(from_series, type=pa_type)
    if isinstance(to_dt, datetime):
        to_dt = pa.scalar(to_dt, type=pa_type)
    elif isinstance(to_dt, date):
        to_dt = pa.scalar(
            datetime.fromisoformat(to_dt.isoformat()),
            type=pa_type,
        )
    else:
        to_dt = pa.array(
            pa.array(to_dt),
            type=pa.timestamp(fr_dt.type.unit),
        )

    if scalar == "Y":
        sr: pa.Array = pa.compute.years_between(fr_dt, to_dt)
    elif scalar == "M":
        sr: pa.Array = pa.compute.months_between(fr_dt, to_dt)
    elif scalar == "D":
        sr: pa.Array = pa.compute.days_between(fr_dt, to_dt)
    elif scalar == "W":
        sr: pa.Array = pa.compute.weeks_between(fr_dt, to_dt)
    elif scalar == "h":
        sr: pa.Array = pa.compute.hours_between(fr_dt, to_dt)
    elif scalar == "m":
        sr: pa.Array = pa.compute.minutes_between(fr_dt, to_dt)
    elif scalar == "s":
        sr: pa.Array = pa.compute.seconds_between(fr_dt, to_dt)
    elif scalar == "ms":
        sr: pa.Array = pa.compute.milliseconds_between(fr_dt, to_dt)

    else:
        return Series()
    return sr.to_pandas(types_mapper=pd.ArrowDtype)
