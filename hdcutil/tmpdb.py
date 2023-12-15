from __future__ import print_function, division, absolute_import
from glob import glob
import os
import pandas as pd
from pandas import DataFrame
from .config import get_conf
from .util import get_budget_year
from typing import Union, Tuple, Optional
from configparser import ConfigParser

_T_YEAR = Union[str, int]

_today_budget_year: str = str(get_budget_year())


def path_tmpdb(
    prefix_name: str, hospcode: str, budget_year: _T_YEAR = _today_budget_year
) -> str:
    """
    Return path of {prefix_name}_{hospcode}_{budget_year}.parquet file

    Args:
        prefix_name (str): prefix name (ex. t_person_db)
        hospcode (str): hospcode
        budget_year (str): budget year (default: current budget year)

    Returns:
        path (str): path of {prefix_name}_{hospcode}_{budget_year}.parquet file
    """
    budget_year = str(budget_year)
    conf: ConfigParser = get_conf()
    dir_storage: str = conf.get("storage", "tmpdb")
    if not os.path.exists(dir_storage):
        os.makedirs(dir_storage, exist_ok=True)
    dir_storage = os.path.join(dir_storage, budget_year, prefix_name)
    if not os.path.exists(dir_storage):
        os.makedirs(dir_storage, exist_ok=True)

    filename: str = f"{prefix_name}_{hospcode}_{budget_year}.parquet"
    path_file = os.path.join(dir_storage, filename)
    return path_file


def read_tmpdb(
    prefix_name: str,
    hospcode: str,
    budget_year: _T_YEAR = _today_budget_year,
    columns: list = None,
):
    """
    Read {prefix_name}_{hospcode}_{budget_year}.parquet file and return DataFrame

    Args:
        prefix_name (str): prefix name (ex. t_person_db)
        hospcode (str): hospcode
        budget_year (str): budget year (default: current budget year)
        columns (list): list of columns to read
    Returns:
        df (DataFrame): DataFrame
    """
    path_file: str = path_tmpdb(prefix_name, hospcode, budget_year)
    if os.path.exists(path_file) == False:
        return DataFrame()
    return pd.read_parquet(
        path_file, engine="pyarrow", dtype_backend="pyarrow", columns=columns
    )


def read_tmpdb_all(
    prefix: str,
    budget_year: _T_YEAR = _today_budget_year,
    columns: list = None,
) -> pd.DataFrame:
    """
    Reads data from a temporary database file with a specified prefix and budget year.

    Parameters:
        prefix (str): The prefix of the database file.
        budget_year (Union[str, int], optional): The budget year to read data from. Defaults to the current budget year.
        columns (list, optional): The list of columns to read from the database file. Defaults to None, which reads all columns.

    Returns:
        pd.DataFrame: The data read from the database file as a pandas DataFrame.

    """
    conf = get_conf()
    file_path = path_tmpdb(prefix, "_all_", budget_year)

    if os.path.exists(file_path):
        return pd.read_parquet(
            file_path, engine="pyarrow", dtype_backend="pyarrow", columns=columns
        )

    dir_storage = conf.get("storage", "tmpdb")
    search_pattern = os.path.join(
        dir_storage, budget_year, prefix, f"{prefix}_*_{budget_year}.parquet"
    )
    list_files = glob(search_pattern)
    df_list: list[DataFrame] = iter(
        [
            pd.read_parquet(
                file, engine="pyarrow", dtype_backend="pyarrow", columns=columns
            )
            for file in list_files
        ]
    )
    return pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()


def read_person_db(
    hospcode: str,
    budget_year: Union[str, int] = _today_budget_year,
    columns: list = None,
) -> DataFrame:
    """
    Read t_person_db_{hospcode}_{budget_year}.parquet file and return DataFrame

    Args:
        hospcode (str): hospcode
        budget_year (str): budget year (default: current budget year)
        columns (list): list of columns to read

    Returns:
        df (DataFrame): DataFrame person_db
    """
    return read_tmpdb(
        prefix_name="t_person_db",
        hospcode=hospcode,
        budget_year=budget_year,
        columns=columns,
    )


def read_person_cid(
    hospcode: str,
    budget_year: _T_YEAR = _today_budget_year,
    columns: list = None,
) -> DataFrame:
    """
    Read t_person_db_{hospcode}_{budget_year}.parquet file and filter unique CID

    Args:
        hospcode (str): hospcode
        budget_year (str): budget year (default: current budget year)
        columns (list): list of columns to read

    Returns:
        df (DataFrame): DataFrame  with CID is unique

    """
    cols = columns
    if not cols is None and "CK_CID" not in cols:
        cols.append("CK_CID")
    df = read_person_db(hospcode=hospcode, columns=cols, budget_year=budget_year)
    if df.empty:
        return df
    if columns is None:
        columns = df.columns
    df = df.loc[df["CK_CID"] > 0]
    return df[columns].copy()
