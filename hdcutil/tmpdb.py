from __future__ import print_function, division, absolute_import
from glob import glob
import os
import pandas as pd
from pandas import DataFrame
from .config import get_conf
from .util import get_budget_year
from typing import Union, Optional, List
from configparser import ConfigParser

TypeYEAR = Union[str, int]
TypeListStr = List[str]
TypeListStrOptional = Optional[List[str]]


_today_budget_year: str = str(get_budget_year())


def path_tmpdb(
    prefix_name: str, hospcode: str, budget_year: TypeYEAR = _today_budget_year
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
    budget_year: TypeYEAR = _today_budget_year,
    columns: TypeListStrOptional = None,
):
    """
    Read {prefix_name}_{hospcode}_{budget_year}.parquet file and return DataFrame

    Args:
        prefix_name (str): prefix name (ex. t_person_db)
        hospcode (str): hospcode
        budget_year (str | int): budget year (default: current budget year)
        columns (list[str] | None): list of columns to read
    Returns:
        df (DataFrame): DataFrame
    """
    path_file: str = path_tmpdb(prefix_name, hospcode, budget_year)
    if os.path.exists(path_file) is False:
        return DataFrame()
    return pd.read_parquet(
        path_file, engine="pyarrow", dtype_backend="pyarrow", columns=columns
    )


def read_tmpdb_all(
    prefix: str,
    budget_year: TypeYEAR = _today_budget_year,
    columns: TypeListStrOptional = None,
) -> DataFrame:
    """
    Reads data from a temporary database file with a specified prefix and budget year.

    Parameters:
        prefix (str): The prefix of the database file.
        budget_year (Union[str, int], optional): The budget year to read data from. Defaults to the current budget year.
        columns (list, optional): The list of columns to read from the database file. Defaults to None, which reads all columns.

    Returns:
        pd.DataFrame: The data read from the database file as a pandas DataFrame.

    """
    conf: ConfigParser = get_conf()
    if isinstance(budget_year, int):
        budget_year = str(budget_year)
    file_path: str = path_tmpdb(prefix, "_all_", budget_year)

    if os.path.exists(file_path):
        return pd.read_parquet(
            file_path, engine="pyarrow", dtype_backend="pyarrow", columns=columns
        )

    dir_storage: str = conf.get("storage", "tmpdb")
    search_pattern: str = os.path.join(
        dir_storage, budget_year, prefix, f"{prefix}_*_{budget_year}.parquet"
    )
    list_files: list[str] = glob(search_pattern)

    if len(list_files) == 0:
        return DataFrame()

    dfs: list[DataFrame] = []
    for file in list_files:
        dfs.append(
            pd.read_parquet(
                file,
                engine="pyarrow",
                dtype_backend="pyarrow",
                columns=columns,
            )
        )

    return pd.concat(
        dfs,
        ignore_index=True,
        sort=False,
    )


def read_person_db(
    hospcode: str,
    budget_year: TypeYEAR = _today_budget_year,
    columns: TypeListStrOptional = None,
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
    budget_year: TypeYEAR = _today_budget_year,
    columns: TypeListStrOptional = None,
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
    cols: TypeListStrOptional = columns
    if cols is not None and "CK_CID" not in cols:
        cols.append("CK_CID")
    df = read_person_db(hospcode=hospcode, columns=cols, budget_year=budget_year)
    if df.empty:
        return df
    if columns is None:
        columns = df.columns.to_list()
    df: DataFrame = df.loc[df["CK_CID"] > 0]
    return df[columns].copy()
