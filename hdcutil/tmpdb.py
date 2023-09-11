from __future__ import print_function, division, absolute_import
from glob import glob
import os
import pandas as pd
from pandas import DataFrame
from .config import get_conf
from .util import get_budget_year
from typing import Union, Tuple, Optional
from configparser import ConfigParser
import pyarrow as pa
from pyarrow.parquet import ParquetWriter

_today_budget_year: str = str(get_budget_year())


def path_tmpdb(prefix_name: str, hospcode: str, budget_year: Union[str, int] = _today_budget_year) -> str:
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


def read_tmpdb(prefix_name: str, hospcode: str, budget_year: Union[str, int] = _today_budget_year, columns: list = None):
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
    return pd.read_parquet(path_file, engine='pyarrow', dtype_backend='pyarrow', columns=columns)


def read_person_db(hospcode: str, budget_year: Union[str, int] = _today_budget_year, columns: list = None) -> DataFrame:
    """
    Read t_person_db_{hospcode}_{budget_year}.parquet file and return DataFrame

    Args:
        hospcode (str): hospcode
        budget_year (str): budget year (default: current budget year)
        columns (list): list of columns to read

    Returns:
        df (DataFrame): DataFrame person_db
    """
    return read_tmpdb(prefix_name="t_person_db", hospcode=hospcode, budget_year=budget_year, columns=columns)


def read_person_cid(hospcode: str,  budget_year: Union[str, int] = _today_budget_year, columns: list = None,) -> DataFrame:
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
    df = read_person_db(hospcode=hospcode, columns=cols,
                        budget_year=budget_year)
    if df.empty:
        return df
    if columns is None:
        columns = df.columns
    return df.loc[df["CK_CID"] > 0, columns]


def read_tmpdb_all(prefix_name: str, budget_year: Union[str, int] = _today_budget_year, columns: list = None):
    """
    Read {prefix_name}_*_{budget_year}.parquet file and return DataFrame

    Args:
        prefix_name (str): prefix name (ex. t_person_db)
        budget_year (str): budget year (default: current budget year)
        columns (list): list of columns to read
    Returns:
        df (DataFrame): DataFrame
    """
    conf = get_conf()
    dir_storage = conf.get("storage", "tmpdb")
    path_search = os.path.join(
        dir_storage, budget_year, prefix_name, f"{prefix_name}_*_{budget_year}.parquet")
    list_files = glob(path_search)
    if len(list_files) == 0:
        return DataFrame()
    df = pd.DataFrame()
    for file in list_files:
        df = pd.concat([df, pd.read_parquet(file, engine='pyarrow',
                       dtype_backend='pyarrow', columns=columns)], ignore_index=True)
    return df


def to_parquet(df: DataFrame, pqwriter: ParquetWriter, filepath: str = None, compression='snappy') -> Optional[ParquetWriter]:
    """
    Write DataFrame to parquet file

    Args:
        df (DataFrame): DataFrame
        pqwriter (ParquetWriter): ParquetWriter
        filepath (str): filepath (default: None)
        compression (str): compression (default: 'snappy')

    Returns:
        pqwriter (ParquetWriter): ParquetWriter
    """

    if df is None or df.empty:
        return pqwriter
    if pqwriter is None and filepath is None:
        return None

    table: pa.Table = pa.Table.from_pandas(df, preserve_index=False)
    if pqwriter is None and filepath is not None:
        pqwriter = ParquetWriter(
            filepath, table.schema, compression=compression)
        pqwriter.write_table(table)
    else:
        pqwriter.write_table(table)
    return pqwriter
