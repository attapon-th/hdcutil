from datetime import datetime, date
import os
import warnings

from typing import Optional, List
from pandas import DataFrame, read_parquet, set_option, Index


def init_pandas_options():
    """
    Initializes the options for the pandas library.

    This function sets two options for the pandas library:
    - `mode.copy_on_write`: If set to `True`, enables copy-on-write support for pandas objects.
    - `mode.use_inf_as_na`: If set to `True`, treats infinity as a missing value in pandas objects.

    Parameters:
    None

    Returns:
    None
    """
    set_option("copy_on_write", True)
    set_option("use_inf_as_na", True)


ALL_HOSPCODE = "__all__"


class HDCFiles:
    def __init__(self, base_path: str, budget_year: str | int):
        """
        Initializes an instance of the class.

        Args:
            base_path (str): The base path for the data files.
            budget_year (str | int): The year for the thai budget.

        Raises:
            Exception: If the budget year is invalid.

        Returns:
            None
        """
        year = int(budget_year)

        if year < 2020 or year > (date.today().year + 1):
            raise Exception(f"Invalid budget year: {year}")

        if not os.path.exists(base_path):
            os.makedirs(base_path, exist_ok=True)

        self.BASE_PATH = base_path
        self.BUDGET_YEAR = str(year)

    @staticmethod
    def validate_hospcode(hospcode: str) -> bool:
        if hospcode == ALL_HOSPCODE:
            return True
        if len(hospcode) != 5 or not hospcode.isnumeric():
            return False
        return True

    def get_path(self, pname: str, hospcode: str) -> str:
        """
        Generates and returns the path to a specific file based on the given parameters.

        Args:
            pname (str): The name of the parameter.
            hospcode (str): The hospital code.

        Returns:
            str: The path to the file.
        """

        if self.validate_hospcode(hospcode=hospcode) is False:
            warnings.warn(message="hospcode is not valid")

        dir_storage: str = os.path.join(
            self.BASE_PATH, hospcode, self.BUDGET_YEAR, pname
        )
        filename: str = f"{pname}_{hospcode}_{self.BUDGET_YEAR}.parquet"
        return os.path.join(dir_storage, filename)

    def has_path(self, pname: str, hospcode: str) -> bool:
        """
        Check if a path exists for a given pname and hospcode.

        Args:
            pname (str): The name of the parameter.
            hospcode (str): The hospital code.

        Returns:
            bool: True if the path exists, False otherwise.
        """
        path_file: str = self.get_path(pname=pname, hospcode=hospcode)
        return os.path.exists(path_file)

    def read_data(
        self,
        pname: str,
        hospcode: str = ALL_HOSPCODE,
        columns: Optional[List[str]] = None,
    ) -> DataFrame:
        """
        Reads data from a file and returns it as a DataFrame.

        Args:
            pname (str): The name of the file to read.
            hospcode (str, optional): The hospital code. Defaults to ALL_HOSPCODE.
            columns (List[str], optional): The list of columns to read. Defaults to None is All collumns.

        Returns:
            DataFrame: The data read from the file as a DataFrame.
        """
        path_file: str = self.get_path(pname=pname, hospcode=hospcode)
        if os.path.exists(path=path_file) is False:
            return DataFrame()
        return read_parquet(
            path=path_file, engine="pyarrow", dtype_backend="pyarrow", columns=columns
        )

    def read_person_db(
        self,
        hospcode: str = ALL_HOSPCODE,
        columns: Optional[List[str]] = None,
    ) -> DataFrame:
        """
        Read all person data for a specific hospital or all hospitals.

        Args:
            hospcode (str, optional): The hospital code. Defaults to ALL_HOSPCODE.
            columns (List[str], optional): The columns to include in the result. Defaults to None is All collumns.

        Returns:
            DataFrame: The person database as a DataFrame.
        """

        df: DataFrame = self.read_data(
            pname="t_person_db", hospcode=hospcode, columns=columns
        )
        return df

    def read_person_cid(
        self,
        hospcode: str = ALL_HOSPCODE,
        columns: Optional[List[str]] = None,
    ) -> DataFrame:
        """
        Read the person is unique CID from the specified hospital code or all hospitals.

        Parameters:
            hospcode (str): The hospital code. Defaults to ALL_HOSPCODE.
            columns (Optional[List[str]]): A list of column names to include in the result DataFrame. Defaults to None is All collumns.

        Returns:
            DataFrame: The DataFrame containing the person's CID.

        """
        cols: List[str] | None = columns
        if columns is not None and "CK_CID" not in columns:
            columns.append("CK_CID")
        df: DataFrame = self.read_person_db(hospcode=hospcode, columns=columns)
        if not df.empty and cols is not None:
            df = df.loc[df["CK_CID"] > 0]
            df = df[cols]
        return df

    def fill_column(self, df: DataFrame) -> DataFrame:
        """
        Only for ***s_ table***
        เติม column ของ DataFrame ที่ต้องการสำหรับตาราง Summary(s table) ในกรณีที่ยังไม่มี column

        Parameters:
        - df (DataFrame): The input DataFrame.

        Returns:
        - df (DataFrame): The modified DataFrame with filled values and reordered columns.
        """
        df.columns = [c.upper() for c in df.columns]
        col: list[str] = df.columns.to_list()
        if "AREACODE" not in col:
            if "VHID" in col:
                df = df.rename(columns={"VHID": "AREACODE"})
            if "HAREA" in col:
                df = df.rename(columns={"HAREA": "AREACODE"})
        if "D_COM" not in col:
            df["D_COM"] = datetime.now().isoformat()
        if "B_YEAR" not in col:
            df["B_YEAR"] = self.BUDGET_YEAR
        if "HOSPCODE" not in col:
            if "HOSCODE" in col:
                df = df.rename(columns={"HOSCODE": "HOSPCODE"})
            elif "HCODE" in col:
                df = df.rename(columns={"HCODE": "HOSPCODE"})
        # remove field
        col = df.columns.to_list()
        if "VHID" in col:
            df = df.drop(columns=["VHID"])
        if "HAREA" in col:
            df = df.drop(columns=["HAREA"])
        # order column
        cols: list[str] = ["HOSPCODE", "AREACODE", "D_COM", "B_YEAR"]
        cols += set(df.columns.to_list()).difference(cols)
        df.columns = cols
        return df

    def verify_df(self, df: DataFrame) -> bool:
        """
        Only for ***s_ table***
        Verifies if the given DataFrame is valid.

        Parameters:
            df (DataFrame): The DataFrame to be verified.

        Returns:
            bool: True if the DataFrame is valid, False otherwise.
        """
        if df.empty:
            return False
        col: Index[str] = df.columns
        if "HOSPCODE" not in col:
            return False
        if "B_YEAR" not in col:
            return False
        if "D_COM" not in col:
            return False
        if "AREACODE" not in col:
            return False
        return True
