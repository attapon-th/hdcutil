from logging import warning
from urllib.parse import urlparse, ParseResult, parse_qs
from typing import Optional, List

from pandas import DataFrame, read_parquet


_BOOLEAN_STR_LIST_: list[str] = [
    "true",
    "t",
    "y",
    "1",
    "T",
    "TRUE",
    "True",
    "Y",
    "YES",
    "yes",
]


class CoLookup:
    def __init__(self, uri: str):
        """
        Initializes the class with the provided URI.

        Parameters:
            uri (str): The URI to initialize the class. supported s3://... or file://... or https://...

                        s3://key:secret@host:port/bucket/path/to/file?use_ssl=true&anon=false
                            use_ssl (default: true)
                            anon (default: false)

        Returns:
            None
        """
        self.STORAGE_TYPE = "file"
        self.BASE_PATH = "./"
        self.STORAGE_OPTIONS = dict()
        try:
            o: ParseResult = urlparse(uri)

        except ValueError:
            self.STORAGE_TYPE = "file"
            self.BASE_PATH = uri
        except Exception as e:
            raise e
        else:
            if o.scheme.lower() in ["file", "s3"] is False:
                raise Exception("Schema is not supported")

            self.STORAGE_TYPE = o.scheme.lower()
            if self.STORAGE_TYPE == "s3":
                self.BASE_PATH = o.path.strip("/")
                if o.username:
                    self.STORAGE_OPTIONS["key"] = o.username
                if o.password:
                    self.STORAGE_OPTIONS["secret"] = o.password
                if o.hostname:
                    # set endpoint_url
                    qs: dict[str, list[str]] = parse_qs(o.query)
                    http_protocal: str = "https"
                    if "use_ssl" in qs:
                        use_ssl: str = qs["use_ssl"][0]
                        if use_ssl not in _BOOLEAN_STR_LIST_:
                            http_protocal = "http"
                    self.STORAGE_OPTIONS[
                        "endpoint_url"
                    ] = f"{http_protocal}://{o.hostname}:{o.port}"

                    # set anon
                    if "anon" in qs:
                        self.STORAGE_OPTIONS["anon"] = (
                            qs["anon"][0] in _BOOLEAN_STR_LIST_
                        )

                    # set client_kwargs
                    for k, v in qs.items():
                        if k not in ["use_ssl", "anon"]:
                            self.STORAGE_OPTIONS[k] = v[0]

            else:
                self.BASE_PATH: str = uri.replace(o.scheme + "://", "")

    def read_pq(
        self, name: str, columns: Optional[List[str]] = None, ext: str = ".parqust"
    ) -> DataFrame:
        """
        Reads a parquet file from the specified path and returns a DataFrame.

        Args:
            name (str): The name of the parquet file.
            columns (Optional[List[str]], optional): A list of column names to read from the parquet file. Defaults to None to read all columns.
            ext (str, optional): The file extension of the parquet file. Defaults to ".parqust".

        Returns:
            DataFrame: The DataFrame read from the parquet file. if error return empty DataFrame


        """
        try:
            if self.STORAGE_TYPE == "s3":
                return read_parquet(
                    path=f"s3://{self.BASE_PATH}/{name}{ext}",
                    engine="pyarrow",
                    dtype_backend="pyarrow",
                    storage_options=self.STORAGE_OPTIONS,
                    columns=columns,
                )
            else:
                return read_parquet(
                    path=f"{self.BASE_PATH}/{name}{ext}",
                    engine="pyarrow",
                    dtype_backend="pyarrow",
                    columns=columns,
                )
        except Exception as e:
            warning(str(e))
            return DataFrame()

    def get_chospital(
        self,
        name: str = "chospital",
        province_code: Optional[str] = None,
        columns: Optional[List[str]] = None,
        ext: str = ".parqust",
    ) -> DataFrame:
        if columns is not None and province_code is not None:
            columns.append("CHW_CODE")
        df = self.read_pq(name=name, columns=columns, ext=ext)
        if df.empty:
            return df

        try:
            if "HOSPCODE" not in df.columns:
                df["HOSPCODE"] = df["HOSCODE"]
            if province_code is not None:
                df: DataFrame = df.loc[df["CHW_CODE"] == province_code]

        except Exception as e:
            warning(str(e))
            return df

        return df
