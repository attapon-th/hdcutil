from pandas import DataFrame
import pyarrow as pa
from pyarrow.parquet import ParquetWriter
from sqlalchemy import table


class ParquetWR():

    def __init__(self, filename: str, compression: str = 'snappy', *, schema: pa.Schema = None,):
        """
        Initialize a ParquetWriter object.

        Args:
            filename (str): The name of the Parquet file to be written.
            compression (str): The compression algorithm to use.
        """
        self.__pqwr: ParquetWriter
        self.filename: str = filename
        self.compression: str = compression
        self.is_start: bool = False
        self.schema: pa.Schema = None
        if schema is not None:
            self.schema: pa.Schema = schema
            self.__pqwr = ParquetWriter(self.filename, self.schema, compression=self.compression)
            self.is_start: bool = True

    def write(self, df: DataFrame):
        """
        Writes a given DataFrame to a Parquet file using the Arrow library.

        Args:
            df (DataFrame): The DataFrame to be written.

        Returns:
            bool: True if the DataFrame was successfully written to the file, False otherwise.
        """
        if not isinstance(df, DataFrame) or df.empty:
            # print("df is not a DataFrame or is empty")
            return self

        table = pa.Table.from_pandas(df, preserve_index=False)

        if not self.is_start:
            self.schema = table.schema
            # print("Creating Parquet Writer")
            self.__pqwr = ParquetWriter(self.filename, self.schema, compression=self.compression)
            self.is_start = True
        # print("Writing to Parquet")
        self.__pqwr.write_table(table)
        return self

    def close(self):
        """
        Closes the object.

        This method is called to close the object. If the object is currently in the 'start' state, it calls the superclass' close method and sets the 'is_start' flag to False.

        Parameters:
        - None

        Returns:
        - None
        """
        if self.is_start:
            self.__pqwr.close()
            self.is_start = False
