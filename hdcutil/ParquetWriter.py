from pandas import DataFrame
import pyarrow as pa
from pyarrow.parquet import ParquetWriter


class ParquetWR(ParquetWriter):

    def __init__(self, filename: str, compression: str = 'snappy'):
        """
        Initialize a ParquetWriter object.

        Args:
            filename (str): The name of the Parquet file to be written.
            compression (str): The compression algorithm to use.
        """

        self.filename: str = filename
        self.compression: str = compression
        self.is_start: bool = False

    def write_table(self, df: DataFrame):
        """
        Writes a given DataFrame to a Parquet file using the Arrow library.

        Args:
            df (DataFrame): The DataFrame to be written.

        Returns:
            bool: True if the DataFrame was successfully written to the file, False otherwise.
        """
        table: pa.Table = pa.Table.from_pandas(df, preserve_index=False)
        if self.is_start == False:
            super().__init__(self.filename, table.schema, compression=self.compression)
            self.is_start = True
        return super().write_table(table)

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
            super().close()
            self.is_start = False
