from pandas import DataFrame
import pyarrow as pa
from pyarrow.parquet import ParquetWriter


class PaquetWR(ParquetWriter):

    def __init__(self, filename: str, compression: str = 'snappy'):
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
        return super().write_table(table)
