from pandas import DataFrame


class ErrorDataFrameEmpty(Exception):
    def __init__(self, message: str = "DataFrame is empty."):
        self.message: str = message
        super().__init__(self.message)


def dataframe_empty(df: DataFrame, error: str = "raise") -> bool:
    """
    Checks if a given DataFrame is empty.

    Args:
        df (DataFrame): The DataFrame to be checked.
        error (str, optional): The error handling behavior. Defaults to 'raise'.

    Returns:
        bool: True if the DataFrame is empty, False otherwise.
    """
    if df.empty:
        if error == "raise":
            raise ErrorDataFrameEmpty()
        return False
    return True
