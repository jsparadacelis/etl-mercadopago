import os

import pandas as pd

class Settings:

    compression_folder_path: str = "./compression"


_settings = Settings()


def compress_file(file_path: str) -> None:
    """
        Compresses a file to Parquet format.

        This function compresses a file to Parquet format based on its extension.
        Supported file formats include CSV and JSON. The compressed file is saved
        in the compression folder with the same name as the original file.

        Args:
            file_path (str): The path to the file to be compressed.

        Raises:
            FileNotFoundError: If the specified file does not exist.
            Exception: If the file format is not supported.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File {file_path} does not exist")
    
    file_name = file_path.split("/")[-1].split(".")[0]
    if file_path.endswith(".csv"):

        csv_as_df = pd.read_csv(file_path)
        new_file_path = f"{_settings.compression_folder_path}/{file_name}.parquet"
        csv_as_df.to_parquet(new_file_path)

    elif file_path.endswith(".json"):

        json_as_df = pd.read_json(file_path, lines=True)
        new_file_path = f"{_settings.compression_folder_path}/{file_name}.parquet"
        json_as_df.to_parquet(new_file_path)

    else:
        raise Exception(f"Format for file {file_path} not supported")
