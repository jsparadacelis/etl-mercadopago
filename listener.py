import os
import time
import logging

from utils.compression import compress_file


class Settings:

    compression_folder_path: str = "./compression"
    ingestion_folder_path: str = "./ingestion"


_settings = Settings()


def detect_new_files(folder: str, live: bool = False) -> None:
    """
        Detects new files in the ingestion folder and compresses them.

        This function checks for new files in the specified ingestion folder. If a new
        file is found, it compresses the file and saves it in the compression folder.

        Args:
            folder (str): The path to the folder containing files to monitor.
            live (bool, optional): If True, the function runs continuously to detect new
                files in real-time. Defaults to False.
    """
    if not os.path.exists(_settings.compression_folder_path):
        os.mkdir(_settings.compression_folder_path)

    files_before = set(os.listdir(_settings.ingestion_folder_path))
    for file in files_before:
        filename = file.split(".")[0]
        if not os.path.exists(f"{_settings.compression_folder_path}/{filename}.parquet"):
            compress_file(f"{_settings.ingestion_folder_path}/{file}")


    if live:
        while True:
            files_after = set(os.listdir(_settings.ingestion_folder_path))
            new_files = files_after - files_before
            if new_files:
                for new_file in list(new_files):
                    compress_file(f"{folder}/{new_file}")
            time.sleep(3)


def main() -> None:
    folder_to_watch = "./ingestion"
    detect_new_files(folder_to_watch)
