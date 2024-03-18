import logging
import os

import pandas as pd

import utils.db_conn as db_conn
import utils.queries as queries

class Settings:

    compression_folder_path: str = "./compression"

_settings = Settings()
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


def expand_dict_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
        Expands dictionary columns in a DataFrame.

        This function iterates through each column of the DataFrame.
        If all elements in a column are dictionaries, it expands the
        dictionary values into separate columns and appends them to the DataFrame.

        Args:
            df (pd.DataFrame): The input DataFrame.

        Return:
            pd.DataFrame: The DataFrame with expanded dictionary columns.
    """
    for col in df.columns:
        if df[col].transform(lambda x: x.apply(type).eq(dict)).all():
            new_columns = pd.DataFrame(df[col].tolist(), index=df.index)
            df.drop(columns=[col], inplace=True)
            df = pd.concat([df, new_columns], axis=1)
    return df


def create_or_insert_from_parquet(parquet_path: str) -> None:
    """
        Creates or inserts data from a Parquet file into a database table.

        This function reads a Parquet file, expands dictionary columns,
        and inserts the data into a database table with the same name as the Parquet file.

        Args:
            parquet_path (str): The path to the Parquet file.

    """
    file_name = parquet_path.split("/")[-1].split(".")[0]
    conn = db_conn.get_db_conn()
    engine = db_conn.create_engine_conn()
    df = pd.read_parquet(parquet_path)
    df = expand_dict_columns(df)
    df.to_sql(file_name, engine, if_exists="append", index=True)
    conn.commit()
    conn.close()


def create_dataset() -> None:
    """
        Creates a final dataset from temporary tables 
        created based on pays, prints and taps tables.
    """
    conn = db_conn.get_db_conn()
    final_query = f"{queries.temp_tables} {queries.final_dataset_query}"
    sql_query = pd.read_sql_query(final_query, conn)
    final_dataset = pd.DataFrame(sql_query)
    final_dataset.to_parquet("final_dataset.parquet")


def main() -> None:
    logging.info("Start to create tables...")
    compression_folder = _settings.compression_folder_path
    for compressed_file in set(os.listdir(compression_folder)):
       parquet_path = f"{compression_folder}/{compressed_file}"
       create_or_insert_from_parquet(parquet_path=parquet_path)
    create_dataset()
