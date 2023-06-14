from spark.spark_utils import SparkSessionCreator, SparkSession
from spark.table_manager import TableManager
from spark.table_schema import *
from spark.transformations import Transformation
from ufo_scraper import scrape_ufo_data
import os


def create_missing_folders(folders: list) -> None:
    """
    Creates folders that do not exist in the parent directory.

    Args:
        parent_dir (str): The path to the parent directory.
        folders (list): A list of folder names to be created.

    Returns:
        None
    """
    for folder in folders:
        if not os.path.exists(folder):
            os.makedirs(folder)
            print(f"Created folder: {folder}")
        else:
            print(f"Folder already exists: {folder}")


def create_spark_session() -> SparkSession:
    """
    Creates and returns a SparkSession object.

    Returns:
        SparkSession: The created SparkSession object.
    """
    spark_creator = SparkSessionCreator()
    spark_creator.create_spark_session()
    return spark_creator.spark


def create_tables(spark: SparkSession) -> None:
    """
    Creates required tables using the provided SparkSession.

    Args:
        spark (SparkSession): The SparkSession object to use.

    Returns:
        None
    """
    tables = [
        ("bronze", BRONZE, TABLE_PATHS.get("bronze")),
        ("silver", SILVER, TABLE_PATHS.get("silver")),
        ("dim_location", DIM_LOCATION, TABLE_PATHS.get("dim_location")),
        ("dim_description", DIM_DESCRIPTION, TABLE_PATHS.get("dim_description")),
        ("dim_date", DIM_DATE, TABLE_PATHS.get("dim_date")),
        ("dim_astro", DIM_ASTRO, TABLE_PATHS.get("dim_astro")),
        ("fact", FACT, TABLE_PATHS.get("fact")),
    ]

    for table_name, columns, path in tables:
        TableManager(spark).create_table(table_name, columns, path)


def load_transformations(spark: SparkSession) -> None:
    """
    Performs data transformations and loads them into the respective tables.

    Args:
        spark (SparkSession): The SparkSession object to use.

    Returns:
        None
    """
    transformations = [
        ("ufo_silver", TABLE_PATHS.get("silver")),
        ("ufo_gold_location", TABLE_PATHS.get("dim_location")),
        ("ufo_gold_description", TABLE_PATHS.get("dim_description")),
        ("ufo_gold_date", TABLE_PATHS.get("dim_date")),
        ("ufo_gold_astro", TABLE_PATHS.get("dim_astro")),
        ("ufo_gold_fact", TABLE_PATHS.get("fact")),
    ]

    for transformation, path in transformations:
        table = getattr(Transformation(spark), transformation)()
        TableManager(spark).load_data(table, path)


def main():
    """
    Entry point of the ETL process.
    - Creates required folders
    - Creates SparkSession
    - Creates required tables
    - Extracts data via webscraping
    - Performs transformations and loads data into tables
    """
    # create folders
    folders = ["./logs/spark", "./lakehouse"]
    create_missing_folders(folders)

    # create spark session
    spark = create_spark_session()

    # create all required tables
    create_tables(spark)

    # extract data and load into bronze table
    df_ufo = scrape_ufo_data("https://nuforc.org/webreports/ndxevent.html")
    TableManager(spark).load_data(df_ufo, "./spark-warehouse/ufo/bronze")

    # perform transformations and load into silver/gold tables
    load_transformations(spark)


if __name__ == "__main__":
    main()
