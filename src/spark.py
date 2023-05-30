from delta import DeltaTable
import webscraper as ws
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.pandas import read_html, DataFrame
from utils import setup_logger, LOG_FILE_NAME, get_package_version
from importlib.metadata import version
import pandas as pd

logger = setup_logger("spark_etl", LOG_FILE_NAME)


# TODO: Create test
def spark_session(app_name: str, master: str) -> SparkSession:
    """Creates a SparkSession"""
    try:
        delta_version = get_package_version("delta-spark")

        spark = (
            SparkSession.builder.appName(app_name)
            .master(master)
            .config("spark.jars.packages", f"io.delta:delta-core_2.12:{delta_version}")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("fs.permissions.umask-mode", "007")
            .getOrCreate()
        )

        logger.info(
            f"Created SparkSession for app:{app_name} @ {master} using delta-spark:{delta_version}"
        )

        return spark
    except Exception as e:
        logger.error(f"{e}")


# TODO: Create test
def ufo_bronze_table(spark_session: SparkSession, table_name: str) -> DeltaTable:
    """Creates a bronze ufo table"""
    try:
        path = "/ufo/bronze"

        if DeltaTable.isDeltaTable(spark_session, f"./spark-warehouse{path}"):
            logger.info(
                f"Delta table: '{table_name}' already exists at './spark-warehouse{path}'"
            )
        else:
            table = (
                DeltaTable.createIfNotExists(spark_session)
                .tableName(table_name)
                .addColumn("DateTime", "STRING")
                .addColumn("City", "STRING")
                .addColumn("State", "STRING")
                .addColumn("Country", "STRING")
                .addColumn("Shape", "STRING")
                .addColumn("Duration", "STRING")
                .addColumn("Summary", "STRING")
                .addColumn("Posted", "STRING")
                .addColumn("Images", "STRING")
                .location(f".{path}")
                .execute()
            )

            logger.info(
                f"Created delta table: {table_name} located at './spark-warehouse{path}'"
            )

            return table
    except Exception as e:
        logger.error(f"{e}")


# TODO create test
def extract_html_table(url: str) -> pd.DataFrame:
    """Extracts html tables from url"""
    try:
        df = read_html(url)
        logger.info(f"Table extracted from f{url}")
        return df[0]
    except Exception as e:
        logger.error(f"{e}")


# TODO create test
def create_spark_dataframe(df: pd.DataFrame) -> DataFrame:
    """Converts pandas to spark dataframe"""
    try:
        spark_df = df.to_spark()
        logger.info("Pandas dataframe converted to spark dataframe")
        return spark_df
    except Exception as e:
        logger.error(f"{e}")


