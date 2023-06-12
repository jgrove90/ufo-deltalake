from spark.tables import *
from spark.transformations import *
from spark.spark_utils import spark_session, load_data
from spark.ufo_scraper import scrape_ufo_data
from pyspark.sql import SparkSession


def create_delta_tables(spark: SparkSession) -> None:
    """
    Creates tables for data.

    Args:
        spark (SparkSession): The Spark session.

    Returns:
        None
    """
    create_ufo_bronze_table(spark, "ufo_bronze")
    create_ufo_silver_table(spark, "ufo_silver")
    create_ufo_gold_dim_location(spark, "ufo_gold_dim_location")
    create_ufo_gold_dim_description(spark, "ufo_gold_dim_description")
    create_ufo_gold_dim_date(spark, "ufo_gold_dim_date")
    create_ufo_gold_dim_astro(spark, "ufo_gold_dim_astro")
    create_ufo_gold_fact(spark, "ufo_gold_fact")


def table_transformations(spark: SparkSession) -> list[DeltaTable]:
    """
    Perform table transformations.

    Args:
        spark (SparkSession): The Spark session.

    Returns:
        list[DeltaTable]: List of DeltaTable objects representing the transformed tables.
    """
    tables = [
        ufo_silver_transform(spark),
        ufo_gold_location_transform(spark),
        ufo_gold_description_transform(spark),
        ufo_gold_date_transform(spark),
        ufo_gold_astro_transform(spark),
        ufo_gold_fact_transform(spark),
    ]

    return tables


def load_tables(spark: SparkSession, tables: list[DeltaTable]) -> None:
    """
    Load tables into Delta Lake.

    Args:
        spark (SparkSession): The Spark session.
        tables (list[DeltaTable]): List of DeltaTable objects representing the tables to load.

    Returns:
        None
    """
    # must be the same order as the transformations
    paths = [
        "./lakehouse/ufo/silver",
        "./lakehouse/ufo/gold/dim_location",
        "./lakehouse/ufo/gold/dim_description",
        "./lakehouse/ufo/gold/dim_date",
        "./lakehouse/ufo/gold/dim_astro",
        "./lakehouse/ufo/gold/fact",
    ]

    for table, path in zip(tables, paths):
        load_data(spark, table, path)


if __name__ == "__main__":
    spark = spark_session()
    create_delta_tables(spark)
    df_ufo = scrape_ufo_data("https://nuforc.org/webreports/ndxevent.html")
    load_data(spark, df_ufo, "./spark-warehouse/ufo/bronze")
    load_tables(spark, table_transformations(spark))
