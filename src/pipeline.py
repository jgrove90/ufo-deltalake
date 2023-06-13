from spark.spark_utils import SparkSessionCreator
from spark.table_manager import TableManager
from spark.table_schema import *
from spark.transformations import Transformation

# from ufo_scraper import scrape_ufo_data


def create_tables(spark):
    TableManager(spark).create_table("bronze", BRONZE, TABLE_PATHS.get("bronze"))
    TableManager(spark).create_table("silver", SILVER, TABLE_PATHS.get("silver"))
    TableManager(spark).create_table(
        "dim_location", DIM_LOCATION, TABLE_PATHS.get("dim_location")
    )
    TableManager(spark).create_table(
        "dim_description", DIM_DESCRIPTION, TABLE_PATHS.get("dim_description")
    )
    TableManager(spark).create_table("dim_date", DIM_DATE, TABLE_PATHS.get("dim_date"))
    TableManager(spark).create_table(
        "dim_astro", DIM_ASTRO, TABLE_PATHS.get("dim_astro")
    )
    TableManager(spark).create_table("fact", FACT, TABLE_PATHS.get("fact"))


def perform_transformations(spark):
    dataframes = [
        Transformation(spark).ufo_silver(),
        Transformation(spark).ufo_gold_location(),
        Transformation(spark).ufo_gold_description(),
        Transformation(spark).ufo_gold_date(),
        Transformation(spark).ufo_gold_astro(),
        Transformation(spark).ufo_gold_fact(),
    ]
    return dataframes


def load_transformations(spark, dataframes):
    # must be the same order as the transformations
    paths = [
        TABLE_PATHS.get("silver"),
        TABLE_PATHS.get("dim_location"),
        TABLE_PATHS.get("dim_description"),
        TABLE_PATHS.get("dim_date"),
        TABLE_PATHS.get("dim_astro"),
        TABLE_PATHS.get("fact"),
    ]

    for table, path in zip(dataframes, paths):
        TableManager(spark).load_data(table, path)


def main():
    # create spark session
    spark_creator = SparkSessionCreator()
    spark_creator.create_spark_session()
    spark = spark_creator.spark

    # create all required tables
    create_tables(spark)

    # extract data and load into bronze table
    # df_ufo = scrape_ufo_data("https://nuforc.org/webreports/ndxevent.html")
    # TableManager(spark).load_data(df_ufo, "./spark-warehouse/ufo/bronze")

    # perform transformations and load into tables
    data_frames = perform_transformations(spark)
    load_transformations(spark, data_frames)


if __name__ == "__main__":
    main()
