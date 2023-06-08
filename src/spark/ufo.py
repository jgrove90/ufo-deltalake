from bs4 import BeautifulSoup
from delta import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.pandas import read_html
from functools import reduce
from random import randint
from time import sleep
from pyspark.sql.types import *
from tqdm import tqdm
from address_cleaning import clean_address
from app_utils import LOG_FILE_NAME, get_moon_phase, setup_logger, soup_html

logger = setup_logger("ufo", LOG_FILE_NAME)


# TODO: Create test
def ufo_bronze_table(spark_session: SparkSession, table_name: str) -> DeltaTable:
    """Creates a bronze ufo table"""
    try:
        path = "./lakehouse/ufo/bronze"

        if DeltaTable.isDeltaTable(spark_session, f"{path}"):
            logger.info(
                f"Delta table: '{table_name}' already exists @ '{path}'"
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
                f"Created delta table: {table_name} located @ '{path}'"
            )

            return table
    except Exception as e:
        logger.error(f"{e}")


def ufo_silver_table(spark_session: SparkSession, table_name: str) -> DeltaTable:
    """Creates a silver ufo table"""
    try:
        path = "./lakehouse/ufo/silver"

        if DeltaTable.isDeltaTable(spark_session, f"{path}"):
            logger.info(
                f"Delta table: '{table_name}' already exists @ '{path}'"
            )
        else:
            table = (
                DeltaTable.createIfNotExists(spark_session)
                .tableName(table_name)
                .addColumn("id", "LONG")
                .addColumn("city", "STRING")
                .addColumn("state", "STRING")
                .addColumn("country", "STRING")
                .addColumn("latitude", "DOUBLE")
                .addColumn("longitude", "DOUBLE")
                .addColumn("shape", "STRING")
                .addColumn("duration", "STRING")
                .addColumn("summary", "STRING")
                .addColumn("images", "STRING")
                .addColumn("date", "DATE")
                .addColumn("year", "INT")
                .addColumn("month", "INT")
                .addColumn("dayofweek", "STRING")
                .addColumn("week", "INT")
                .addColumn("hour", "INT")
                .addColumn("moonPhaseAngle", "DOUBLE")
                .location(f".{path}")
                .execute()
            )

            logger.info(
                f"Created delta table: {table_name} located @ '{path}'"
            )

            return table
    except Exception as e:
        logger.error(f"{e}")


def get_links_inside_table(soup_html: BeautifulSoup) -> list:
    """Returns a list of links in a table retreived from a BeautifulSoup object."""
    try:
        # find table and create list
        table = soup_html.find("table")
        link_list = [link.get("href") for link in table.find_all("a")]

        logger.info(f"Retreived {len(link_list)} links")

        return link_list
    except AttributeError as e:
        logger.error(f"{e}")


def scrape_ufo_data(url: str) -> DataFrame:
    """Extracts ufo data from website"""
    try:
        df_list = []
        counter = 0

        soup = soup_html(url)
        html_file_list = get_links_inside_table(soup)
        base_url = "https://nuforc.org/webreports"

        for file in tqdm(html_file_list, desc="Scraping UFO data"):
            temp_df = (
                read_html(f"{base_url}/{file}")[0].rename(
                    columns={"Date / Time": "DateTime"}
                )
                # set index_col to suppress warning
                .to_spark(index_col="temp_index")
            )

            df_list.append(temp_df)

            counter += 1

            # used to escape bot detection
            sleep(randint(1, 3))

        # spark dataframes dont have a concat method must use reduce method
        df = reduce(
            DataFrame.unionAll, tqdm(df_list, desc="Unioning UFO dataframes")
        ).drop("temp_index")

        logger.info(
            f"UFO data extraction complete with {df.count()} rows and {len(df.columns)} columns"
        )

        return df
    except Exception as e:
        logger.error(f"{e}")


def ufo_silver_transform(spark_session: SparkSession) -> DataFrame:
    """Performs transformations for the silver layer"""
    try:
        # Register the get_moon_phase() function as a UDF.
        get_moon_phase_udf = udf(get_moon_phase)
        clean_address_udf = udf(clean_address)

        df = spark_session.read.format("delta").load("./lakehouse/ufo/bronze").limit(5)

        df = (
            df.filter(df.Country == "USA")
            .withColumn("timestamp", to_timestamp("DateTime", "MM/dd/yy HH:mm"))
            .withColumn("temp_address", concat_ws(", ", df.City, df.State, df.Country))
            .withColumn("address", clean_address_udf("temp_address"))
            .withColumn("city", split("address", ",").getItem(0))
            .withColumn("state", split("address", ",").getItem(1))
            .withColumn("country", split("address", ",").getItem(2))
            .withColumn("latitude", round(split("address", ",").getItem(3).cast("double"), 3))
            .withColumn("longitude", round(split("address", ",").getItem(4).cast("double"), 3))
            .withColumn("date", to_date("timestamp"))
            .withColumn("year", year("date"))
            .withColumn("month", month("date"))
            .withColumn("dayOfWeek", date_format("date", "EEEE"))
            .withColumn("week", weekofyear("date"))
            .withColumn("hour", hour("timestamp"))
            .withColumn("moonPhaseAngle", get_moon_phase_udf("date").cast("double"))
            .withColumnRenamed("Images", "images")
            .drop("DateTime", "Posted", "timestamp", "temp_address")
            .na.drop(subset=["latitude", "longitude"])
            .dropDuplicates()
            .withColumn("id", monotonically_increasing_id())
            .select(
                "id",
                "city",
                "state",
                "country",
                "latitude",
                "longitude",
                "shape",
                "duration",
                "summary",
                "images",
                "date",
                "year",
                "month",
                "dayOfWeek",
                "week",
                "hour",
                "moonPhaseAngle",
            )
        )
        logger.info(f"Silver layer transformation completed")
        return df
    except Exception as e:
         logger.error(f"{e}")

