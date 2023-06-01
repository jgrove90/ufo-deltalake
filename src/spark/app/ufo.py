from delta import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.pandas import read_html
from functools import reduce
from random import randint
from time import sleep
from pyspark.sql.types import *
from utils import setup_logger, LOG_FILE_NAME, BeautifulSoup, soup_html

logger = setup_logger("ufo", LOG_FILE_NAME)


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


def scrapes_ufo_data(url: str) -> DataFrame:
    """Extracts ufo data from website"""
    try:
        df_list = []
        counter = 0

        soup = soup_html(url)
        html_file_list = get_links_inside_table(soup)
        base_url = "https://nuforc.org/webreports"

        # TODO Add progress bar and move to webscraper
        for file in html_file_list:
            temp_df = (
                read_html(f"{base_url}/{file}")[0].rename(
                    columns={"Date / Time": "DateTime"}
                )
                # set index_col to suppress warning
                .to_spark(index_col="temp_index")
            )

            df_list.append(temp_df)

            counter += 1

            logger.info(
                f"Successfully scraped {base_url}/{file} | ({counter}/{len(html_file_list)})"
            )

            # used to escape bot detection
            sleep(randint(1, 3))

        # spark dataframes dont have a concat method must use reduce method
        df = reduce(DataFrame.unionAll, df_list).drop("temp_index")

        logger.info(
            f"UFO data extraction complete with {df.count()} rows and {len(df.columns)} columns"
        )

        return df
    except Exception as e:
        logger.error(f"{e}")
