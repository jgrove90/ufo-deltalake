from bs4 import BeautifulSoup
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import *
from pyspark.pandas import read_html
from functools import reduce
from random import randint
from time import sleep
from pyspark.sql.types import *
from tqdm import tqdm
from app_utils import LOG_FILE_NAME, get_moon_phase, setup_logger, soup_html

logger = setup_logger("ufo_scraper", LOG_FILE_NAME)


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