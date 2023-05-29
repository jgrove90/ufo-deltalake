import requests
from bs4 import BeautifulSoup
from utils import setup_logger, LOG_FILE_NAME

logger = setup_logger("webscraping", LOG_FILE_NAME)


def soup_html(url: str) -> BeautifulSoup:
    """Returns BeautifulSoup html object ready for parsing."""
    try:
        # retreive html and create soup object for parsing
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")

        logger.info(f"Received response from {url}")

        return soup
    except requests.exceptions.RequestException as e:
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
