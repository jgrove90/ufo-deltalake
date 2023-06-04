from datetime import date, datetime
import logging
import requests
from importlib.metadata import version
from bs4 import BeautifulSoup
import ephem

# set log file name for app
LOG_FILE_NAME = "app.log"


def get_package_version(package_name: str) -> str:
    """Retreives the version of a python package"""
    return version(package_name)


# TODO add attribute to either print to console, file, or both
def setup_logger(name: str, log_file: str) -> logging.Logger:
    """Defines the logging setup"""

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] %(message)s"
    )

    # create console and file handlers
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


# TODO Create test
def soup_html(url: str) -> BeautifulSoup:
    """Returns BeautifulSoup html object ready for parsing."""
    try:
        logger = setup_logger("webscraping", LOG_FILE_NAME)

        # retreive html and create soup object for parsing
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")

        logger.info(f"Received response from {url}")

        return soup
    except requests.exceptions.RequestException as e:
        logger.error(f"{e}")

def get_moon_phase(date):
    try:
        # Convert date to ephem format
        ephem_date = ephem.Date(date)

        # Calculate the phase angle of the Moon
        moon = ephem.Moon()
        moon.compute(ephem_date)
        phase_angle = round(moon.phase / 100.0, 2)

        return phase_angle
    except Exception as e:
        logger = setup_logger("utils", LOG_FILE_NAME)
        logger.error(f"{e}")