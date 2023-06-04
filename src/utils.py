import logging
import requests
from importlib.metadata import version
from bs4 import BeautifulSoup
import ephem

# set log file name for app
LOG_FILE_NAME = "app.log"


def get_package_version(package_name: str) -> str:
    """
    Retrieves the version of a Python package.

    Args:
        package_name (str): The name of the package.

    Returns:
        str: The version of the specified package.
    """
    return version(package_name)


# TODO add attribute to either print to console, file, or both
def setup_logger(name: str, log_file: str) -> logging.Logger:
    """
    Sets up a logger with the specified name and log file.

    Args:
        name (str): The name of the logger.
        log_file (str): The path to the log file.

    Returns:
        logging.Logger: The configured logger instance.
    """
    try:
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
    except Exception as e:
        print(f"{e}")


# TODO Create test
def soup_html(url: str) -> BeautifulSoup:
    """
    Retrieves the HTML content from a given URL and returns a BeautifulSoup object for parsing.

    Args:
        url (str): The URL of the webpage to retrieve and parse.

    Returns:
        BeautifulSoup: The BeautifulSoup object representing the parsed HTML.
    """
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
    """
    Calculate the phase angle of the Moon for a given date.

    Args:
        date (str or datetime.datetime): The date for which to calculate the moon phase.

    Returns:
        float: The phase angle of the Moon, rounded to two decimal places.
    """
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
