import logging
from requests import get
from importlib.metadata import version
from bs4 import BeautifulSoup
import ephem
import sys
import os

# set log file name for app
LOG_FILE_NAME = "./logs/app.log"


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
        response = get(url)
        soup = BeautifulSoup(response.text, "html.parser")

        logger.info(f"Received response from {url}")

        return soup
    except Exception as e:
        logger.error(f"{e}")
        sys.exit()


def get_moon_phase(date):
    """
    Calculate the phase angle of the Moon for a given date.

    Args:
        date (str or datetime.datetime): The date for which to calculate the moon phase.

    Returns:
        float: The phase angle of the Moon, rounded to two decimal places.
    """
    try:
        # Validate the year
        if len(str(date)) == 10:
            year = int(str(date)[:4])
            if year < 1900 or year > 2100:
                return None

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
        return None


def get_file_path(directory: str, extension: str) -> list[str]:
    """
    Retrieve a list of files with a specific extension from a directory and its subdirectories.

    Args:
        directory (str): The path to the directory to search in.
        extension (str): The file extension to filter the files by.

    Returns:
        list[str]: A list of file names that match the specified extension.
    """
    try:
        logger = setup_logger("utils", LOG_FILE_NAME)
        
        files = []

        for root, dirnames, filenames in os.walk(directory):
            for file in filenames:
                if file.endswith(extension):
                    relative_path = os.path.relpath(os.path.join(root, file))
                    files.append(f"./{relative_path}")
        
        logger.info(f"{len(files)} {extension} files found {files}")
        
        return files
    except Exception as e:
        logger.error(f"{e}")
        sys.exit()

def read_requirements(file_path: str) -> list[str]:
    """
    Read a requirements.txt file and return a list of packages.

    Args:
        file_path (str): Path to the requirements.txt file.

    Returns:
        list: List of packages specified in the requirements.txt file.
    """
    try:
        with open(file_path, 'r') as file:
            requirements = file.readlines()
        
        requirements = [line.strip() for line in requirements]
        
        return requirements
    except Exception as e:
        logger = setup_logger("utils", LOG_FILE_NAME)
        logger.error(f"{e}")