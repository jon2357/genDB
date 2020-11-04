import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

### Example Usage
# import log
# logger = log. get_logger("any_name") # the logger will log all values larger than the one set ( )
# logger.debug("this is a debug message")  # integer value: 10
# logger.info("this is a info message")  # integer value: 20
# logger.warn("this is a warn message")  # integer value: 30
# logger.error("this is a error message")  # integer value: 40
# logger.critical("this is a critical message")  # integer value: 50
###

### Grab Logging details from environment variables if they exist, if not use defaults
LOG_LEVEL = logging.getLevelName("DEBUG")
if os.environ.get("LOG_LEVEL"):
    LOG_LEVEL = logging.getLevelName(os.environ.get("LOG_LEVEL").upper())

LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
if os.environ.get("LOG_FORMAT"):
    LOG_FORMAT = os.environ.get("LOG_FORMAT")

LOG_FOLDER = Path(__file__).resolve().parent / "logs"
if os.environ.get("LOG_FOLDER"):
    LOG_FOLDER = os.environ.get("LOG_FOLDER")

if not LOG_FOLDER.is_dir():
    LOG_FOLDER.mkdir(parents=True, exist_ok=True)

LOG_FILE = "genDB.log"
if os.environ.get("LOG_FILE"):
    LOG_FILE = os.environ.get("LOG_FILE")


def get_console_handler():
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(LOG_FORMAT)
    return console_handler


def get_file_handler():
    outputFile = Path(LOG_FOLDER, LOG_FILE)
    print(f"Logging file location: {outputFile}")
    file_handler = TimedRotatingFileHandler(outputFile, when="midnight")
    file_handler.setFormatter(LOG_FORMAT)
    return file_handler


def get_logger(logger_name):
    logger = logging.getLogger(logger_name)
    print(f"Setting logging level to: {LOG_LEVEL}")
    logger.setLevel(LOG_LEVEL)  # better to have too much log than not enough
    logger.addHandler(get_console_handler())
    logger.addHandler(get_file_handler())
    # with this pattern, it's rarely necessary to propagate the error up to parent
    logger.propagate = False
    return logger