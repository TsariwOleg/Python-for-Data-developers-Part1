"""
Module provides a function to set up logging for the application.

The setup_logging function configures the logging format, log file, and log level.
It ensures that logs are written to 'app.log' and formats the logs with timestamps,
logger names, log levels, and messages. Additionally, it suppresses verbose logging
from the py4j library used by PySpark.
"""

import logging


def setup_logging(name: str) -> logging.Logger:
    """
    Set up and return a logger object with the specified name.

    This function configures the logging system to write logs to a file named 'app.log'.
    The logs include timestamps, logger names, log levels, and log messages.
    The py4j library's logging level is set to WARNING to suppress verbose logs from PySpark.

    Arguments:
    name (str): The name of the logger.

    Returns:
    logging.Logger: A configured logger object.
    """

    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.basicConfig(filename='app.log',
                        filemode='w',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S', )
    return logging.getLogger(name)
