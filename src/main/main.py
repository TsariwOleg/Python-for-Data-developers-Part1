"""
Module provides the main entry point for the ETL process using PySpark.
It initializes Spark, parses the configuration, and delegates transformation tasks
based on the specified API type (DataFrame, SQL, or RDD).
"""
import sys
from argparse import ArgumentParser
import yaml

from pyspark.sql import SparkSession

from utils.logging_utils import setup_logging
from transformer.dataframe_api_transformer import transform as df_transform
from transformer.rdd_api_transformer import transform as rdd_transform
from transformer.sql_api_transformer import transform as sql_transform

logger = setup_logging(__name__)


def initialize_spark(app_name="default_name"):
    """
    Set the Python executable for PySpark to the current Python executable to ensure compatibility
    with the current environment.

    Arguments:
        app_name (str): The name of the Spark application. Defaults to "default_name".

    Returns:
        SparkSession: An initialized SparkSession.
    """
    try:
        return SparkSession \
            .builder \
            .appName(app_name) \
            .getOrCreate()
    except Exception as e:
        logger.error(f"Failed to initialize Spark session: {e}")
        raise


def parse_config(config_path_to_parse):
    """
    Parses a YAML configuration file.

    Arguments:
        config_path_to_parse (str): The path to the YAML configuration file.

    Returns:
        dict: The parsed configuration as a dictionary.

    Raises:
        YAMLError: If there is an error parsing the YAML file.
    """
    with open(config_path_to_parse) as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logger.error(f"Error parsing YAML configuration file: {exc}")
            raise


def main(config_path: str, api: int):
    """
    Initializes Spark, parses the configuration, and calls the appropriate transformation
    function based on the specified API type.

    Arguments:
    config_path (str): the path to the YAML configuration file.
    api (int): The type of API to use for transformations:
        1 for DataFrame API,
        2 for SQL API,
        3 for RDD API.
    """
    spark = initialize_spark("final_proj")
    yaml_config = parse_config(config_path)

    try:
        if api == 1:
            df_transform(spark, yaml_config)
        elif api == 2:
            sql_transform(spark, yaml_config)
        elif api == 3:
            rdd_transform(spark, yaml_config)
        else:
            raise ValueError(
                "Invalid API type specified. Please use 1 for DataFrame API, 2 for SQL API, or 3 for RDD API.")

    except ValueError as ve:
        logger.error(f"Invalid API type specified: {ve}")
        raise
    except Exception as e:
        logger.error(f"An error occurred during transformation: {e}")
        raise


if __name__ == "__main__":
    """
    The main block parses command-line arguments and calls the main function.
    """
    parser = ArgumentParser(prog="final_proj")
    parser.add_argument("--config-path", help="config-path", type=str, required=True)
    parser.add_argument("--api", help="api", type=int, required=True)
    passed_params = parser.parse_args(sys.argv[1:])

    main(passed_params.config_path, passed_params.api)
