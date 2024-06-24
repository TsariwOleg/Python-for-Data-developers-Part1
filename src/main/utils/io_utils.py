"""
Module provides functions to read data from CSV files into PySpark DataFrames
and write the resulting DataFrames to files based on a provided configuration.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import concat_ws, col
from pyspark.sql.types import ArrayType

from utils.logging_utils import setup_logging

logger = setup_logging(__name__)


def read_csv_data(spark: SparkSession, path: str) -> DataFrame:
    """
    Read data from a CSV file into a DataFrame.

    Arguments:
    spark (SparkSession): The active Spark session.
    path (str): The path to the CSV file.

    Returns:
    DataFrame: The DataFrame containing the data read from the CSV file.
    """
    try:
        return spark.read.csv(path, header=True)
    except Exception as e:
        logger.error(f"Error reading data from {path}: {e}")
        raise


def write_result(df: DataFrame, product_config: dict, column_order: list = None):
    """
    Write the resulting DataFrame to a file based on the provided configuration.

    This function writes the DataFrame to a single file using the specified format,
    mode, and options provided in the product_config dictionary. If the format is 'csv',
    it converts any columns of ArrayType to strings with elements separated by commas.
    Optionally, it reorders the columns of the DataFrame before writing.

    Arguments:
    df (DataFrame): The DataFrame to be written to a file.
    product_config (dict): The configuration dictionary containing write options.
    column_order (list, optional):  A list of column names to reorder the DataFrame before writing.
    """
    try:
        file_format = product_config.get("format")

        if file_format == "csv":
            for field in df.schema.fields:
                if isinstance(field.dataType, ArrayType):
                    df = df.withColumn(field.name, concat_ws(", ", col(field.name)))

        if column_order is not None:
            df = df.select(*column_order)

        df.coalesce(1) \
            .write \
            .options(**product_config.get("options")) \
            .format(file_format) \
            .mode(product_config.get("mode")) \
            .save(product_config.get("path"))

    except Exception as e:
        logger.error(f"Error writing data to {product_config.get('path')}: {e}")
        raise
