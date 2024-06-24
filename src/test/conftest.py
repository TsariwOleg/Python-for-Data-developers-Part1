"""Setup tests for LCC Sub-divider."""
import os
import sys
# pylint: disable=wrong-import-position, missing-function-docstring

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructField, IntegerType, StringType, StructType, LongType, ArrayType


def initialize_spark(app_name="default_name"):
    """
    Set the Python executable for PySpark to the current Python executable to ensure compatibility
    with the current environment.

    Arguments:
        app_name (str): The name of the Spark application. Defaults to "default_name".

    Returns:
        SparkSession: An initialized SparkSession.
    """
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    return SparkSession.builder.appName(app_name).getOrCreate()


spark = initialize_spark()


@pytest.fixture(scope="session")
def spark_session() -> SparkSession:
    return spark


@pytest.fixture(scope="session")
def sample_awards_dataframe() -> DataFrame:
    data = [
        ("player1", "Art Ross", 2000),
        ("player1", "Art Ross", 2001),
        ("player1", "Vezina", 2003),
        ("player2", "Art Ross", 2000),
        ("player3", "Hart", 2003),
        ("player3", "Art Ross", 2004),
    ]
    return spark.createDataFrame(data, ["playerID", "award", "year"])


@pytest.fixture(scope="session")
def sample_prepared_awards_dataframe() -> DataFrame:
    data = [
        ("player1", 3),
        ("player2", 1),
        ("player3", 2)
    ]
    return spark.createDataFrame(data, ["playerID", "Awards"])


@pytest.fixture(scope="session")
def sample_teams_dataframe() -> DataFrame:
    data = [
        ("tmId1", "Team Name1", 2000),
        ("tmId1", "Team name1", 2001),
        ("tmId1", "Team Name1", 2003),
        ("tmId2", "Team Name2", 2000),
        ("tmId3", "Team Name3", 2003),
    ]
    return spark.createDataFrame(data, ["tmID", "name", "year"])


@pytest.fixture(scope="session")
def sample_prepared_teams_dataframe() -> DataFrame:
    data = [
        ("tmId1", "Team Name1"),
        ("tmId2", "Team Name2"),
        ("tmId3", "Team Name3"),
    ]
    return spark.createDataFrame(data, ["tmID", "name"])


@pytest.fixture(scope="session")
def sample_masters_dataframe() -> DataFrame:
    data = [
        ("player1", None, None, "firstName1", "lastName1"),
        (None, "player2", None, "firstName2", "lastName2  "),
        (None, None, "player3", " firstName3", None),
        ("player4", "coach2", None, "firstName4", "lastName4"),
    ]
    return spark.createDataFrame(data, ["playerID", "coachID", "hofID", "firstName", "lastName"])


@pytest.fixture(scope="session")
def sample_prepared_masters_dataframe() -> DataFrame:
    data = [
        ("player1", "firstName1 lastName1"),
        ("player2", "firstName2 lastName2"),
        ("player3", "firstName3"),
        ("player4", "firstName4 lastName4"),
    ]
    return spark.createDataFrame(data, ["masterId", "userName"])


@pytest.fixture(scope="session")
def sample_scoring_dataframe() -> DataFrame:
    data = [
        ("player1", 2001, "tmId1", 2),
        ("player1", 2002, "tmId1", 1),
        ("player1", 2003, "tmId3", 2),
        ("player1", 2004, "tmId2", 3),
        ("player2", 2000, "tmId2", 1),
        ("player3", 2000, "tmId3", 4),
        ("player4", 2000, "tmId3", 14),
    ]
    return spark.createDataFrame(data, ["playerID", "year", "tmID", "G"])


@pytest.fixture(scope="session")
def sample_prepared_scoring_teams_dataframe() -> DataFrame:
    data = [
        ("player1", 2, "Team Name1"),
        ("player1", 1, "Team Name1"),
        ("player1", 3, "Team Name2"),
        ("player1", 2, "Team Name3"),
        ("player2", 1, "Team Name2"),
        ("player3", 4, "Team Name3"),
        ("player4", 14, "Team Name3"),
    ]
    return spark.createDataFrame(data, ["playerID", "G", "name"])


@pytest.fixture(scope="session")
def sample_final_dataframe() -> DataFrame:
    schema = StructType([
        StructField("Name", StringType(), True),
        StructField("Awards", LongType(), True),
        StructField("Goals", IntegerType(), True),
        StructField("Teams", ArrayType(StringType()), True)
    ])

    data = [
        ("firstName4 lastName4", 0, 14, ["Team Name3"]),
        ("firstName1 lastName1", 3, 8, ["Team Name1", "Team Name2", "Team Name3"]),
        ("firstName2 lastName2", 1, 1, ["Team Name2"]),
        ("firstName3", 2, 4, ["Team Name3"]),
    ]
    return spark.createDataFrame(data, schema)
