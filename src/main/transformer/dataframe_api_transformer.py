"""
Module provides functions to transform and aggregate hockey player data from CSV files using PySpark.

The module defines functions to prepare individual DataFrames, aggregate the data, and write the final result to a file
using DataFrame API.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import count, collect_set, col, sum, coalesce, concat_ws, trim, first

from utils.io_utils import read_csv_data, write_result


def prepare_awards_df(raw_df: DataFrame):
    """
    Prepare the awards DataFrame by counting the number of awards per playerID.

    Arguments:
    raw_df (DataFrame): The raw awards DataFrame.

    Returns:
    DataFrame: A DataFrame with playerID and awards count.
    """
    return raw_df.groupby("playerID") \
        .agg(count("*").alias("Awards"))


def prepare_teams_df(raw_df: DataFrame):
    """
    Prepare the teams DataFrame with unique team IDs and names.

    Arguments:
    raw_df (DataFrame): The raw teams DataFrame.

    Returns:
    DataFrame: A DataFrame with unique tmID and name.
    """
    return raw_df \
        .select("tmID", "name").dropDuplicates(["tmID"])


def prepare_master_df(raw_df: DataFrame):
    """
    Prepare the master DataFrame by creating a unified identifier and a formatted username.

    Arguments:
    raw_df (DataFrame): The raw master DataFrame

    Returns:
    DataFrame: A transformed DataFrame with 'masterId' (the unified identifier) and
        'userName' (the concatenated and trimmed name).
    """
    return raw_df \
        .withColumn("masterId", coalesce(col("playerID"), col("coachID"), col("hofID"))) \
        .withColumn("userName", trim(concat_ws(" ", col("firstName"), col("lastName")))) \
        .select("masterId", "userName")


def prepare_scoring_teams_df(raw_scoring_df: DataFrame, prepared_teams_df: DataFrame):
    """
    Prepare the scoring teams DataFrame by joining scoring data with team names.

    Arguments:
    raw_scoring_df (DataFrame): The raw scoring DataFrame.
    prepared_teams_df (DataFrame): The prepared teams DataFrame.

    Returns:
    DataFrame: A DataFrame with playerID, goals, and team names.
    """
    return raw_scoring_df \
        .join(prepared_teams_df, raw_scoring_df.tmID == prepared_teams_df.tmID, "left") \
        .select(raw_scoring_df["playerID"], raw_scoring_df["G"], prepared_teams_df["name"])


def prepare_final_df(master_df: DataFrame, prepared_awards_df: DataFrame, prepared_scoring_teams_df: DataFrame):
    """
    Prepare the final DataFrame by joining master, awards, and scoring data,
    then aggregates and orders the data by goals.

    Arguments:
    master_df (DataFrame): The prepared master DataFrame.
    prepared_awards_df (DataFrame): The prepared awards DataFrame.
    prepared_scoring_teams_df (DataFrame): The prepared scoring teams DataFrame.

    Returns:
    DataFrame: The final aggregated and ordered DataFrame.
    """
    return master_df.join(prepared_awards_df, master_df.masterId == prepared_awards_df.playerID, "left") \
        .join(prepared_scoring_teams_df, master_df.masterId == prepared_scoring_teams_df.playerID, "left") \
        .select(master_df["masterId"], master_df["userName"], prepared_awards_df["Awards"],
                prepared_scoring_teams_df["G"],
                prepared_scoring_teams_df["name"]) \
        .groupby("masterId", "Awards") \
        .agg(first(col("userName")).alias("Name"), sum(col("G")).cast("int").alias("Goals"),
             collect_set("name").alias("Teams")) \
        .drop("masterId") \
        .fillna(0, subset=['Awards', 'Goals']) \
        .orderBy(col("Goals").desc()) \
        .limit(10)


def transform(spark: SparkSession, yaml_config: dict):
    """
    Transform data read from CSV files, prepare individual DataFrames, and write the final aggregated DataFrame.

    This function reads data from CSV files as specified in the yaml_config, prepares DataFrames for each data source,
    and calls the prepare functions to process and aggregate the data.
    Finally, it writes the resulting DataFrame to a file.

    Arguments:
    spark (SparkSession): The active Spark session.
    yaml_config (dict): The configuration dictionary containing paths to source data files.
    """
    team_df = read_csv_data(spark, yaml_config.get("source").get("team_path"))
    scoring_df = read_csv_data(spark, yaml_config.get("source").get("scoring_path"))
    masters_df = read_csv_data(spark, yaml_config.get("source").get("master_path"))
    awards_df = read_csv_data(spark, yaml_config.get("source").get("awards_players_path"))

    prepared_master_df = prepare_master_df(masters_df)
    prepared_awards_df = prepare_awards_df(awards_df)
    prepared_team_df = prepare_teams_df(team_df)
    prepared_scoring = prepare_scoring_teams_df(scoring_df, prepared_team_df)

    final_df = prepare_final_df(prepared_master_df, prepared_awards_df, prepared_scoring)

    write_result(final_df, yaml_config.get("product").get("final_df"), ["Name", "Awards", "Goals", "Teams"])
