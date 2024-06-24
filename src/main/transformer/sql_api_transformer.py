"""
Module provides functions to transform and aggregate hockey player data from CSV files using PySpark.

The module defines functions to prepare individual DataFrames, aggregate the data, and write the final result to a file
using SQL.
"""

from pyspark.sql import SparkSession

from utils.io_utils import read_csv_data, write_result


def prepare_awards_df(spark: SparkSession):
    """
    Prepare the awards DataFrame by counting the number of awards per playerID.

    This function uses a SQL query to count the number of awards per playerID
    from the 'awards_df_table' temporary view, and creates or replaces a
    temporary view 'prepared_awards_df_table' with the result.

    Arguments:
    spark (SparkSession): The active Spark session.
    """
    spark.sql("select playerID, count(*) as Awards from awards_df_table group by playerID") \
        .createOrReplaceTempView("prepared_awards_df_table")


def prepare_teams_df(spark: SparkSession):
    """
    Prepare the teams DataFrame with unique team IDs and names.


    This function uses a SQL query to select distinct team IDs and names
    from the 'team_df_table' temporary view, and creates or replaces a
    temporary view 'prepared_teams_df_table' with the result.

    Arguments:
    spark (SparkSession): The active Spark session.
    """
    spark.sql("select distinct(tmID) , name  from team_df_table") \
        .createOrReplaceTempView("prepared_teams_df_table")


def prepare_teams_df(spark: SparkSession):
    """
    Prepare the teams DataFrame with unique team IDs and names.

    This function uses a SQL query to select distinct team IDs and names
    from the 'team_df_table' temporary view, and creates or replaces a
    temporary view 'prepared_teams_df_table' with the result.

    Arguments:
    spark (SparkSession): The active Spark session.
    """
    spark.sql("select tmID, first(name) as name from team_df_table group by tmID") \
        .createOrReplaceTempView("prepared_teams_df_table")


def prepare_master_df(spark: SparkSession):
    """
    Prepare the master DataFrame by creating a unified identifier and a formatted username.

    This function executes a SQL query to perform the following transformations:
    1. Coalesces 'playerID', 'coachID', and 'hofID' into a single column 'masterId'.
    2. Concatenates 'firstName' and 'lastName' into a single column 'userName', trimming any extra spaces and
        replacing null values with empty strings.
    The result is then used to create or replace a temporary view named 'prepared_master_df_table'.

    Arguments:
    spark (SparkSession): The active Spark session.
    """
    spark.sql(
        "select coalesce(playerID, coachID, hofID) as masterId, "
        "trim(concat(coalesce(firstName, ''), ' ', coalesce(lastName, ''))) as userName "
        "from masters_df_table"
    ).createOrReplaceTempView("prepared_master_df_table")


def prepare_scoring_teams_df(spark: SparkSession):
    """
    Prepare the scoring teams DataFrame by joining scoring data with team names.

    This function uses a SQL query to join the 'scoring_df_table'
    with the 'prepared_teams_df_table', and creates or replaces a
    temporary view 'prepared_scoring_teams_df_table' with the result.

    Arguments:
    spark (SparkSession): The active Spark session.
    """
    spark.sql("select scoring_df_table.playerID , scoring_df_table.G , prepared_teams_df_table.name  "
              "from scoring_df_table "
              "left join prepared_teams_df_table "
              "on scoring_df_table.tmID = prepared_teams_df_table.tmID") \
        .createOrReplaceTempView("prepared_scoring_teams_df_table")


def prepare_final_df(spark: SparkSession):
    """
    Prepare the final DataFrame by joining master, awards, and scoring data,
    then aggregates and orders the data by goals.

    This function uses a SQL query to join the 'masters_df_table'
    with 'prepared_awards_df_table' and the 'prepared_scoring_teams_df_table'.
    Then it aggregates the data by playerID and total awards, and collects the set of teams and total goals.
    The result is ordered by goals in descending order and limited to the top 10 players
    and finally creates or replaces a temporary view 'prepared_scoring_teams_df_table' with the result

    Arguments:
    spark (SparkSession): The active Spark session.
    """
    spark.sql(
        "select first(prepared_master_df_table.userName) as Name, coalesce(Awards, 0) AS Awards, "
        "coalesce(cast(sum(G) as integer), 0) AS Goals, collect_set(name)  as Teams "
        "from prepared_master_df_table "
        "left join prepared_awards_df_table "
        "on prepared_master_df_table.masterId = prepared_awards_df_table.playerID "
        "left join prepared_scoring_teams_df_table "
        "on prepared_master_df_table.masterId = prepared_scoring_teams_df_table.playerID "
        "group by prepared_master_df_table.masterId , prepared_awards_df_table.Awards "
        "order by Goals desc "
        "limit 10 ") \
        .createOrReplaceTempView("final_df_table")


def transform(spark: SparkSession, yaml_config: dict):
    """
    Transform data read from CSV files, prepare individual DataFrames, and write the final aggregated DataFrame.

    This function reads data from CSV files as specified in the yaml_config, prepares temp view for each data source,
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

    team_df.createOrReplaceTempView("team_df_table")
    scoring_df.createOrReplaceTempView("scoring_df_table")
    masters_df.createOrReplaceTempView("masters_df_table")
    awards_df.createOrReplaceTempView("awards_df_table")

    prepare_master_df(spark)
    prepare_awards_df(spark)
    prepare_teams_df(spark)
    prepare_scoring_teams_df(spark)

    prepare_final_df(spark)
    final_df = spark.sql("select * from final_df_table")

    write_result(final_df, yaml_config.get("product").get("final_df"))
