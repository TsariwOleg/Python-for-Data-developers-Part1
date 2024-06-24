"""
Module provides functions to transform and aggregate hockey player data from CSV files using PySpark.

The module defines functions to prepare individual DataFrames, aggregate the data, and write the final result to a file
using RDD.
"""

from pyspark import RDD

from utils.io_utils import read_csv_data, write_result


def prepare_awards_rdd(raw_rdd: RDD):
    """
    Prepare the awards RDD by grouping and counting the number of awards per playerID.

    Arguments:
    raw_rdd (RDD): The raw awards RDD.

    Returns:
    RDD: An RDD with playerID and awards count.
    """
    return raw_rdd.groupBy(lambda x: x.playerID).mapValues(len)


def prepare_teams_rdd(raw_rdd: RDD):
    """
    Prepare the teams RDD with unique team IDs and names.

    Arguments:
    raw_rdd (RDD): The raw teams RDD.

    Returns:
    RDD: An RDD with unique tmID and name.
    """
    return raw_rdd.map(lambda x: (x.tmID, x.name)).groupByKey().mapValues(lambda x: list(x)[0])


def prepare_master_rdd(raw_rdd: RDD):
    """
    Prepare the master RDD by coalescing playerID, coachID, and hofID into a single ID (masterId).

    Arguments:
    raw_rdd (RDD): The raw master RDD.

    Returns:
    RDD: An RDD with a new column masterId.
    """

    def coalesce(row):
        return row.playerID or row.coachID or row.hofID, " ".join(
            [str(x) if x is not None else '' for x in (row.firstName, row.lastName)]).strip()

    return raw_rdd.map(coalesce).filter(lambda x: x[0] is not None)


def prepare_scoring_teams_rdd(raw_scoring_rdd: RDD, prepared_teams_rdd: RDD):
    """
    Prepare the scoring teams RDD by joining scoring data with team names.

    Arguments:
    raw_scoring_rdd (RDD): The raw scoring RDD.
    prepared_teams_rdd (RDD): The prepared teams RDD.

    Returns:
    RDD: An RDD with playerID, goals, team names, and team ID.
    """
    return raw_scoring_rdd.map(lambda x: (x.tmID, (x.playerID, x.G))) \
        .leftOuterJoin(prepared_teams_rdd) \
        .map(lambda x: (x[1][0][0], x[1][0][1], x[1][1]))


def prepare_final_rdd(master_rdd, prepared_awards_rdd: RDD, prepared_scoring_teams_rdd: RDD, sparkContext):
    """
    Prepare the final RDD by joining master, awards, and scoring data,
    then aggregates and orders the data by goals.

    Arguments:
    master_rdd (RDD): The prepared master RDD.
    prepared_awards_rdd (RDD): The prepared awards RDD.
    prepared_scoring_teams_rdd (RDD): The prepared scoring teams RDD.
    sparkContext (SparkContext): The active Spark context.

    Returns:
    RDD: The final aggregated and ordered RDD.
    """

    def explode_list(awards_count, list_to_explode):
        team_name_set = set()
        total_score = 0
        user_name = ""
        for i in list_to_explode:
            if i[0][1] is not None:
                user_name = i[0][0][0]
                total_score += int(i[0][1][0]) if i[0][1][0] is not None else 0
                team_name_set.add(i[0][1][1])
        return user_name, awards_count if awards_count is not None else 0, total_score, list(team_name_set)

    master_awards_scoring_rdd = master_rdd.leftOuterJoin(prepared_awards_rdd) \
        .leftOuterJoin(prepared_scoring_teams_rdd.map(lambda x: (x[0], x[1:]))) \
        .map(lambda x: ((x[0], x[1][0][1]), (x[1:]))) \
        .groupByKey() \
        .mapValues(list) \
        .map(lambda x: explode_list(x[0][1], x[1])) \
        .sortBy(lambda x: x[2], ascending=False) \
        .take(10)

    return sparkContext.parallelize(master_awards_scoring_rdd)


def transform(spark, yaml_config):
    """
    Transform data read from CSV files, prepare individual RDDs, and write the final aggregated RDD.

    This function reads data from CSV files as specified in the yaml_config, prepares RDDs for each data source,
    and calls the prepare functions to process and aggregate the data. Finally, it writes the resulting RDD to a file.

    Arguments:
    spark (SparkSession): The active Spark session.
    yaml_config (dict): The configuration dictionary containing paths to source data files.
    """
    team_rdd = read_csv_data(spark, yaml_config.get("source").get("team_path")).select(["tmID", "name"]).rdd
    scoring_rdd = read_csv_data(spark, yaml_config.get("source").get("scoring_path")).select(
        ["playerID", "tmID", "G"]).rdd
    masters_rdd = read_csv_data(spark, yaml_config.get("source").get("master_path")).select(
        ["playerID", "coachID", "hofID", "firstName", "lastName"]).rdd
    awards_rdd = read_csv_data(spark, yaml_config.get("source").get("awards_players_path")).select(
        ["playerID", "award"]).rdd

    prepared_master_rdd = prepare_master_rdd(masters_rdd)
    prepared_awards_rdd = prepare_awards_rdd(awards_rdd)
    prepared_team_rdd = prepare_teams_rdd(team_rdd)
    prepared_scoring = prepare_scoring_teams_rdd(scoring_rdd, prepared_team_rdd)

    final_rdd = prepare_final_rdd(prepared_master_rdd, prepared_awards_rdd, prepared_scoring, spark.sparkContext)
    final_df = final_rdd.toDF(["Name", "Awards", "Goals", "Teams"])

    write_result(final_df, yaml_config.get("product").get("final_df"))
