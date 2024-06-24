from chispa import assert_df_equality
from pyspark.sql.functions import array_sort, col
from transformer.rdd_api_transformer import prepare_awards_rdd, prepare_teams_rdd, prepare_master_rdd, \
    prepare_scoring_teams_rdd, prepare_final_rdd


def test_prepare_awards_rdd(sample_awards_dataframe, sample_prepared_awards_dataframe):
    expected_schema = sample_prepared_awards_dataframe.schema

    actual_df = prepare_awards_rdd(sample_awards_dataframe.select(["playerID", "award"]).rdd).toDF(expected_schema)

    assert_df_equality(actual_df, sample_prepared_awards_dataframe, ignore_nullable=True, ignore_row_order=True)


def test_prepare_teams_rdd(sample_teams_dataframe, sample_prepared_teams_dataframe):
    expected_schema = sample_prepared_teams_dataframe.schema

    actual_df = prepare_teams_rdd(sample_teams_dataframe.select(["tmID", "name"]).rdd).toDF(expected_schema)

    assert_df_equality(actual_df, sample_prepared_teams_dataframe, ignore_nullable=True, ignore_row_order=True)


def test_prepare_masters_rdd(sample_masters_dataframe, sample_prepared_masters_dataframe):
    expected_schema = sample_prepared_masters_dataframe.schema

    actual_df = prepare_master_rdd(
        sample_masters_dataframe.select(["playerID", "coachID", "hofID", "firstName", "lastName"]).rdd).toDF(
        expected_schema)

    assert_df_equality(actual_df, sample_prepared_masters_dataframe, ignore_nullable=True, ignore_row_order=True)


def test_prepare_scoring_teams_rdd(sample_scoring_dataframe, sample_prepared_teams_dataframe,
                                   sample_prepared_scoring_teams_dataframe):
    expected_schema = sample_prepared_scoring_teams_dataframe.schema

    sample_scoring_rdd = sample_scoring_dataframe.rdd
    sample_prepared_teams_rdd = sample_prepared_teams_dataframe.rdd
    actual_df = prepare_scoring_teams_rdd(sample_scoring_rdd, sample_prepared_teams_rdd).toDF(expected_schema)

    assert_df_equality(actual_df, sample_prepared_scoring_teams_dataframe, ignore_nullable=True, ignore_row_order=True)


def test_prepare_final_df(sample_prepared_masters_dataframe, sample_prepared_awards_dataframe,
                          sample_prepared_scoring_teams_dataframe, sample_final_dataframe, spark_session):
    expected_schema = sample_final_dataframe.schema

    sample_prepared_masters_rdd = sample_prepared_masters_dataframe.rdd
    sample_prepared_awards_rdd = sample_prepared_awards_dataframe.rdd
    sample_prepared_scoring_teams_rdd = sample_prepared_scoring_teams_dataframe.rdd
    actual_df = prepare_final_rdd(sample_prepared_masters_rdd, sample_prepared_awards_rdd,
                                  sample_prepared_scoring_teams_rdd, spark_session.sparkContext).toDF(expected_schema) \
        .withColumn("Teams", array_sort(col("Teams")))

    assert_df_equality(actual_df, sample_final_dataframe, ignore_nullable=True, ignore_row_order=True)
