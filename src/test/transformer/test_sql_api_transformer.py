from chispa import assert_df_equality
from pyspark.sql.functions import array_sort
from transformer.sql_api_transformer import prepare_awards_df, prepare_teams_df, prepare_master_df, \
    prepare_scoring_teams_df, prepare_final_df


def test_prepare_awards_df(sample_awards_dataframe, sample_prepared_awards_dataframe, spark_session):
    sample_awards_dataframe.createOrReplaceTempView("awards_df_table")
    prepare_awards_df(spark_session)

    actual_df = spark_session.sql("select * from prepared_awards_df_table")
    assert_df_equality(actual_df, sample_prepared_awards_dataframe, ignore_nullable=True, ignore_row_order=True)


def test_prepare_teams_df(sample_teams_dataframe, sample_prepared_teams_dataframe, spark_session):
    sample_teams_dataframe.createOrReplaceTempView("team_df_table")
    prepare_teams_df(spark_session)

    actual_df = spark_session.sql("select * from prepared_teams_df_table")
    assert_df_equality(actual_df, sample_prepared_teams_dataframe, ignore_nullable=True, ignore_row_order=True)


def test_prepare_masters_df(sample_masters_dataframe, sample_prepared_masters_dataframe, spark_session):
    sample_masters_dataframe.createOrReplaceTempView("masters_df_table")
    prepare_master_df(spark_session)

    actual_df = spark_session.sql("select * from prepared_master_df_table")
    assert_df_equality(actual_df, sample_prepared_masters_dataframe, ignore_nullable=True, ignore_row_order=True)


def test_prepare_scoring_teams_df(sample_scoring_dataframe, sample_prepared_teams_dataframe,
                                  sample_prepared_scoring_teams_dataframe, spark_session):
    sample_scoring_dataframe.createOrReplaceTempView("scoring_df_table")
    sample_prepared_teams_dataframe.createOrReplaceTempView("prepared_teams_df_table")
    prepare_scoring_teams_df(spark_session)

    actual_df = spark_session.sql("select * from prepared_scoring_teams_df_table")
    assert_df_equality(actual_df, sample_prepared_scoring_teams_dataframe, ignore_nullable=True, ignore_row_order=True)


def test_prepare_final_df(sample_prepared_masters_dataframe, sample_prepared_awards_dataframe,
                          sample_prepared_scoring_teams_dataframe, sample_final_dataframe, spark_session):
    sample_prepared_masters_dataframe.createOrReplaceTempView("prepared_master_df_table")
    sample_prepared_awards_dataframe.createOrReplaceTempView("prepared_awards_df_table")
    sample_prepared_scoring_teams_dataframe.createOrReplaceTempView("prepared_scoring_teams_df_table")

    prepare_final_df(spark_session)
    actual_df = spark_session.sql("select * from final_df_table").withColumn("Teams", array_sort("Teams"))

    assert_df_equality(actual_df, sample_final_dataframe, ignore_nullable=True, ignore_row_order=True)
