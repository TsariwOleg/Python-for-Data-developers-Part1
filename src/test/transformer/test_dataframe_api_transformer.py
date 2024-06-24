from chispa import assert_df_equality
from pyspark.sql.functions import array_sort
from transformer.dataframe_api_transformer import prepare_awards_df, prepare_teams_df, prepare_master_df, \
    prepare_scoring_teams_df, prepare_final_df


def test_prepare_awards_df(sample_awards_dataframe, sample_prepared_awards_dataframe):
    actual_df = prepare_awards_df(sample_awards_dataframe)

    assert_df_equality(actual_df, sample_prepared_awards_dataframe, ignore_nullable=True, ignore_row_order=True)


def test_prepare_teams_df(sample_teams_dataframe, sample_prepared_teams_dataframe):
    actual_df = prepare_teams_df(sample_teams_dataframe)

    assert_df_equality(actual_df, sample_prepared_teams_dataframe, ignore_nullable=True, ignore_row_order=True)


def test_prepare_masters_df(sample_masters_dataframe, sample_prepared_masters_dataframe):
    actual_df = prepare_master_df(sample_masters_dataframe)

    assert_df_equality(actual_df, sample_prepared_masters_dataframe, ignore_nullable=True, ignore_row_order=True)


def test_prepare_scoring_teams_df(sample_scoring_dataframe, sample_prepared_teams_dataframe,
                                  sample_prepared_scoring_teams_dataframe):
    actual_df = prepare_scoring_teams_df(sample_scoring_dataframe, sample_prepared_teams_dataframe)

    assert_df_equality(actual_df, sample_prepared_scoring_teams_dataframe, ignore_nullable=True, ignore_row_order=True)


def test_prepare_final_df(sample_prepared_masters_dataframe, sample_prepared_awards_dataframe,
                          sample_prepared_scoring_teams_dataframe, sample_final_dataframe):
    actual_df = prepare_final_df(sample_prepared_masters_dataframe, sample_prepared_awards_dataframe,
                                 sample_prepared_scoring_teams_dataframe).withColumn("Teams", array_sort("Teams"))

    assert_df_equality(actual_df, sample_final_dataframe, ignore_nullable=True, ignore_row_order=True,
                       ignore_column_order=True)
