from chispa import assert_df_equality
from pyspark.sql import SparkSession, DataFrame

from utils.io_utils import read_csv_data


def test_read_csv_data(spark_session):
    data = [
        ("1", "2", "3", "row1_4"),
        ("1", "2", "3", "row2_4"),
    ]
    sample_input_dataframe = spark_session.createDataFrame(data, ["colName1", "colName2", "colName3", "colName4"])

    input_file_path = 'src/test/resources/test.csv'
    actual_df = read_csv_data(spark_session, input_file_path)

    assert_df_equality(actual_df, sample_input_dataframe)
