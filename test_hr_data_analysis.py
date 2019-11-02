import pyspark
import pyspark.sql
import pytest
import hr_data_analysis

@pytest.mark.old
def test_spark_session_sql0(spark_session):
    test_df = spark_session.read.csv('test_hr_data.csv',inferSchema=True, header=True)
    assert hr_data_analysis.getCountHardWorkingLessPaid(test_df) == 1


@pytest.mark.new
def test_spark_session_sql(spark_session):
    test_df = spark_session.createDataFrame([[1, 3], [2, 4]], "a: int, b: int")
    test_df.registerTempTable('test')

    test_filtered_df = spark_session.sql('SELECT a, b from test where a > 1')
    assert test_filtered_df.count() == 1

@pytest.mark.old
def test_spark_session_sql2(spark_session):
    test_df = spark_session.createDataFrame([[1, 3], [2, 4]], "a: int, b: int")
    test_df.registerTempTable('test')

    test_filtered_df = spark_session.sql('SELECT a, b from test where a > 1')
    assert test_filtered_df.count() == 1

