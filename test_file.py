import pyspark
import pyspark.sql
import pytest
from spark_utilities import df_count


testdata = [
        ([[1, 3], [2, 4]]),
        ([[1, 3], [2, 4], [3,3]])
        ]
@pytest.mark.parametrize("a",testdata)
def test_spark_session_dataframe(spark_session,a):
    test_df = spark_session.createDataFrame(a, "a: int, b: int")
    assert type(test_df) == pyspark.sql.dataframe.DataFrame
    assert df_count(test_df) == 2


@pytest.mark.new
def test_spark_session_sql(spark_session):
    test_df = spark_session.createDataFrame([[1, 3], [2, 4]], "a: int, b: int")
    test_df.registerTempTable('test')

    test_filtered_df = spark_session.sql('SELECT a, b from test where a > 1')
    assert test_filtered_df.count() == 1

@pytest.mark.webtest
def test_spark_session_s(spark_session):
    test_df = spark_session.createDataFrame([[1, 3], [2, 4]], "a: int, b: int")
    test_df.registerTempTable('test')

    test_filtered_df = spark_session.sql('SELECT a, b from test where a > 1')
    assert test_filtered_df.count() == 1
