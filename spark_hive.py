from os.path import expanduser, join, abspath

from pyspark.sql import SparkSession
from pyspark.sql import Row

# warehouse_location points to the default location for managed databases and tables
warehouse_location = '/home/awantik/spark-warehouse'

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# spark is an existing SparkSession
spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
spark.sql("LOAD DATA LOCAL INPATH '/home/awantik/packages/spark-2.4.3-bin-hadoop2.7/examples/src/main/resources/kv1.txt' INTO TABLE src")
df = spark.sql("SELECT * FROM src")
df.show()

spark.sql("CREATE TABLE IF NOT EXISTS newsrc (key INT, value STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")
spark.sql("LOAD DATA LOCAL INPATH '/home/awantik/emp.txt' INTO TABLE newsrc")
df2 = spark.sql("SELECT * FROM newsrc")
df2.show()

df = df2.unionAll(df)
df.show()
