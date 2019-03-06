from pyspark.sql import SparkSession

import argparse

if __name__ == '__main__': 
    spark = SparkSession.builder.appName('PySpark-App').getOrCreate() 
    print('Session created') 
    emp_data = spark.createDataFrame([(1,2),(3,4),(5,6)],['a','b'])
    print (emp_data.count())
