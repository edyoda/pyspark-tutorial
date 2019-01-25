from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as func
import argparse




if __name__ == '__main__':


    parser = argparse.ArgumentParser()

    parser.add_argument("--input",  help="Path to file/directory for training data", required=True)
    params = parser.parse_args()

    spark = SparkSession \
        .builder \
        .appName('PySpark-App') \
        .getOrCreate()

    print('Session created')

    try:
        data = spark.read.csv(params.input,header=True,inferSchema=True)
        print ('Final Count ', data.count())

    finally:
        spark.stop()