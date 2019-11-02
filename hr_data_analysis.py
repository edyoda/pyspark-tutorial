from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def getCountHardWorkingLessPaid(hr_data):
    return hr_data[(hr_data.satisfaction_level > .9) & (hr_data.salary == "low")].count()

def increaseSalary(hr_data):
    hr_data = hr_data.withColumn('ActualSalary', hr_data.last_evaluation * 10000)
    hr_data = hr_data.withColumn('multifactor', 
                             F.when(hr_data.salary == "low",1)
                              .when(hr_data.salary == "medium",2)
                              .otherwise(3))
    hr_data = hr_data.withColumn('ActualSalary', hr_data.ActualSalary * hr_data.multifactor)
    hr_data = hr_data.drop('multifactor')


if __name__ == '__main__':

    spark = SparkSession.builder.appName('HR-Data-Analysis').getOrCreate()
    print('Session created')

    hr_data = spark.read.csv('hr_data.csv', inferSchema=True, header=True)
 
    hr_data = hr_data.withColumnRenamed("sales","department")
    
    hr_data = hr_data.cache()
    count = getCountHardWorkingLessPaid(hr_data)
    print ('Count of hardworking & Less Paid folks ',count)

    increaseSalary(hr_data)
    print (hr_data.show())
