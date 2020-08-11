import pandas as pd
from pyspark.sql import SparkSession

print('pyspark found')

spark = SparkSession.builder.appName('weather').getOrCreate()

"""SPARK DOESNT WORK!!!"""


df = spark.read.csv("C:/Users/SZ/PycharmProjects/berlin_weather/ber_weather_combined_1876_2019.csv",
                    inferSchema=True, header=True)