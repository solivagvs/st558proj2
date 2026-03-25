# Name: Bryan Sandor
# Class: Stat 558
# Title: Project 2 Class File

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql.types import *
import pandas as pd

class SparkDataCheck:
    """
    Class functions on a Spark SQL style data frame.
    """

    def __init__(self, data):
        self.df = data

    @classmethod
    def makespark(cls, spark, path , delimiter):
        sparkdf = spark.read.load(path, \
                                  format = "csv", \
                                  delimiter = delimiter, \
                                  header = True)
        return cls(sparkdf)
    
    @classmethod
    def makepdf(cls, spark, pd_dataframe):
        pdf = spark.createDataFrame(pd_dataframe)
        return cls(pdf)
    
    def numrange(df, column, lower, upper):
        if df[column].dtypes not in ("float", "int", "longint", \
                                    "bigint", "double", "integer"):
            print("The column must be numeric!")
            return df[column]
        else:
            return df.assign(Result = df[column].between(lower, upper))