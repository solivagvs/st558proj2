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
    def makespark(cls, spark, path, delimiter):
        sparkdf = spark.read.load(path, \
                                  format = "csv", \
                                  delimiter = delimiter, \
                                  header = True)
        return cls(sparkdf)
    
    @classmethod
    def makepdf(cls, spark, pd_dataframe):
        pdf = spark.createDataFrame(pd_dataframe)
        return cls(pdf)
    
    def numrange(self, column, lower, upper):
        if self[column].dtypes not in ("float", "int", "longint", \
                                       "bigint", "double", "integer"):
            print("The column must be numeric!")
            return self
        else:
            self = self.assign(Result = self[column].between(lower, upper))
            return self

    def strrange(self, column, string):
        if self[column].dtypes in ("float", "int", "longint", \
                                   "bigint", "double", "integer"):
            print("The column must contain character strings!")
            return self
        else:
            self = self.assign(Result = self[column].isin([string]))
            return self

    def nulrange(self, column):
        self = self.assign(Result = self[column].isnull())
        return self
    
    def minmax(self, column = None, groupby = None):
        if groupby ==  None:
            if column != None:
                return F"min: {self.min(column)} max: {self.max()}"
        return