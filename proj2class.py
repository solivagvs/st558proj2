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
    Class functions on a Spark SQL style data frame
    """

    def __init__(self, data):
        self.df = data

    @classmethod
    def makespark(cls, spark, path):
        sparkdf = spark.read.load(path, format = "csv")
        return cls(sparkdf)
    
    @classmethod
    def makepdf(cls, spark, pd_dataframe):
        pdf = spark.createDataFrame(pd_dataframe)
        return cls(pdf)