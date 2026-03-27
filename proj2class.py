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

    neg_infty = float("-inf") # create parameters for pos/neg infinity
    infty = float("inf")
    
    def __init__(self, data):
        self.df = data

    # used to read in a CSV file as a spark dataframe
    @classmethod
    def read_spark(cls, spark, path, delimiter):
        sparkdf = spark.read.load(path, \
                                  format = "csv", \
                                  delimiter = delimiter, \
                                  inferSchema = True,
                                  header = True)
        return cls(sparkdf)

    # used to convert a pandas dataframe to spark
    @classmethod
    def convert_sdf(cls, spark, pd_dataframe):
        sparkdf = spark.createDataFrame(pd_dataframe)
        return cls(sparkdf)

    # If the column data is numeric, returns a dataframe with an extra column
    # of boolean values; range is all real numbers unless bounds are provided
    def numrange(self, column, lower = neg_infty, upper = infty):
        if self.df.select(column).dtypes[0][1] in ("float", "int", "longint", \
                                                "bigint", "double", "integer"):
                self = self.df.withColumn(colName = "Result", \
                                          col = F.col(column) \
                                          .between(lower, upper))
                return self                                  
        else:
            print("The column must be numeric!")
            return self

    # If the column data is string-type, returns a dataframe with an extra
    # column of boolean values, whether the string in a cell matches given str
    def strrange(self, column, string):
        if self.df.select(column).dtypes[0][1] in ("float", "int", "longint", \
                                                "bigint", "double", "integer"):
            print("The column must contain strings!")
            return self
        else:
            self = self.df.withColumn(colName = "Result", \
                                      col = F.col(column) \
                                      .contains(string))
            return self
        
    # For a column, returns a dataframe with an extra column of boolean values,
    # whether a value in a cell is missing (NULL), dependent upon whether the
    # column contains strings or values
    def nulrange(self, column):
        if self.df.select(column).dtypes[0][1] in ("float", "int", "longint", \
                                                "bigint", "double", "integer"):
            self = self.df.withColumn(colName = "Result", \
                                      col = F.col(column) \
                                      .contains("NaN"))
            return self
        else:
            self = self.df.withColumn(colName = "Result", 
                                      col = F.col(column) \
                                      .isNull())
            return self
    
    # For a specified column and groupby column, computes the min and max
    # values of a numeric column and returns the results as a Pandas dataframe
    def minmax(self, column = None, groupby = None):
        if self.df.select(column).dtypes[0][1] in ("float", "int", "longint", \
                                                "bigint", "double", "integer"):
            if groupby == None:
                x = self.df.agg({column : "min"}).toPandas()
                y = self.df.agg({column : "max"}).toPandas()
                z = x.merge(y, how = "right", \
                            left_index = True, right_index = True)
                return z
            else:
                x = self.df.groupby(groupby).agg({column : "min"}).toPandas()
                y = self.df.groupby(groupby).agg({column : "max"}).toPandas()
                z = x.merge(y)
                return z
        else:
            print("The column must be numeric!")
            return
    
# I WAS NEARLY DONE WITH THE ASSIGNMENT BEFORE I REALIZED I HAD COMPLETELY 
# MISSED THE INTENT. BELOW IS THE WORK I HAD COMPLETED UNTIL THAT POINT WHICH
# UNFORTUNATELY ONLY WORKS WITH PANDAS DATAFRAMES
#
#    def numrange(self, column, lower, upper):
#        if self[column].dtypes not in ("float", "int", "longint", \
#                                       "bigint", "double", "integer"):
#            print("The column must be numeric!")
#            return self
#        else:
#            self = self.assign(Result = self[column].between(lower, upper))
#            return self
#
#    def strrange(self, column, string):
#        if self[column].dtypes in ("float", "int", "longint", \
#                                   "bigint", "double", "integer"):
#            print("The column must contain character strings!")
#            return self
#        else:
#            self = self.assign(Result = self[column].isin([string]))
#            return self
#
#    def nulrange(self, column):
#        self = self.assign(Result = self[column].isnull())
#        return self
#
# I REALIZED MY MISTAKE AT THIS METHOD
#
#    def minmax(self, column = None, groupby = None):
#        if groupby ==  None:
#            if column != None:
#                return pd.DataFrame(self.select(F.min(column)), \
#                                    self.select(F.max(column)))
#        return