# This file contains test functions

import pyspark
from delta import *
from pyspark.sql.functions import col
import os


# Because this file is not a Databricks notebook, you
# must create a Spark session. Databricks notebooks
# create a Spark session for you by default.


builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

#Checks if delta schema matches with the schema provided
def checkSchema(schema, path):
    df = spark.read.format('delta').load(path)
    # df = spark.read.format('delta').load('table_name') #table_name = 'housing_dataset'
   
    if (df.schema==schema):
        return True
    else:
        return False

#Checks if the delta table is empty
def emptyTable(path):
    df = spark.read.format('delta').load(path)
    # df = spark.read.format('delta').load('table_name') #table_name = 'housing_dataset'
    if df.count() == 0:
        return True
    else:
        return False

# Does the specified table exist in the specified database?
def deltaExists(path):
    if len(os.listdir(path)) == 0:
        return False
    else:    
        return True
    
# Does the specified column exist in the given DataFrame?
def columnExists(dataFrame, columnName):
    if columnName in dataFrame.columns:
        return True
    else:
        return False

# How many rows are there for the specified value in the specified column
# in the given DataFrame?
def numRowsInColumnForValue(dataFrame, columnName, columnValue):
    df = dataFrame.filter(col(columnName) == columnValue)

    return df.count()