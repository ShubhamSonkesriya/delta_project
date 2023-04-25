#This file tests the functions from myfunctions.py

from myfunctions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType
from sklearn.datasets import fetch_california_housing

path = '/home/shubham/project/FileStore/tables/housing_dataset'
columnName   = "HouseAge"
columnValue  = 52.0

# Because this file is not a Databricks notebook, you
# must create a Spark session. Databricks notebooks
# create a Spark session for you by default.
spark = SparkSession.builder \
                    .appName('integrity-tests') \
                    .getOrCreate()

raw_data = fetch_california_housing()
schema = StructType([StructField('MedInc', DoubleType(), True), StructField('HouseAge', DoubleType(), True), StructField('AveRooms', DoubleType(), True), StructField('AveBedrms', DoubleType(), True), StructField('Population', DoubleType(), True), StructField('AveOccup', DoubleType(), True), StructField('Latitude', DoubleType(), True), StructField('Longitude', DoubleType(), True)])
df = spark.createDataFrame(raw_data['data'].tolist(), schema)

# Does the table exist?
def test_deltaExists():
  assert deltaExists(path) is True

# Does the column exist?
def test_columnExists():
  assert columnExists(df, columnName) is True

# Is there at least one row for the value in the specified column?
def test_numRowsInColumnForValue():
  assert numRowsInColumnForValue(df, columnName, columnValue) > 0

# Is the table empty?
def test_emptyTable():
  assert emptyTable(path) is False

# Does the schema match?
def test_checkSchema():
    assert checkSchema(schema, path) is True

