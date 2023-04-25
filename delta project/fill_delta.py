from sklearn.datasets import fetch_california_housing
from pyspark.sql.types import StructType,StructField, DoubleType
from modules.create_delta import create_delta
from modules.spark import spark

#Initializing Spark session
spark = spark('delta_project')

#Path to save delta to
path = "/home/shubham/project/FileStore/tables/housing_dataset"

raw_data = fetch_california_housing()

schema = StructType([StructField('MedInc', DoubleType(), True), StructField('HouseAge', DoubleType(), True), StructField('AveRooms', DoubleType(), True), StructField('AveBedrms', DoubleType(), True), StructField('Population', DoubleType(), True), StructField('AveOccup', DoubleType(), True), StructField('Latitude', DoubleType(), True), StructField('Longitude', DoubleType(), True)])

#calling create delta function to store data
create_delta(raw_data['data'].tolist(), schema, path, 'overwrite')