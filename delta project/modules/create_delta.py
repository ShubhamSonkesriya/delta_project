from modules.spark import spark

#Initializing Spark session
spark = spark('delta_project')

#A function create delta table from the list of data(ls), schema of data, path to save to, and mode e.g. overwrite, append etc.
def create_delta(ls, schema, path, mode):
    try:
        df = spark.createDataFrame(ls, schema)
        df.write.mode(mode).format('delta').save(path)
        # df.write.mode(mode).format('delta').saveAsTable('table_name') #table_name="housing_dataset" as table name
        return True
    except Exception as e:
        print(e.message)
        return False