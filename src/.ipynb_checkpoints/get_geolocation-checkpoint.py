import findspark
# Initiate findspark
findspark.init()
# Check the location for Spark
print(findspark.find())


from pyspark.sql import SparkSession
spark = SparkSession.builder\
        .master("local")\
        .appName("bkk-properties")\
        .config("spark.sql.repl.eagerEval.enabled", True)\
        .getOrCreate()


