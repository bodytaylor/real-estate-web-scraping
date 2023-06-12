# find spark environment
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract


import os
import googlemaps
from dotenv import load_dotenv

spark = SparkSession.builder\
        .master("local")\
        .appName("bkk-properties")\
        .config("spark.sql.repl.eagerEval.enabled", True)\
        .getOrCreate()

# Import csv
df = spark.read.csv('data/bangkok_condo.csv', header = True)

# Extract a title from listing title, that make it easier for google API in later process
df = df.withColumn("extracted_title", regexp_extract("title", r"for sale at (.*)", 1))

# Create view table 
df.createOrReplaceTempView("view")

# Select Distinct title for save cost when query with google map API
unique_name_df = spark.sql("SELECT DISTINCT extracted_title, location FROM view").toPandas()
location_list = list(unique_name_df['location'])
property_name_list = list(unique_name_df['extracted_title'])

# initialize google map api client
load_dotenv()
map_api = os.getenv("API_KEY")
gm = googlemaps.Client(key=map_api)

# Get geolocation from google API by passing property name and location
def get_geolocation(p_name, location):
  try:
    search_name = f'{p_name} {location}'
    viewport = {'northeast': {'lat': 13.955111, 'lng': 100.938408},
  'southwest': {'lat': 13.4940881, 'lng': 100.3278136}}
    geo_code_result = gm.geocode(search_name, bounds=viewport)[0]
    geo_result = geo_code_result['geometry']['location']
    geo_result["property_name"] = p_name
  except IndexError as e:
    geo_result = {'lat': 'N/A', 'lng': 'N/A', 'property_name': f'{p_name}'}
  return geo_result

# Save result to CSV file
from csv import writer
file_exists = os.path.isfile('/code/data/property_geo_location.csv')
with open('/code/data/property_geo_location.csv', 'a', newline='') as f:
    thewriter = writer(f)
    if not file_exists:  # write header row only once
        heading = ['lat', 'lng', 'property_name']    
        thewriter.writerow(heading)
    for i in range(len(property_name_list)):
      geolocation = get_geolocation(property_name_list[i], location_list[i])
      information = [geolocation['lat'], geolocation['lng'], geolocation['property_name']]
      thewriter.writerow(information)

# Load the Geolocation from previous CSV file
df_geo = spark.read.csv('/code/data/property_geo_location.csv', header = True)

# Create function to find the nearest train station and Subway Station from the Property
## Warning this function is using nearby search from googlemap.
## The cost of using this function is 40$ per 1000 queries.

# Trainstation
def find_nearest_trainstation(lat, lng):
  try:
    result = gm.places_nearby(location=f'{lat},{lng}', type='train_station', rank_by='distance')
    name = result['results'][0]['name']
    lat = result['results'][0]['geometry']['location']['lat']
    lng = result['results'][0]['geometry']['location']['lng']
  except IndexError as e:
    name = 'N/A'
    lat = "N/A"
    lng = "N/A"
  return name, lat, lng

# Subway
def find_nearest_subway_station(lat, lng):
  try:
    result = gm.places_nearby(location=f'{lat},{lng}', type='subway_station', rank_by='distance')
    name = result['results'][0]['name']
    lat = result['results'][0]['geometry']['location']['lat']
    lng = result['results'][0]['geometry']['location']['lng']
  except IndexError as e:
    name = 'N/A'
    lat = "N/A"
    lng = "N/A"
  return name, lat, lng


## import package for create User Defined Function for applying on the dataframe
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Create UDF with schema
schema = StructType([
    StructField("name", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True)
])

find_nearest_trainstation_udf = udf(find_nearest_trainstation, schema)
find_nearest_subway_station_udf = udf(find_nearest_subway_station, schema)

# Appply function to Dataframe
df_geo = df_geo.withColumn("trainstation_info", find_nearest_trainstation_udf(df_geo["lat"], df_geo["lng"]))
# Split result into seperate columns
df_geo = df_geo.withColumn("trainstation_name", df_geo["trainstation_info"]["name"])
df_geo = df_geo.withColumn("trainstation_lat", df_geo["trainstation_info"]["lat"])
df_geo = df_geo.withColumn("trainstation_lng", df_geo["trainstation_info"]["lng"])
df_geo = df_geo.drop('trainstation_info')

# Do the same for Subway
df_geo = df_geo.withColumn("subway_station_info", find_nearest_subway_station_udf(df_geo["lat"], df_geo["lng"]))
df_geo = df_geo.withColumn("subway_station_name", df_geo["subway_station_info"]["name"])
df_geo = df_geo.withColumn("subway_station_lat", df_geo["subway_station_info"]["lat"])
df_geo = df_geo.withColumn("subway_station_lng", df_geo["subway_station_info"]["lng"])
df_geo = df_geo.drop('subway_station_info')


# Create Function for finding the distance from the property to public transport.
# The function return walking distance in meters

# distance from trainstation
def distance_from_train_station(lat, lng, trainstation_lat, trainstation_lng):
  result = gm.distance_matrix(origins=f'{lat},{lng}', destinations=f'{trainstation_lat},{trainstation_lng}', mode='walking')
  if result['rows'][0]['elements'][0]['status'] == 'OK' and trainstation_lat != 'null':
    distance_m = result['rows'][0]['elements'][0]['distance']['value']
  else:
    distance_m = 'null'
  return distance_m

# distance from subway_station
def distance_from_subway_station(lat, lng, subway_station_lat, subway_station_lng):
  result = gm.distance_matrix(origins=f'{lat},{lng}', destinations=f'{subway_station_lat},{subway_station_lng}', mode='walking')
  if result['rows'][0]['elements'][0]['status'] == 'OK' and subway_station_lat != 'null':
    distance_m = result['rows'][0]['elements'][0]['distance']['value']
  else:
    distance_m = 'null'
  return distance_m

# Create UDF for apply to the dataframe
distance_from_train_station_udf = udf(distance_from_train_station)
distance_from_subway_station_udf = udf(distance_from_subway_station)

# Apply the UDF to Data Frame
df_geo = df_geo.withColumn("subway_station_distance_meter", distance_from_subway_station_udf(df_geo["lat"], df_geo["lng"], df_geo["subway_station_lat"], df_geo["subway_station_lng"]))
df_geo = df_geo.withColumn("train_station_distance_meter", distance_from_train_station_udf(df_geo["lat"], df_geo["lng"], df_geo["trainstation_lat"], df_geo["trainstation_lng"]))

# Save result to csv file
df_geo.write.option("header",True).csv('data/property_public_transport_options.csv')