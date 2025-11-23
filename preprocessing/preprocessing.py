"""
Preprocessing module for the cloud
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import FloatType
import datetime

spark = SparkSession.builder.appName("Preprocessing").getOrCreate()
sc = spark.sparkContext

# load the dataset
df = spark.read.csv("subset.csv", header=True, inferSchema=True)

# remove the following columns: urls, track_id, data, available markets, id, and date
# also remove title, artist, album, region, and name because they are strings not worth embedding for now
# also remove chart because I don't think there are enough charts for this to be relevant
# finally, remove index because dataframes already have an index
columns_to_remove = [
    "urls", "track_id", "data", "available_markets", "id", "url", "date",
    "title", "artist", "album", "region", "name", "chart"
]
df = df.drop(*columns_to_remove)

# rename the unnamed column to index
df = df.withColumnRenamed("Unnamed: 0", "index_col")

# convert the date columns into a float representing the year
def date_to_year(date):
    try:
        return float(date.year)
    except:
        return None
    
date_to_year_udf = F.udf(date_to_year, FloatType())
df = df.withColumn("date", date_to_year_udf(F.col("release_date")))
df = df.drop("release_date")

# convert the trend column into a scale from 0-2
def trend_to_scale(trend):
    if trend == "MOVE_UP":
        return 2.0
    elif trend == "MOVE_DOWN":
        return 0.0
    else:
        return 1.0

trend_to_scale_udf = F.udf(trend_to_scale, FloatType())
df = df.withColumn("trend", trend_to_scale_udf(F.col("trend")))

# remove explicit content, and then remove the explicit column
df = df.filter(F.col("explicit") == False)
df = df.drop("explicit")

# convert the rank column to a float
df = df.withColumn("rank", F.col("rank").cast(FloatType()))

df.show(5)

"""
Next steps:
1. ensure we don't have duplicates, maybe we can should save like the song title or something to do that
2. save the preprocessed dataframe to a parquet or csv - make sure we remove the index column for that as well
3. save just the index column, title, artist to a separate csv for reference and creating the language detection
4. test this on the small data before we do it on the big data
"""