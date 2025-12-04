"""
Preprocessing module for the cloud
"""

import sys
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import FloatType
import datetime

# Check arguments
if len(sys.argv) != 3:
    print("Usage: preprocessing.py <input_file> <output_file>", file=sys.stderr)
    sys.exit(-1)

file_path = sys.argv[1]
output_path = sys.argv[2]

spark = SparkSession.builder.appName("Preprocessing").getOrCreate()
sc = spark.sparkContext

# load the dataset
df = spark.read.csv(file_path, header=True, inferSchema=True)

# remove the following columns: urls, track_id, data, available markets, id, and date
# also remove artist, album, region, and name because they are strings not worth embedding for now
# also remove chart because I don't think there are enough charts for this to be relevant
# finally, remove index because dataframes already have an index
columns_to_remove = [
    "urls", "track_id", "data", "available_markets", "id", "url", "date",
    "region", "name", "chart",
    "rank", "streams", "trend", "popularity", "duration_ms", "release_date", "af_time_signature", "af_key", "af_mode", "af_liveness"
]
df = df.drop(*columns_to_remove)

# remove duplicates by checking to see if any titles match
df = df.dropDuplicates(["title"])

# remove explicit content, and then remove the explicit column
df = df.filter(F.col("explicit") == False)
df = df.drop("explicit")

df.coalesce(5).write.csv(output_path, header=True, mode="overwrite")

"""
Next steps:
1. ensure we don't have duplicates, maybe we can should save like the song title or something to do that
2. save the preprocessed dataframe to a parquet or csv - make sure we remove the index column for that as well
3. save just the index column, title, artist to a separate csv for reference and creating the language detection
4. test this on the small data before we do it on the big data
"""