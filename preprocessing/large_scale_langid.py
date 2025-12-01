from lingua import Language, LanguageDetectorBuilder
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import FloatType
import re
import os

spark = SparkSession.builder.appName("LID").getOrCreate()
sc = spark.sparkContext

languages = [
    Language.ENGLISH,
    Language.SPANISH,
    Language.ARABIC,
    Language.RUSSIAN,
    Language.GERMAN,
    Language.FRENCH,
    Language.ITALIAN,
    Language.SWEDISH,
    Language.FINNISH,
    Language.POLISH,
    Language.BULGARIAN,
    Language.ROMANIAN,
    Language.HUNGARIAN,
    Language.GREEK,
    Language.TURKISH,
    Language.HINDI,
    Language.JAPANESE,
    Language.KOREAN,
    Language.VIETNAMESE,
    Language.THAI,
    Language.INDONESIAN,
    Language.PORTUGUESE,
    Language.PUNJABI,
    Language.TAMIL,
    Language.TELUGU,
    Language.TAGALOG,
]
detector = LanguageDetectorBuilder.from_languages(*languages).build()
pattern = r"\s*[\(\[].*?[\)\]]"

def detect_language_udf(title, album):
    cleaned_title = re.sub(pattern, "", title, flags=re.IGNORECASE).strip()
    cleaned_album = re.sub(pattern, "", album, flags=re.IGNORECASE).strip()
    if cleaned_title == cleaned_album:
        combined_text = cleaned_title
    else:
        combined_text = f"{cleaned_title} {cleaned_album}"
    if not combined_text.strip():
        return -1.0  # return -1.0 for empty strings, but we'll remove these afterwards
    
    detector = LanguageDetectorBuilder.from_languages(*languages).build()
    language = detector.detect_language_of(combined_text)

    confidence_values = detector.compute_language_confidence_values(combined_text)
    if confidence_values[0].value < 0.5:
        return -2.0  # return -2.0 if confidence is below 50%

    return float(languages.index(language))

def langid(csv_path):
    df = spark.read.csv(csv_path, header=True, inferSchema=True)

    # drop rows where the language id is 0, or the title or album is empty
    df = df.filter(F.col("title").isNotNull())
    df = df.filter(F.col("album").isNotNull())

    detect_language = F.udf(detect_language_udf, FloatType())

    df = df.withColumn("language_id", detect_language(F.col("title"), F.col("album")))

    # filter out language id's where we couldn't detect a language
    df = df.filter(F.col("language_id") != -1.0)

    return df

if __name__ == "__main__":
    partition_dir = "partitions"
    input_paths = [f"{partition_dir}/{i}" for i in os.listdir(partition_dir)]
    output_path = "language_id_output.csv"
    print(input_paths)

    stacked_df = None
    for i in range(len(input_paths)):
        input_path = input_paths[i]
        print(f"Processing partition {i+1}/{len(input_paths)}: {input_path}")
        if stacked_df is None:
            stacked_df = langid(input_path)
        else:
            stacked_df = stacked_df.union(langid(input_path))

    stacked_df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")