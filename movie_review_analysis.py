"""
Homework 1.2 - IMDb Movie Review Analysis with PySpark

Usage:
    python movie_review_analysis.py [path/to/IMDB Dataset.csv]
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, trim

INPUT_PATH = sys.argv[1] if len(sys.argv) > 1 else "IMDB Dataset.csv"
OUTPUT_PATH = "output/cleaned_reviews"

spark = (SparkSession.builder.appName("IMDbMovieReviews")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# 1. Load the Dataset
# Reviews contain commas, quotes and line breaks, so multiLine + escape are needed.
df = spark.read.csv(INPUT_PATH, header=True, inferSchema=True,
                    multiLine=True, escape='"')

# 2. Explore the Data
print("\n=== Schema ===")
df.printSchema()

print("=== First 5 rows ===")
df.show(5)

# 3. Clean the Data: remove rows with null or empty reviews
df_clean = df.filter(col("review").isNotNull() & (trim(col("review")) != ""))

# 4. Analyze the Data
print("=== Total number of reviews ===")
print(df_clean.count())

print("\n=== Reviews per sentiment ===")
df_clean.groupBy("sentiment").count().show()

# 5. Transform the Data
df_transformed = df_clean.withColumn("review_length", length(col("review")))

print("=== Reviews longer than 500 characters ===")
long_reviews = df_transformed.filter(col("review_length") > 500)
print("Count:", long_reviews.count())
long_reviews.show(5)

# 6. Save Your Results
df_transformed.coalesce(1).write.csv(OUTPUT_PATH, header=True, mode="overwrite",
                                     escape='"')
print(f"Saved cleaned & transformed data to: {OUTPUT_PATH}/")

spark.stop()
