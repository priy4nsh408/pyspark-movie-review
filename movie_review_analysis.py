from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length

spark = SparkSession.builder.appName("IMDb Reviews").getOrCreate()

df = spark.read.csv("IMDB Dataset.csv", header=True, multiLine=True, escape='"')

df.printSchema()
df.show(5)

df = df.filter(col("review").isNotNull() & (col("review") != ""))

total = df.count()
print("Total reviews:", total)
df.groupBy("sentiment").count().show()

df = df.withColumn("review_length", length(col("review")))
df.filter(col("review_length") > 500).show()

df.toPandas().to_csv("cleaned_reviews.csv", index=False)

spark.stop()
