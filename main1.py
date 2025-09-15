from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, sqrt, pow, when, max, min


#collect data
spark = SparkSession.builder \
    .appName("NYC Yellow Taxi Analysis") \
    .getOrCreate()

df = spark.read.parquet("yellow_tripdata_2023-01.parquet")

#data exploration
print("Schema:")
df.printSchema()

#sample data
print("Sample data:")
df.show(5)

#checking for missing values
print("Missing values count:")
missing_values = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
missing_values.show()

#drop missing values
df = df.na.drop()

#filter out invalid fare amounts (negative or zero)
df = df.filter(df.fare_amount > 0)


#max fare amount
avg_fare = df.select(max("fare_amount").alias("max_fare"))
print("Max fare amount:")
avg_fare.show()

#min fare amount
avg_fare = df.select(min("fare_amount").alias("min_fare"))
print("Min fare amount:")
avg_fare.show()


#average fare amount
avg_fare = df.select(avg("fare_amount").alias("avg_fare"))
print("Average fare amount:")
avg_fare.show()

