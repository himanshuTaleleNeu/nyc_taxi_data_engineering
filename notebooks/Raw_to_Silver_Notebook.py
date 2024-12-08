# Databricks notebook source
spark.catalog.clearCache()

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.<storage-account>.dfs.core.windows.net", "<application-id>")
spark.conf.set("fs.azure.account.oauth2.client.secret.<storage-account>.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage-account>.dfs.core.windows.net", "https://login.microsoftonline.com/<directory-id>/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC To view the content available in the particular storage location

# COMMAND ----------

dbutils.fs.ls('abfss://bronze@aznyctaxidatalake.dfs.core.windows.net')

# COMMAND ----------

# MAGIC %md
# MAGIC # DATA READING

# COMMAND ----------

# MAGIC %md
# MAGIC **Importing Libraries**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading CVS DATA**

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Type Data**

# COMMAND ----------

df_trip_type = spark.read.format('csv')\
                        .option('inferSchema', 'true')\
                        .option('header', 'true')\
                        .load('abfss://bronze@aznyctaxidatalake.dfs.core.windows.net/trip_type')

# COMMAND ----------

df_trip_type.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Zone Data**

# COMMAND ----------

df_trip_zone = spark.read.format('csv')\
                        .option('inferSchema', 'true')\
                        .option('header', 'true')\
                        .load('abfss://bronze@aznyctaxidatalake.dfs.core.windows.net/trip_zone')

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Data**

# COMMAND ----------

df_trip = spark.read.format('parquet')\
               .option('inferSchema',True) \
              .option('recursiveFileLookup',True)\
              .load('abfss://bronze@aznyctaxidatalake.dfs.core.windows.net/trips2023data/')

# COMMAND ----------

df_trip.count()

# COMMAND ----------

df_trip.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # DATA TRANSFORMATION

# COMMAND ----------

# MAGIC %md
# MAGIC **Taxi_Trip_Type**

# COMMAND ----------

df_trip_type.display()

# COMMAND ----------

df_trip_type = df_trip_type.withColumnRenamed("description", "trip_description")
df_trip_type.display()

# COMMAND ----------

df_trip_type.write.format('parquet')\
                .mode('append')\
                .option("path", "abfss://silver@aznyctaxidatalake.dfs.core.windows.net/trip_type")\
                .save()

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip_Zone**

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

df_trip_zone = df_trip_zone.withColumn('zone_1', split(col('Zone'), '/')[0])\
                            .withColumn('zone_2', split(col('Zone'), '/')[1])

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

df_trip_zone.write.format('parquet')\
                .mode('append')\
                .option("path", "abfss://silver@aznyctaxidatalake.dfs.core.windows.net/trip_zone")\
                .save()

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Data**

# COMMAND ----------

df_trip.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Column Renanimg of lpep_pickup_datetime
# MAGIC , lpep_dropoff_datetime
# MAGIC And Calculating Total trip duation in minutes**

# COMMAND ----------

df_trip = df_trip.withColumn('pickup_trip_date', to_date('lpep_pickup_datetime'))\
                 .withColumn('dropoff_trip_date', to_date('lpep_dropoff_datetime'))

# COMMAND ----------

df_trip = df_trip.withColumn("pickup_time", date_format("lpep_pickup_datetime", "HH:mm:ss"))\
                 .withColumn("dropoff_time", date_format("lpep_dropoff_datetime", "HH:mm:ss"))


# COMMAND ----------

df_trip = df_trip.withColumn("trip_duration_minutes", 
                   (unix_timestamp("lpep_dropoff_datetime") - unix_timestamp("lpep_pickup_datetime")) / 60)

# COMMAND ----------

# MAGIC %md
# MAGIC **fare_amount and congestion_surcharge contains negative values. So will convert the values to positive i.e. 0**

# COMMAND ----------

negative_fare_count = df_trip.filter(col("fare_amount") < 0).count()
print(f"Number of negative values in fare_amount: {negative_fare_count}")

negative_congestion_count = df_trip.filter(col("congestion_surcharge") < 0).count()
print(f"Number of negative values in congestion_surcharge: {negative_congestion_count}")


# COMMAND ----------

df_trip = df_trip.withColumn("fare_amount", when(col("fare_amount") < 0, 0).otherwise(col("fare_amount")))
df_trip = df_trip.withColumn("congestion_surcharge", when(col("congestion_surcharge") < 0, 0).otherwise(col("congestion_surcharge")))

# COMMAND ----------

negative_fare_count = df_trip.filter(col("fare_amount") < 0).count()
print(f"Number of negative values in fare_amount: {negative_fare_count}")

negative_congestion_count = df_trip.filter(col("congestion_surcharge") < 0).count()
print(f"Number of negative values in congestion_surcharge: {negative_congestion_count}")


# COMMAND ----------

columns_to_drop = ["lpep_pickup_datetime", "lpep_dropoff_datetime", "store_and_fwd_flag", "improvement_surcharge", "tolls_amount", "extra", "mta_tax","passenger_count","ehail_fee"]

df_trip = df_trip.drop(*columns_to_drop)

# COMMAND ----------

df_trip = df_trip.select('VendorID','RatecodeID','trip_distance','fare_amount','tip_amount','total_amount','payment_type','trip_type','PULocationID','DOLocationID','congestion_surcharge','pickup_trip_date','dropoff_trip_date','pickup_time','dropoff_time','trip_duration_minutes')

df_trip.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Handle Inconsistent Data**
# MAGIC
# MAGIC **1. payment_type**
# MAGIC
# MAGIC **2. RatecodeID**
# MAGIC
# MAGIC **3. trip_type**

# COMMAND ----------

null_count = df_trip.filter(col("payment_type").isNull()).count()
print(f"Number of nulls in payment_type column: {null_count}")

payment_type_counts = (
    df_trip.groupBy("payment_type")
    .count()
    .orderBy(col("count").desc())
)

payment_type_counts.show()

# COMMAND ----------

df_trip = df_trip.withColumn(
    "payment_type",
    when(col("payment_type").isNotNull(), col("payment_type").cast("integer"))
)

# COMMAND ----------

null_count = df_trip.filter(col("RatecodeID").isNull()).count()
print(f"Number of nulls in RatecodeID column: {null_count}")

payment_type_counts = (
    df_trip.groupBy("RatecodeID")
    .count()
    .orderBy(col("count").desc())
)

payment_type_counts.display()

# COMMAND ----------

df_trip = df_trip.withColumn(
    "RatecodeID",
    when(col("RatecodeID").isNotNull(), col("RatecodeID").cast("integer"))
)

# COMMAND ----------

null_count = df_trip.filter(col("trip_type").isNull()).count()
print(f"Number of nulls in trip_type column: {null_count}")

payment_type_counts = (
    df_trip.groupBy("trip_type")
    .count()
    .orderBy(col("count").desc())
)

payment_type_counts.display()

# COMMAND ----------

df_trip = df_trip.withColumn(
    "trip_type",
    when(col("trip_type").isNotNull(), col("trip_type").cast("integer"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Mapping  the numeric values with it descriptions**
# MAGIC
# MAGIC **Replace VendorID, RatecodeID, payment_type,trip_type with thier respective look up values**

# COMMAND ----------

df_trip_type_lookup = df_trip.withColumn(
    "VendorID",
    when(col("VendorID") == 1, "Creative Mobile Technologies, LLC")
    .when(col("VendorID") == 2, "VeriFone Inc.")
)

df_trip_type_lookup = df_trip_type_lookup.withColumn(
    "RatecodeID",
    when(col("RatecodeID") == 1, "Standard rate")
    .when(col("RatecodeID") == 2, "JFK")
    .when(col("RatecodeID") == 3, "Newark")
    .when(col("RatecodeID") == 4, "Nassau or Westchester")
    .when(col("RatecodeID") == 5, "Negotiated fare")
    .when(col("RatecodeID") == 6, "Group ride")
    .when(col("RatecodeID") == 99, "Unmapped")  
    .when(col("RatecodeID").isNull(), "Unknown") 
    .otherwise("Invalid") 
)

# Replace payment_type
df_trip_type_lookup = df_trip_type_lookup.withColumn(
    "payment_type",
    when(col("payment_type") == 1, "Credit card")
    .when(col("payment_type") == 2, "Cash")
    .when(col("payment_type") == 3, "No charge")
    .when(col("payment_type") == 4, "Dispute")
    .when(col("payment_type") == 5, "Unknown")
    .when(col("payment_type") == 6, "Voided trip")
    .when(col("payment_type").isNull(), "Unknown")
    .otherwise("Invalid")
)

# # Replace trip_type
# df_trip_type_lookup = df_trip_type_lookup.withColumn(
#     "trip_type",
#     when(col("trip_type") == 1, "Street-hail")
#     .when(col("trip_type") == 2, "Dispatch")
#     .when(col("payment_type").isNull(), "Unknown")
#     .otherwise("Invalid")
# )

# COMMAND ----------

null_count = df_trip_type_lookup.filter(col("RatecodeID").isNull()).count()
print(f"Number of nulls in payment_type column: {null_count}")

# Count of each payment_type value, including nulls
payment_type_counts = (
    df_trip_type_lookup.groupBy("RatecodeID")
    .count()
    .orderBy(col("count").desc())
)

# Display the result
payment_type_counts.show()

# COMMAND ----------

# df_trip_type_lookup.display()

df_trip_type_lookup.limit(5).display()


# COMMAND ----------

# MAGIC %md
# MAGIC  **Now Perform data type transformations for necessary columns**

# COMMAND ----------

df_transformed = df_trip_type_lookup \
    .withColumn("VendorID", col("VendorID").cast("string")) \
    .withColumn("RatecodeID", col("RatecodeID").cast("string")) \
    .withColumn("trip_distance", col("trip_distance").cast("double")) \
    .withColumn("fare_amount", col("fare_amount").cast("double")) \
    .withColumn("tip_amount", col("tip_amount").cast("double")) \
    .withColumn("total_amount", col("total_amount").cast("double")) \
    .withColumn("payment_type", col("payment_type").cast("string")) \
    .withColumn("trip_type", col("trip_type").cast("int")) \
    .withColumn("PULocationID", col("PULocationID").cast("int")) \
    .withColumn("DOLocationID", col("DOLocationID").cast("int")) \
    .withColumn("congestion_surcharge", col("congestion_surcharge").cast("double")) \
    .withColumn("pickup_trip_date", col("pickup_trip_date").cast("date")) \
    .withColumn("dropoff_trip_date", col("dropoff_trip_date").cast("date")) \
    .withColumn("pickup_time", col("pickup_time").cast("string")) \
    .withColumn("dropoff_time", col("dropoff_time").cast("string")) \
    .withColumn("trip_duration_minutes", col("trip_duration_minutes").cast("double"))

# COMMAND ----------

df_transformed.printSchema()

# COMMAND ----------

df_transformed.write.format('parquet')\
                .mode('append')\
                .option("path", "abfss://silver@aznyctaxidatalake.dfs.core.windows.net/trips2023data")\
                .save()
