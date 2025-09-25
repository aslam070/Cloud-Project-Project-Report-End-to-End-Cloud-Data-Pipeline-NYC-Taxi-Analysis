# Databricks notebook source
# MAGIC %md
# MAGIC # Data Access

# COMMAND ----------

# Application (client) ID = e9629a13-e45d-44fe-b2b5-eec8cfb46a78
# Directory (tenant) ID = c457d3f6-a8d8-4cc0-abe2-52ca3ae89e87
# secret id = 2Gi8Q~F5LlOMvD1oM4Db7Hm5lrq0AgzCcZGLocQ6

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.nyctaxistorageacc01.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.nyctaxistorageacc01.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.nyctaxistorageacc01.dfs.core.windows.net", "165c32f6-cff4-47c7-99e7-c987ea432968")
spark.conf.set("fs.azure.account.oauth2.client.secret.nyctaxistorageacc01.dfs.core.windows.net","kLa8Q~Zprk.DXtlL-7-195_wj_JIb6TiXlLERczu")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.nyctaxistorageacc01.dfs.core.windows.net", "https://login.microsoftonline.com/52301e1e-46c0-4824-b769-89cc6c4c3dc4/oauth2/token")

# COMMAND ----------

dbutils.fs.ls('abfss://bronze@nyctaxistorageacc01.dfs.core.windows.net/')

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Reading

# COMMAND ----------

# MAGIC %md
# MAGIC ##### importing libraries

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading CSV Data**

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Type Data**

# COMMAND ----------

df_trip_type = spark.read.format("csv")\
                    .option('inferSchema',True)\
                    .option('header',True)\
                    .load('abfss://bronze@nyctaxistorageacc01.dfs.core.windows.net/trip_type')
                    

# COMMAND ----------

df_trip_type.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Zone**

# COMMAND ----------

df_trip_zone = spark.read.format("csv")\
                    .option('inferSchema',True)\
                    .option('header',True)\
                    .load('abfss://bronze@nyctaxistorageacc01.dfs.core.windows.net/trip_zone')
df_trip_zone.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Data**

# COMMAND ----------

myschema = '''
    VendorID BIGINT,
    lpep_pickup_datetime TIMESTAMP,
    lpep_dropoff_datetime TIMESTAMP,
    store_and_fwd_flag STRING,
    RatecodeID BIGINT,
    PULocationID BIGINT,
    DOLocationID BIGINT,
    passenger_count BIGINT,
    trip_distance DOUBLE,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    ehail_fee DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    payment_type BIGINT,
    trip_type BIGINT,
    congestion_surcharge DOUBLE
'''


# COMMAND ----------

df_trip =spark.read.format("parquet")\
                    .schema(myschema)\
                    .option('header',True)\
                    .option('recursiveFileLookup',True)\
                    .load('abfss://bronze@nyctaxistorageacc01.dfs.core.windows.net/trips2024data/')
df_trip.display()


# COMMAND ----------

# MAGIC %md
# MAGIC # Data Transformation

# COMMAND ----------

df_trip_type.display()

# COMMAND ----------

df_trip_type = df_trip_type.withColumnRenamed('description','trip_description')
df_trip_type.display()

# COMMAND ----------

df_trip_type.write.format("parquet")\
                    .mode("append")\
                    .option("path","abfss://silver@nyctaxistorageacc01.dfs.core.windows.net/trip_type")\
                    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Zone**

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

df_trip_zone =df_trip_zone.withColumn('zone1',split(col('zone'),'/')[0])\
                          .withColumn('zone2',split(col('zone'),'/')[1])
                          
df_trip_zone.display()                        

# COMMAND ----------

df_trip_zone.write.format("parquet")\
                    .mode("append")\
                    .option("path","abfss://silver@nyctaxistorageacc01.dfs.core.windows.net/trip_zone")\
                    .save()

# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip = df_trip.withColumn('trip_pickup_date',to_date(col('lpep_pickup_datetime')))\
                 .withColumn('trip_month',month(col('lpep_pickup_datetime')))\
                    .withColumn('trip_year',year(col('lpep_pickup_datetime')))

df_trip.display()
                    

# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip = df_trip.withColumn('trip_month',month(col('lpep_pickup_datetime')))\
                    .withColumn('trip_year',year(col('lpep_pickup_datetime')))
df_trip.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Dropping unwanted column**

# COMMAND ----------

columns_to_drop = ["store_and_fwd_flag", "airport_fee","ehail_fee"]

df_trip = df_trip.drop(*columns_to_drop)
df_trip.display()


# COMMAND ----------

# MAGIC %md
# MAGIC **Filtering data**

# COMMAND ----------

df_trip = df_trip.filter(

    (col("trip_distance") > 0) &

    (col("total_amount") > 0) &

    (col("passenger_count") > 0)

)

# COMMAND ----------

# MAGIC %md
# MAGIC **Creating derived column**

# COMMAND ----------


df_tripf = df_trip.withColumn(
   # Transformation 1: trip_duration_mins 
    "trip_duration_mins", 
    round(
        (unix_timestamp(col("lpep_dropoff_datetime")) - unix_timestamp(col("lpep_pickup_datetime"))) / 60, 
        2  
    )
).withColumn(
    # Transformation 2: cost_per_mile 
    "cost_per_mile", 
    round(
        when(col("trip_distance") > 0, col("total_amount") / col("trip_distance"))
        .otherwise(0),
        2 
    )
).withColumn(
    # Transformation 3: tip_percentage
    "tip_percentage", 
    round(
        when(col("total_amount") > 0, (col("tip_amount") / col("total_amount")) * 100)
        .otherwise(0),
        2 
    )
)

# COMMAND ----------

df_tripf.display()


# COMMAND ----------

df_trip=df_tripf
df_trip.display()

# COMMAND ----------

df_trip.write.format("parquet")\
                    .mode("append")\
                    .option("path","abfss://silver@nyctaxistorageacc01.dfs.core.windows.net/trip")\
                    .save()