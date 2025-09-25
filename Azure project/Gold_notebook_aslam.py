# Databricks notebook source
# MAGIC %md
# MAGIC # Data Access

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.nyctaxistorageacc01.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.nyctaxistorageacc01.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.nyctaxistorageacc01.dfs.core.windows.net", "165c32f6-cff4-47c7-99e7-c987ea432968")
spark.conf.set("fs.azure.account.oauth2.client.secret.nyctaxistorageacc01.dfs.core.windows.net","kLa8Q~Zprk.DXtlL-7-195_wj_JIb6TiXlLERczu")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.nyctaxistorageacc01.dfs.core.windows.net", "https://login.microsoftonline.com/52301e1e-46c0-4824-b769-89cc6c4c3dc4/oauth2/token")

# COMMAND ----------

# MAGIC %sql
# MAGIC create database gold2

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Reading and Writing and Creating Delta

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_trip_zone = spark.read.format('parquet')\
                .option("header", "true")\
                    .option("inferSchema", "true")\
                        .load("abfss://silver@nyctaxistorageacc01.dfs.core.windows.net/trip_zone")

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Database**

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW STORAGE CREDENTIALS;

# COMMAND ----------

# --- EE DETAILS NIRBANDAMAYUM ADD CHEYYUKA ---
# # This code ensures Spark knows how to securely connect to your storage in this session
storage_account_name = "nyctaxistorageacc01"
# container_name = "gold"
# client_id = "e9629a13-e45d-44fe-b2b5-eec8cfb46a78"  # Ningalude Service Principal-inte Client ID
# tenant_id = "c457d3f6-a8d8-4cc0-abe2-52ca3ae89e87"    # Ningalude Tenant ID
# client_secret = "2Gi8Q~F5LlOMvD1oM4Db7Hm5lrq0AgzCcZGLocQ6"  # Ningalude Client Secret

# spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
# spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", client_id)
# spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", client_secret)
# spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# --- STEP 1: SAVE THE DATA ---
# This saves the data to the 'trip_zone' folder in your 'gold' container
df_trip_zone.write.format("delta") \
    .mode("append") \
    .save(f"abfss://gold@{storage_account_name}.dfs.core.windows.net/trip_zone")

print("Data saved successfully to the gold layer.")

# COMMAND ----------

# --- Secure Connection Configuration ---
# Session thudangumbol ee configuration code run cheyyunnathu nallathanu
# storage_account_name = "nycprojectstorageacc"
# container_name = "gold"
# client_id = "e9629a13-e45d-44fe-b2b5-eec8cfb46a78"  # Ningalude Service Principal-inte Client ID
# tenant_id = "c457d3f6-a8d8-4cc0-abe2-52ca3ae89e87"    # Ningalude Tenant ID
# client_secret = "2Gi8Q~F5LlOMvD1oM4Db7Hm5lrq0AgzCcZGLocQ6"  # Ningalude Client Secret

# spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
# spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", client_id)
# spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", client_secret)
# spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")


# # --- STEP 1: READ CLEAN DATA FROM SILVER LAYER ---
# # Ningalude clean cheytha data silver layer-il evidano, aa correct path ivide kodukkuka
# # Udaharanathinu: "abfss://silver@.../trip_zone_cleaned"
silver_df = spark.read.format("parquet").load("abfss://silver@nyctaxistorageacc01.dfs.core.windows.net/trip_zone")


# --- STEP 2: SAVE AS A MANAGED TABLE (THE FINAL FIX) ---
# Evide path onnum kodukkunnilla ennu sradhikkuka.
silver_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold2.trip_zone")

print("Table 'gold2.trip_zone' created successfully as a MANAGED TABLE!")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold2.trip_zone LIMIT 10;

# COMMAND ----------

silver_df_type= spark.read.format("parquet").load("abfss://silver@nyctaxistorageacc01.dfs.core.windows.net/trip_type")


# --- STEP 2: SAVE AS A MANAGED TABLE (THE FINAL FIX) ---
# Evide path onnum kodukkunnilla ennu sradhikkuka.
silver_df_type.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold2.trip_type")

print("Table 'gold2.trip_type' created successfully as a MANAGED TABLE!")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold2.trip_type LIMIT 10;

# COMMAND ----------

silver_df_trip= spark.read.format("parquet").load("abfss://silver@nyctaxistorageacc01.dfs.core.windows.net/trip")


# --- STEP 2: SAVE AS A MANAGED TABLE (THE FINAL FIX) ---
# Evide path onnum kodukkunnilla ennu sradhikkuka.
silver_df_trip.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold2.trip_main_table")

print("Table 'gold2.trip_main_table' created successfully as a MANAGED TABLE!")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold2.trip_main_table LIMIT 10;

# COMMAND ----------

