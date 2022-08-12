# Databricks notebook source
# MAGIC %run ../Schemas

# COMMAND ----------

from pyspark.sql.functions import regexp_replace,col
from pyspark.sql.functions import *

# COMMAND ----------

df_impression = spark.read.format("csv").options(header="true", inferSchema="true").option("delimiter", "\t").load('abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/Impression')
df_impression = df_impression.withColumnRenamed("BannerId-AdGroupId","BannerId").withColumnRenamed("PlacementId-ActivityId","PlacementId")

# COMMAND ----------

df_impression.write.saveAsTable('iz_gdc_bronze.impression')
