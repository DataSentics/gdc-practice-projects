# Databricks notebook source
from includes.schema import schema_impression

# COMMAND ----------

df_impression = spark.read.format("csv").options(header="true").option("delimiter", "\t").schema(schema_impression).load('abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/Impression')

# COMMAND ----------

df_impression.write.saveAsTable('iz_gdc_bronze.impression')
