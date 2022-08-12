# Databricks notebook source
df_click = spark.read.format("csv").options(header="true", inferSchema="true").option("delimiter", "\t").load('abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/Click')

# COMMAND ----------

df_click.write.saveAsTable('iz_gdc_bronze.click')
