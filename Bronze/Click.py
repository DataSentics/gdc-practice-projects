# Databricks notebook source
# MAGIC %run ../Includes/databases

# COMMAND ----------

from Includes.const import click_file_path
from Includes.schemas import schema_bronze_click

# COMMAND ----------

df_bronze_click = spark.read.option("sep", "\t").option("header", True).schema(schema_bronze_click).csv(click_file_path)
spark.sql("DROP TABLE IF EXISTS odap_bronze.click")
df_bronze_click.write.mode("append").saveAsTable("odap_bronze.click")
