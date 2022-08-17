# Databricks notebook source
# MAGIC %run ../Bronze/Click

# COMMAND ----------

# MAGIC %run ../Bronze/Meta

# COMMAND ----------

from pyspark.sql import functions as f
from Includes.schemas import schema_silver_click

df_silver_click = (df_bronze_click
                   .withColumn("yyyymmdd", f.to_date(f.col("yyyymmdd"), "yyyyMMdd"))
                   .withColumn("UnloadVars", f.regexp_replace("UnloadVars", '""', '"'))
                   .withColumn("UnloadVars", f.expr("substring(UnloadVars, 2, length(UnloadVars)-2)"))
                   .withColumn("UnloadVars", f.from_json("UnloadVars", schema_silver_click))
                   .drop("MouseOverTime")
                   .select("*", "UnloadVars.id", "UnloadVars.visibility.*", "UnloadVars.interaction.*", "UnloadVars.clicks")
                   .drop("UnloadVars")
                  )

df_silver_click.write.format("delta").mode("overwrite").saveAsTable("silver.click")
