# Databricks notebook source
from pyspark.sql import functions as f
from Includes.schemas import schema_silver_click

df_bronze_click = spark.table("odap_bronze.click")
df_silver_click = (df_bronze_click
                   .withColumn("yyyymmdd", f.to_date(f.col("yyyymmdd"), "yyyyMMdd"))
                   .withColumn("UnloadVars", f.regexp_replace("UnloadVars", '""', '"'))
                   .withColumn("UnloadVars", f.expr("substring(UnloadVars, 2, length(UnloadVars)-2)"))
                   .withColumn("UnloadVars", f.from_json("UnloadVars", schema_silver_click))
                   .drop("MouseOverTime")
                   .select("*", "UnloadVars.id", "UnloadVars.visibility.*", "UnloadVars.interaction.*", "UnloadVars.clicks")
                   .drop("UnloadVars")
                  )

spark.sql("DROP TABLE IF EXISTS silver.click")
df_silver_click.write.mode("append").saveAsTable("silver.click")

# COMMAND ----------

batch_id = spark.sql("select distinct(BatchId) from silver.click").rdd.map(lambda row : row[0]).collect()
batch_id.sort()
dbutils.widgets.dropdown("batch_id", "210463", [str(x) for x in batch_id])
display(df_silver_click.filter(df_silver_click.BatchId == dbutils.widgets.get("batch_id")))
