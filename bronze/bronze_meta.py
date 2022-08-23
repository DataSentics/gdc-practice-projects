# Databricks notebook source
from includes.const import dir_path
from includes.schema import schema_from_meta

# COMMAND ----------

files = dbutils.fs.ls(dir_path)
i = 0
for file in files:
    if i == 20:
        break
    if (str(file.path).endswith(".json")):
        df = spark.read.option("inferSchema", True).json(file.path, multiLine = True)
        if (len(df.columns) > 1):
            df = spark.read.schema(schema_from_meta[i]).json(file.path, multiLine = True)
            file = file.name[:-5].replace('-', '_')
            df.write.format("delta").mode("overwrite").saveAsTable(f'iz_gdc_bronze.{file}')
            i = i + 1
