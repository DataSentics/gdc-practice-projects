# Databricks notebook source
dir_path = "abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/"
files = dbutils.fs.ls(dir_path)
df_list = []
for file in files:
    if (".json" in str(file)):
        df = spark.read.option("inferSchema", True).json(file.path, multiLine = True)
        if (len(df.columns) > 1):
            file = file.name[:-5].replace('-', '_')
            df.write.format("delta").mode("overwrite").saveAsTable(f'iz_gdc_bronze.{file}')

