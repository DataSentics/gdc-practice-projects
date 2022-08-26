# Databricks notebook source
# MAGIC %run ../Includes/databases

# COMMAND ----------

from Includes.const import meta_dir_path
from Includes import schemas as sc

# COMMAND ----------

meta_files = dbutils.fs.ls(meta_dir_path)
i = 0
for file in meta_files:
    if i == len(sc.meta_schemas_list):
        break
    elif file.path.endswith(".json"):
        df_meta = spark.read.json(file.path, multiLine = True)
        if(len(df_meta.columns) > 0 and "_corrupt_record" not in df_meta.columns):
            df_meta = spark.read.schema(sc.meta_schemas_list[i]).json(file.path, multiLine = True)
            file_name = file.name.replace("-","_")[:-5]
            spark.sql(f"DROP TABLE IF EXISTS odap_bronze.meta_{file_name}")
            df_meta.write.mode("append").saveAsTable(f"odap_bronze.meta_{file_name}")
            i = i + 1
