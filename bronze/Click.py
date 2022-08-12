# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS odap_bronze;
# MAGIC USE odap_bronze;

# COMMAND ----------

click_path = 'abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/Click/click_210463.csv'
df_bronze_click = spark.read.csv(click_path, sep="\t", header=True, inferSchema=True)
df_bronze_click.write.format("delta").mode("overwrite").saveAsTable("odap_bronze.click")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

schema = StructType([
        StructField('id', LongType(), True),
        StructField('visibility', StructType([
            StructField('percentage', IntegerType(), True),
            StructField('time', IntegerType(), True),
            StructField('visible1', BooleanType(), True),
            StructField('visible2', BooleanType(), True),
            StructField('visible3', BooleanType(), True),
            StructField('activetab', BooleanType(), True),
        ]), True),
        StructField('interaction', StructType([
            StructField('mouseovercount', IntegerType(), True),
            StructField('mouseovertime', IntegerType(), True),
            StructField('videoplaytime', IntegerType(), True),
            StructField('engagementtime', IntegerType(), True),
            StructField('expandtime', IntegerType(), True),
            StructField('exposuretime', IntegerType(), True),
        ]), True),
        StructField('clicks', ArrayType(
        StructType([
            StructField('x', IntegerType(), True),
            StructField('y', IntegerType(), True),
        ])
    ), True),
])

df_UnloadVars = (df_bronze_click
                   .withColumn("UnloadVars", regexp_replace("UnloadVars", '""', '"'))
                   .withColumn("UnloadVars", expr("substring(UnloadVars, 2, length(UnloadVars)-2)"))
                   .select("UnloadVars")
                   .withColumn("UnloadVars", from_json("UnloadVars", schema))
                   .select("UnloadVars.*").select("id", "visibility.*", "interaction.*", "clicks")
                )
df_silver_click = (df_bronze_click
                   .join(df_UnloadVars, on="MouseOverTime", how="inner")
                   .withColumn("yyyymmdd", col("yyyymmdd").cast(StringType()))
                   .withColumn("yyyymmdd", to_date(col("yyyymmdd"),"yyyyMMdd").alias("Date"))
                   .drop("UnloadVars")
                  )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS silver;
# MAGIC USE silver;

# COMMAND ----------

df_silver_click.write.format("delta").mode("overwrite").saveAsTable("silver.click")
dir_path = "abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/"
files = dbutils.fs.ls(dir_path)
for file in files:
    if ".json" in str(file):
        df = spark.read.option("inferSchema", True).json(file.path, multiLine = True)
        if(len(df.columns) > 1):
            file_name = file.name[:-5].replace("-","_")
            df_silver_click.write.format("delta").mode("overwrite").saveAsTable(f"silver.{file_name}")
