# Databricks notebook source
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, BooleanType, TimestampType

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
        ]), True)
])
