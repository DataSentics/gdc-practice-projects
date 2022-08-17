# Databricks notebook source
# MAGIC %run ../bronze/bronze_impression

# COMMAND ----------

from pyspark.sql import functions as F
from includes.schema import schema_unload_vars

# COMMAND ----------

df_impression = df_impression.withColumnRenamed("BannerId-AdGroupId","BannerId").withColumnRenamed("PlacementId-ActivityId","PlacementId")

# COMMAND ----------

df_unload_vars = df_impression.withColumn("UnloadVars", F.regexp_replace("UnloadVars", '""', '"'))
df_unload_vars = df_unload_vars.withColumn("UnloadVars", F.expr("substring(UnloadVars, 2, length(UnloadVars)-2)"))
df_unload_vars = df_unload_vars.withColumn("UnloadVars", F.from_json("UnloadVars", schema_unload_vars)).select(F.col("yyyymmdd"), F.col("BatchId"), F.col("TransactionId"), F.col("Timestamp"), F.col("CookieID"), F.col("TagId"), F.col("RotatorId"), F.col("BannerId"), F.col("InnerBannerId"), F.col("BannerElementGroupId"), F.col("PublisherDomain"), F.col("CampaignId"), F.col("PlacementId"), F.col("CookiesEnabled"), F.col("IsRobot"), F.col("UniqueBannerFlag"), F.col("UniquePlacementFlag"), F.col("UniqueMediaFlag"), F.col("UniqueCampaignFlag"), F.col("UniquePlacementDayFlag"), F.col("UniqueCampaignDayFlag"), F.col("FrequencyCampaignViewInterval"), F.col("FrequencyPlacementViewInterval"), F.col("BackupBanner"), F.col("IP"), F.col("DeviceTypeId"), F.col("Engagement"), F.col("ClientId"), F.col("CityId"), F.col("BrowserId"), F.col("RtbCost"), F.col("InventorySourceId"), F.col("RtbDomain"), F.col("Visibility1Flag"), F.col("VisibilityTime"), F.col("VisibilityPercentage"), F.col("MouseOvers"), F.col("CrossDeviceData"), F.col("RtbVars"), F.col("Timestamp-Server"), F.col("RtbAdformIncludedFee"), F.col("RtbBrandSafetyCost"), F.col("RtbContextualTargetingCost"), F.col("RTBCrossDeviceCost"), F.col("RtbMediaCost"), F.col("RtbRichMediaFee"), F.col('UnloadVars.*')).select("*", "visibility.*", "interaction.*").drop("visibility", "interaction")

# COMMAND ----------

df_date = df_unload_vars.withColumn('yyyymmdd', F.to_date(df_unload_vars.yyyymmdd, 'yyyyMMdd'))

# COMMAND ----------

df_date.write.saveAsTable('iz_gdc_silver.impression')

# COMMAND ----------

df_impression = spark.read.table('iz_gdc_silver.impression')
df_bannersadgroups = spark.read.table('iz_gdc_bronze.banners_adgroups')
df_browsers = spark.read.table('iz_gdc_bronze.browsers')
df_campaigns = spark.read.table('iz_gdc_bronze.campaigns')
df_clients = spark.read.table('iz_gdc_bronze.clients')
df_devices = spark.read.table('iz_gdc_bronze.devices')
df_geolocations = spark.read.table('iz_gdc_bronze.geolocations')
df_placementsactivities = spark.read.table('iz_gdc_bronze.placements_activities')
df_tags = spark.read.table('iz_gdc_bronze.tags')
df_zipcodes = spark.read.table('iz_gdc_bronze.zip_codes')

# COMMAND ----------

df_joined=df_impression.join(df_bannersadgroups, df_impression.BannerId == df_bannersadgroups.id, how = "left")\
                       .join(df_browsers, df_impression.BrowserId == df_browsers.id, how = "left")\
                       .join(df_campaigns, df_impression.ClientId == df_campaigns.clientId, how = "left")\
                       .join(df_clients, df_impression.ClientId == df_clients.id, how = "left")\
                       .join(df_devices, df_impression.DeviceTypeId == df_devices.id, how = "left")\
                       .join(df_geolocations, df_impression.CityId == df_geolocations.cityId, how = "left")\
                       .join(df_placementsactivities, df_impression.PlacementId == df_placementsactivities.id, how = "left")\
                       .join(df_tags, df_impression.TagId == df_tags.id, how = "left")\
                       .join(df_zipcodes, df_impression.CityId == df_zipcodes.cityId, how = "left")
