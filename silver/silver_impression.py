# Databricks notebook source
# MAGIC %run ../bronze/bronze_impression

# COMMAND ----------

from pyspark.sql.functions import *
df1 = df_impression.withColumn("UnloadVars", regexp_replace("UnloadVars", '""', '"'))
df1 = df1.withColumn("UnloadVars", expr("substring(UnloadVars, 2, length(UnloadVars)-2)"))
df1 = df1.withColumn("UnloadVars", from_json("UnloadVars", schema)).select(col("yyyymmdd"), col("BatchId"), col("TransactionId"), col("Timestamp"), col("CookieID"), col("TagId"), col("RotatorId"), col("BannerId"), col("InnerBannerId"), col("BannerElementGroupId"), col("PublisherDomain"), col("CampaignId"), col("PlacementId"), col("CookiesEnabled"), col("IsRobot"), col("UniqueBannerFlag"), col("UniquePlacementFlag"), col("UniqueMediaFlag"), col("UniqueCampaignFlag"), col("UniquePlacementDayFlag"), col("UniqueCampaignDayFlag"), col("FrequencyCampaignViewInterval"), col("FrequencyPlacementViewInterval"), col("BackupBanner"), col("IP"), col("DeviceTypeId"), col("Engagement"), col("ClientId"), col("CityId"), col("BrowserId"), col("RtbCost"), col("InventorySourceId"), col("RtbDomain"), col("Visibility1Flag"), col("VisibilityTime"), col("VisibilityPercentage"), col("MouseOvers"), col("CrossDeviceData"), col("RtbVars"), col("Timestamp-Server"), col("RtbAdformIncludedFee"), col("RtbBrandSafetyCost"), col("RtbContextualTargetingCost"), col("RTBCrossDeviceCost"), col("RtbMediaCost"), col("RtbRichMediaFee"), col('UnloadVars.*')).select("*", "visibility.*", "interaction.*").drop("visibility", "interaction")

# COMMAND ----------

df2 = df1.withColumn("yyyymmdd", col("yyyymmdd").cast(StringType()))
df2 = df2.withColumn('yyyymmdd',to_date(df2.yyyymmdd, 'yyyyMMdd'))

# COMMAND ----------

df2.write.saveAsTable('iz_gdc_silver.impression')

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
