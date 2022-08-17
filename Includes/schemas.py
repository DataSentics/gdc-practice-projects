from pyspark.sql import types as T

schema_bronze_click = T.StructType([
    T.StructField("yyyymmdd", T.StringType(),True),
    T.StructField("BatchId", T.IntegerType(),True),
    T.StructField("TransactionId", T.LongType(),True),
    T.StructField("Timestamp", T.TimestampType(),True),
    T.StructField("CookieID", T.LongType(),True),
    T.StructField("TagId", T.IntegerType(),True),
    T.StructField("RotatorId", T.IntegerType(),True),
    T.StructField("BannerId-AdGroupId", T.IntegerType(),True),
    T.StructField("ClickDetailId-PaidKeywordId", T.IntegerType(), True),
    T.StructField("InnerBannerId", T.IntegerType(),True),
    T.StructField("BannerElementGroupId", T.IntegerType(),True),
    T.StructField("PublisherDomain", T.StringType(),True),
    T.StructField("CampaignId", T.IntegerType(),True),
    T.StructField("PlacementId-ActivityId", T.IntegerType(),True),
    T.StructField("CookiesEnabled", T.IntegerType(),True),
    T.StructField("IsRobot", T.StringType(),True),
    T.StructField("UniqueBannerFlag", T.IntegerType(),True),
    T.StructField("UniquePlacementFlag", T.IntegerType(),True),
    T.StructField("UniqueMediaFlag", T.IntegerType(),True),
    T.StructField("UniqueCampaignFlag", T.IntegerType(),True),
    T.StructField("UniquePlacementDayFlag", T.IntegerType(),True),
    T.StructField("UniqueCampaignDayFlag", T.IntegerType(),True),
    T.StructField("FrequencyCampaignViewInterval", T.IntegerType(),True),
    T.StructField("FrequencyPlacementViewInterval", T.IntegerType(),True),
    T.StructField("BackupBanner", T.IntegerType(),True),
    T.StructField("PublisherURL", T.StringType(), True),
    T.StructField("DestinationURL", T.StringType(), True),
    T.StructField("IP", T.StringType(),True),
    T.StructField("DeviceTypeId", T.IntegerType(),True),
    T.StructField("ClientId", T.IntegerType(),True),
    T.StructField("CityId", T.IntegerType(),True),
    T.StructField("BrowserId", T.IntegerType(),True),
    T.StructField("RtbCost", T.StringType(),True),
    T.StructField("InventorySourceId", T.IntegerType(),True),
    T.StructField("RtbDomain", T.StringType(),True),
    T.StructField("Visibility1Flag", T.IntegerType(),True),
    T.StructField("VisibilityTime", T.IntegerType(),True),
    T.StructField("MouseOvers", T.IntegerType(),True),
    T.StructField("MouseOverTime", T.IntegerType(),True),
    T.StructField("CrossDeviceData", T.StringType(),True),
    T.StructField("UnloadVars", T.StringType(),True),
    T.StructField("RtbVars", T.StringType(),True),
    T.StructField("Timestamp-Server", T.TimestampType(),True)
])

schema_silver_click = T.StructType([
        T.StructField('id', T.LongType(), True),
        T.StructField('visibility', T.StructType([
            T.StructField('percentage', T.IntegerType(), True),
            T.StructField('time', T.IntegerType(), True),
            T.StructField('visible1', T.BooleanType(), True),
            T.StructField('visible2', T.BooleanType(), True),
            T.StructField('visible3', T.BooleanType(), True),
            T.StructField('activetab', T.BooleanType(), True),
        ]), True),
        T.StructField('interaction', T.StructType([
            T.StructField('mouseovercount', T.IntegerType(), True),
            T.StructField('mouseovertime', T.IntegerType(), True),
            T.StructField('videoplaytime', T.IntegerType(), True),
            T.StructField('engagementtime', T.IntegerType(), True),
            T.StructField('expandtime', T.IntegerType(), True),
            T.StructField('exposuretime', T.IntegerType(), True),
        ]), True),
        T.StructField('clicks', T.ArrayType(
        T.StructType([
            T.StructField('x', T.IntegerType(), True),
            T.StructField('y', T.IntegerType(), True),
        ])
    ), True),
])

meta_schemas_list = [
    
#CUSTOM SCHEMA FOR banners-adgroups
T.StructType([
    T.StructField("id",T.LongType(),True),
    T.StructField("name",T.StringType(),True),
    T.StructField("bannerAttribute1",T.StringType(),True),
    T.StructField("bannerAttribute2",T.StringType(),True),
    T.StructField("bannerAttribute3",T.StringType(),True),
    T.StructField("bannerAttribute4",T.StringType(),True),
    T.StructField("bannerAttribute5",T.StringType(),True),
    T.StructField("bannerSize",T.StringType(),True),
    T.StructField("bannerType",T.StringType(),True),
    T.StructField("videoDuration",T.LongType(),True)
]),



#CUSTOM SCHEMA FOR browsers
T.StructType([
    T.StructField("id",T.LongType(),True),
    T.StructField("name",T.StringType(),True)
]),



#CUSTOM SCHEMA FOR campaigns
T.StructType([
    T.StructField("id",T.LongType(),True),
    T.StructField("clientId",T.LongType(),True),
    T.StructField("name",T.StringType(),True),
    T.StructField("type",T.StringType(),True),
    T.StructField("visibilityTime",T.LongType(),True),
    T.StructField("visibilityArea",T.LongType(),True),
    T.StructField("visibilityTime2",T.StringType(),True),
    T.StructField("visibilityArea2",T.StringType(),True),
    T.StructField("visibilityTime3",T.StringType(),True),
    T.StructField("visibilityArea3",T.StringType(),True),
    T.StructField("Label1",T.StringType(),True),
    T.StructField("Label2",T.StringType(),True),
    T.StructField("Label3",T.StringType(),True),
    T.StructField("Label4",T.StringType(),True),
    T.StructField("Label5",T.StringType(),True),
    T.StructField("Label6",T.StringType(),True),
    T.StructField("Label7",T.StringType(),True),
    T.StructField("Label8",T.StringType(),True),
    T.StructField("Label9",T.StringType(),True),
    T.StructField("Label10",T.StringType(),True),
    T.StructField("Label11",T.StringType(),True),
    T.StructField("Label12",T.StringType(),True),
    T.StructField("Label13",T.StringType(),True),
    T.StructField("Label14",T.StringType(),True),
    T.StructField("Label15",T.StringType(),True),
    T.StructField("Label16",T.StringType(),True),
    T.StructField("Label17",T.StringType(),True),
    T.StructField("Label18",T.StringType(),True),
    T.StructField("Label19",T.StringType(),True),
    T.StructField("Label20",T.StringType(),True),
    T.StructField("timeZone",T.StringType(),True),
    T.StructField("budget",T.DoubleType(),True),
    T.StructField("budgetPeriodType",T.StringType(),True),
    T.StructField("budgetGoalType",T.StringType(),True),
    T.StructField("startDate",T.StringType(),True),
    T.StructField("endDate",T.StringType(),True),
    T.StructField("campaignStatus",T.StringType(),True),
    T.StructField("currencyName",T.StringType(),True),
    T.StructField("currencyCode",T.StringType(),True),
]),



#CUSTOM SCHEMA FOR clickdetails-paidkeywords
T.StructType([
    T.StructField("id",T.LongType(),True),
    T.StructField("name",T.StringType(),True)
]),



#CUSTOM SCHEMA FOR clients
T.StructType([
    T.StructField("id",T.LongType(),True),
    T.StructField("name",T.StringType(),True)
]),



#CUSTOM SCHEMA FOR costs
T.StructType([
      T.StructField("LineItem",T.LongType(),True),
      T.StructField("BuyType",T.StringType(),True),
      T.StructField("TransactioncostFp",T.DoubleType(),True),
      T.StructField("TransactioncostFc",T.DoubleType(),True),
      T.StructField("TransactioncostMax",T.DoubleType(),True)
]),



#CUSTOM SCHEMA FOR deals
T.StructType([
      T.StructField("dealId",T.StringType(),True),
      T.StructField("dealName",T.StringType(),True)
]),



#CUSTOM SCHEMA FOR devices
T.StructType([
    T.StructField("id",T.LongType(),True),
    T.StructField("name",T.StringType(),True)
]),



#CUSTOM SCHEMA FOR events
T.StructType([
    T.StructField("id",T.LongType(),True),
    T.StructField("name",T.StringType(),True)
]),



#CUSTOM SCHEMA FOR geolocations
T.StructType([
      T.StructField("countryId",T.LongType(),True),
      T.StructField("country",T.StringType(),True),
      T.StructField("countryCodeISO3",T.StringType(),True),
      T.StructField("regionId",T.LongType(),True),
      T.StructField("region",T.StringType(),True),
      T.StructField("regionCode",T.StringType(),True),
      T.StructField("cityId",T.LongType(),True),
      T.StructField("city",T.StringType(),True),
]),



#CUSTOM SCHEMA FOR iabcategories
T.StructType([
    T.StructField("id",T.LongType(),True),
    T.StructField("name",T.StringType(),True)
]),



#CUSTOM SCHEMA FOR inventorysources
T.StructType([
    T.StructField("id",T.LongType(),True),
    T.StructField("name",T.StringType(),True),
    T.StructField("partyid",T.LongType(),True)
]),



#CUSTOM SCHEMA FOR languages
T.StructType([
    T.StructField("id",T.LongType(),True),
    T.StructField("name",T.StringType(),True)
]),



#CUSTOM SCHEMA FOR medias
T.StructType([
    T.StructField("id",T.LongType(),True),
    T.StructField("name",T.StringType(),True)
]),



#CUSTOM SCHEMA FOR operatingsystems
T.StructType([
    T.StructField("id",T.LongType(),True),
    T.StructField("name",T.StringType(),True)
]),



#CUSTOM SCHEMA FOR parties
T.StructType([
    T.StructField("id",T.LongType(),True),
    T.StructField("name",T.StringType(),True)
]),



#CUSTOM SCHEMA FOR placements-activities
T.StructType([
    T.StructField("id",T.LongType(),True),
    T.StructField("name",T.StringType(),True),
    T.StructField("section",T.StringType(),True),
    T.StructField("Label01",T.StringType(),True),
    T.StructField("Label02",T.StringType(),True),
    T.StructField("Label03",T.StringType(),True),
    T.StructField("Label04",T.StringType(),True),
    T.StructField("Label05",T.StringType(),True),
    T.StructField("Label06",T.StringType(),True),
    T.StructField("Label07",T.StringType(),True),
    T.StructField("Label08",T.StringType(),True),
    T.StructField("Label09",T.StringType(),True),
    T.StructField("Label10",T.StringType(),True),
    T.StructField("Label11",T.StringType(),True),
    T.StructField("Label12",T.StringType(),True),
    T.StructField("Label13",T.StringType(),True),
    T.StructField("Label14",T.StringType(),True),
    T.StructField("Label15",T.StringType(),True),
    T.StructField("Label16",T.StringType(),True),
    T.StructField("Label17",T.StringType(),True),
    T.StructField("Label18",T.StringType(),True),
    T.StructField("Label19",T.StringType(),True),
    T.StructField("Label20",T.StringType(),True),
    T.StructField("budget",T.DoubleType(),True),
    T.StructField("budgetPeriodType",T.StringType(),True),
    T.StructField("bidPrice",T.DoubleType(),True),
    T.StructField("startDate",T.StringType(),True),
    T.StructField("endDate",T.StringType(),True),
    T.StructField("active",T.BooleanType(),True),
    T.StructField("paused",T.BooleanType(),True),
    T.StructField("capping",T.LongType(),True),
    T.StructField("cappingTypeName",T.StringType(),True),
    T.StructField("channelTypeName",T.StringType(),True)
]),



#CUSTOM SCHEMA FOR screensizes
T.StructType([
    T.StructField("id",T.LongType(),True),
    T.StructField("name",T.StringType(),True)
]),



#CUSTOM SCHEMA FOR tags
T.StructType([
    T.StructField("id",T.LongType(),True),
    T.StructField("name",T.StringType(),True),
    T.StructField("deleted",T.BooleanType(),True)
]),



#CUSTOM SCHEMA FOR zip-codes
T.StructType([
    T.StructField("cityId",T.LongType(),True),
    T.StructField("zipCode",T.StringType(),True),
    T.StructField("zipCodeId",T.LongType(),True)
])
]