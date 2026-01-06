# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "64e04aa9-1d1a-4e60-864a-cf577f69cd39",
# META       "default_lakehouse_name": "Lakehouse",
# META       "default_lakehouse_workspace_id": "75f1c875-0ee7-47ad-a0a7-385b33d47af7",
# META       "known_lakehouses": [
# META         {
# META           "id": "64e04aa9-1d1a-4e60-864a-cf577f69cd39"
# META         },
# META         {
# META           "id": "b61aff81-6991-4cbb-8f4b-880569e38a47"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql import types as T

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sensors_location = spark.table("openaq_NYC_raw")
sensors_measure = spark.table("openaq_days_yearly_raw")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sensor measure

# CELL ********************

sensors_measure.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sensors_measure.select("name").distinct().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(sensors_measure)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sensors_measure =(sensors_measure
   .withColumnRenamed("id", "parameter_id")
   .withColumnRenamed("name", "parameter_name")
   .withColumnRenamed("units", "parameter_units")
   .withColumnRenamed("utc", "utc_from")
   .withColumnRenamed("utc.1", "utc_to")
   .withColumn("date", F.to_date("utc_from"))
   .withColumn("year", F.year("utc_from"))
   .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
   .withColumn("parameter_name", F.lower(F.trim(F.col("parameter_name"))))
   .withColumn("parameter_units", F.lower(F.trim(F.col("parameter_units"))))

)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sensors_measure.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sensors_measure.select("parameter_name").distinct().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

exprs = []
for name, dtype in sensors_measure.dtypes:
    col = F.col(name)
    if dtype in ("double", "float"):
        miss = col.isNull() | F.isnan(col)
    else:
        miss = col.isNull()
    exprs.append(F.sum(F.when(miss, 1).otherwise(0)).alias(name))

display(sensors_measure.agg(*exprs))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sensors_measure = sensors_measure.filter(
    F.col("parameter_name").isin("pm25", "no2")
)

display(sensors_measure)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sensors_measure.groupBy("parameter_name", "parameter_units").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sensors_measure.groupBy("parameter_name").agg(
    F.min("date").alias("min_date"),
    F.max("date").alias("max_date")
).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sensors_measure.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    sensors_measure
    .groupBy(
        "sensor_id",
        "parameter_id",
        "parameter_name",
        "parameter_units",
        "utc_from",
        "utc_to",
        "q02",
        "q25",
        "q75"
    )
    .count()
    .filter(F.col("count") > 1)
    .orderBy(F.desc("count"))
    .show(20, False)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sensors_measure.filter(
    (F.col("sensor_id") == 2644) &
    (F.col("parameter_name") == "no2") &
    (F.col("utc_from") == F.to_timestamp(F.lit("2025-06-08 04:00:00")))
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sensors_measure.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sensors_measure_dedup = sensors_measure.dropDuplicates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sensors_measure_dedup.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    sensors_measure_dedup
    .groupBy(
        "sensor_id",
        "parameter_id",
        "parameter_name",
        "parameter_units",
        "utc_from",
        "utc_to",
        "q02",
        "q25",
        "q75"
    )
    .count()
    .filter(F.col("count") > 1)
    .orderBy(F.desc("count"))
    .show(20, False)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sensors_measure_final = sensors_measure_dedup

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sensor location

# CELL ********************

sensors_location.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(sensors_location)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sensors_location = (sensors_location
    .withColumn("sensor_parameter_name", F.lower(F.col("sensor_parameter_name")))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sensors_location.select("sensor_parameter_name").distinct().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sensors_location = sensors_location.filter(
    F.col("sensor_parameter_name").isin("pm25", "no2")
)

display(sensors_location)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sensors_location.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sensors_location = (sensors_location
.withColumnRenamed("id", "location_id")
.withColumnRenamed("sensor_name", "sensor_parameter_units_2")
.withColumnRenamed("name", "location_name")
.withColumnRenamed("sensor_parameter_name", "parameter_name")
.withColumnRenamed("id.1", "country_id")
.withColumnRenamed("code", "country_code")
.withColumnRenamed("name.1", "country_name")
.withColumnRenamed("isMobile", "is_mobile")
.withColumnRenamed("isMonitor", "is_monitor")
.withColumnRenamed("datetimeFirst_utc", "datetime_first_utc")
.withColumnRenamed("datetimeLast_utc", "datetime_last_utc")
.withColumn("location_name", F.trim(F.col("location_name")))
.withColumn("locality", F.trim(F.col("locality")))
.withColumn("date", F.to_date("datetime_first_utc"))
.withColumn("year", F.year("datetime_first_utc"))
.withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sensors_location.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(sensors_location)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sensors_location.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sensors_location = sensors_location.select(
"location_id",
"location_name",
"country_id",
"country_code",
"country_name",
"sensor_id",
"sensor_parameter_id",
"parameter_name",
"sensor_parameter_units",
"latitude",
"longitude",
"datetime_first_utc",
"datetime_last_utc",
"date",
"year",
"date_key"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sensors_location.select(
    F.sum(F.col("latitude").isNull().cast("int")).alias("null_lat"),
    F.sum(F.col("longitude").isNull().cast("int")).alias("null_lon"),
    F.sum((F.col("latitude").isNull() | F.col("longitude").isNull()).cast("int")).alias("null_lat_or_lon"),
    F.count("*").alias("rows_total")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    sensors_location
    .groupBy("sensor_id", "parameter_name")
    .count()
    .filter(F.col("count") > 1)
    .orderBy(F.desc("count"))
    .show(20, False)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    sensors_location
    .groupBy(
        "location_id",
        "location_name",
        "country_id",
        "country_code",
        "country_name",
        "sensor_id",
        "sensor_parameter_id",
        "parameter_name",
        "sensor_parameter_units",
        "latitude",
        "longitude",
        "datetime_first_utc",
        "datetime_last_utc"
    )
    .count()
    .filter(F.col("count") > 1)
    .orderBy(F.desc("count"))
    .show(20, False)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sensors_location.printSchema()
sensors_measure.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

m = (
    sensors_measure_final
    .select(
       "value",
       "parameter_id",
       "parameter_name",
       "parameter_units",
       "utc_from",
       "utc_to",
       "q02",
       "q25",
       "q75",
       "q98",
       "sensor_id",
       "date",
       "year",
       "date_key"
    )
    .alias("m")
)

l = (
    sensors_location
    .select(
        "sensor_id",
        "parameter_name",
        "location_id",
        "location_name",
        "country_id",
        "country_code",
        "country_name",
        "latitude",
        "longitude",
    )
    .alias("l")
)

joined = m.join(l, ["sensor_id", "parameter_name"], "inner")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# joined_left = m.join(
#     l,
#     ["sensor_id", "parameter_name"],
#     "left"
# )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# m.count(), l.count(), joined.count(), joined_left.count()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(joined)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

joined.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

joined.filter(
    F.col("location_name").isNull() |
    (F.col("location_name") == "")
).count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

joined.filter(
    F.col("location_id") == 4569917).show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

joined_final = joined.select(
    "sensor_id",
    "parameter_id",
    "parameter_name",
    "parameter_units",

    "utc_from",
    "utc_to",
    "date",
    "year",
    "date_key",

    "value",
    "q02", "q25", "q75", "q98",

    "location_id",
    "location_name",
    "country_id",
    "country_code",
    "country_name",
    "latitude",
    "longitude",
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

len(joined.columns), len(joined_final.columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sensors_location.filter(
    F.col("location_name").isin("Bayonne", "Bayonne, NJ")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(joined_final.write
    .format("delta")
    .mode("append")
    .saveAsTable("silver.openaq_air_quality")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
