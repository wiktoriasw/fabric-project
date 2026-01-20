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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import col, from_unixtime, to_utc_timestamp

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.json("Files/OpenWeather/CurrentWeather.json")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

flat = df.select(
    col("id").alias("city_id"),
    col("name").alias("city_name"),
    col("sys.country").alias("country"),
    col("coord.lat").alias("lat"),
    col("coord.lon").alias("lon"),

    col("dt").alias("dt_epoch"),
    to_utc_timestamp(from_unixtime(col("dt")), "UTC").alias("dt_utc"),

    col("timezone").alias("timezone_sec"),
    col("visibility").alias("visibility_m"),

    col("clouds.all").alias("clouds_all"),

    col("main.temp").alias("temp"),
    col("main.feels_like").alias("feels_like"),
    col("main.humidity").alias("humidity"),
    col("main.pressure").alias("pressure"),
    col("main.temp_min").alias("temp_min"),
    col("main.temp_max").alias("temp_max"),

    col("wind.speed").alias("wind_speed"),
    col("wind.deg").alias("wind_deg"),

    col("weather")[0]["main"].alias("weather_main"),
    col("weather")[0]["description"].alias("weather_description")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

flat.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

flat.write.mode("append").saveAsTable("silver.weather_observation_flat")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
