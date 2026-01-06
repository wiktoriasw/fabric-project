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

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import *
from functools import reduce

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

yellow_path = "Files/bronze/nyc_taxi/yellow/"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def list_parquet_files(base_path: str):
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)

    Path = jvm.org.apache.hadoop.fs.Path
    files = []

    def walk(path):
        for status in fs.listStatus(Path(path)):
            if status.isDirectory():
                walk(status.getPath().toString())
            else:
                p = status.getPath().toString()
                if p.lower().endswith(".parquet"):
                    files.append(p)

    walk(base_path)
    return files

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

PICKUP_CANDIDATES = ["tpep_pickup_datetime", "Trip_Pickup_DateTime", "pickup_datetime"]

def ensure_col(df, name, dtype=T.StringType()):
    if name not in df.columns:
        return df.withColumn(name, F.lit(None).cast(dtype))
    return df

def read_and_normalize(path):
    df = spark.read.parquet(path)

    # Ensure all candidate columns exist (as NULL if missing)
    df = ensure_col(df, "tpep_pickup_datetime", T.TimestampType())
    df = ensure_col(df, "Trip_Pickup_DateTime", T.StringType())
    df = ensure_col(df, "pickup_datetime", T.StringType())

    # Parse strings + coalesce into one canonical timestamp
    df = df.withColumn(
        "pickup_ts",
        F.coalesce(
            F.col("tpep_pickup_datetime").cast("timestamp"),
            F.to_timestamp("Trip_Pickup_DateTime", "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp("Trip_Pickup_DateTime", "yyyy-MM-dd HH:mm:ss.SSS"),
            F.to_timestamp("pickup_datetime", "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp("pickup_datetime", "yyyy-MM-dd HH:mm:ss.SSS"),
        )
    )
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

files = list_parquet_files(yellow_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfs = [read_and_normalize(p) for p in files]

df = dfs[0]
for d in dfs[1:]:
    df = df.unionByName(d, allowMissingColumns=True)

# df.groupBy(F.year("pickup_ts").alias("year")).count().orderBy("year").show(50)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Choosing the same date range as for green taxi

# CELL ********************

df_clean = df.filter(
    (F.year("pickup_ts") >= 2014) &
    (F.year("pickup_ts") <= 2025)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df_clean.select(
#     F.count("*").alias("rows"),
#     F.sum(F.col("pickup_ts").isNull().cast("int")).alias("pickup_ts_nulls"),
#     (F.avg(F.col("pickup_ts").isNull().cast("int"))*100).alias("pickup_ts_null_pct")
# ).show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Choosing needed columns

# CELL ********************

df_clean.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver = df_clean.select(
    "pickup_ts",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "Passenger_Count",
    "Trip_Distance",
   
    F.coalesce("Fare_Amt", "fare_amount").alias("Fare_Amt"),
    F.coalesce("Total_Amt", "total_amount").alias("Total_Amt"),

    "PULocationID",
    "DOLocationID",
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver.show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver.groupBy(F.year("pickup_ts").alias("year")).count().orderBy("year").show(20)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Checking start and stop location

# CELL ********************

silver.select(

    F.min("DOLocationID").alias("min_DOLocationID"),
    F.max("DOLocationID").alias("max_DOLocationID"),
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver.select(
    F.min("PULocationID").alias("min_PULocationID"),
    F.max("PULocationID").alias("max_PULocationID"),

).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Removing dropoff > pickup

# CELL ********************

silver_tpep_fixed = silver.filter(
    F.col("tpep_dropoff_datetime") >= F.col("tpep_pickup_datetime")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_tpep_fixed = (
    silver_tpep_fixed
    .withColumn(
        "duration_sec",
        F.unix_timestamp("tpep_dropoff_datetime") -
        F.unix_timestamp("tpep_pickup_datetime")
    )
    .withColumn(
        "duration_min",
        F.col("duration_sec") / 60.0
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_without_outliers = silver_tpep_fixed.filter(
    (F.col("Trip_Distance") > 0) & (F.col("trip_distance") <= 50) &
    (F.col("Fare_Amt") >= 2) & (F.col("fare_amount") <= 300) &
    (F.col("Total_Amt") >= 1) & (F.col("total_amount") <= 500) &
    (F.col("Passenger_Count") >= 1) & (F.col("passenger_count") <= 6) &
    F.col("duration_min").between(2, 120)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver.select(
    "Trip_Distance",
    "Fare_Amt",
    "Total_Amt",
    "Passenger_Count",
).summary("count", "min", "max").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_without_outliers.select(
    "Trip_Distance",
    "Fare_Amt",
    "Total_Amt",
    "Passenger_Count",
).summary("count", "min", "max").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Checking null values

# CELL ********************

nulls = df_without_outliers.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in df_without_outliers.columns
])
nulls.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Fixing the same start and end location

# CELL ********************

df_without_outliers = df_without_outliers.filter(
    ~(
        (F.col("PULocationID") == F.col("DOLocationID")) &
        (F.col("duration_min") < 5)
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

total = df_without_outliers.count()
distinct = df_without_outliers.distinct().count()
dups = total - distinct

print("total:", total)
print("distinct:", distinct)
print("duplicates:", dups, f"({dups/total*100:.4f}%)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_without_outliers.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Addding needed columns

# CELL ********************

#  |-- pickup_ts: timestamp (nullable = true)
#  |-- tpep_pickup_datetime: timestamp (nullable = true)
#  |-- tpep_dropoff_datetime: timestamp_ntz (nullable = true)
#  |-- Passenger_Count: double (nullable = true)
#  |-- Trip_Distance: double (nullable = true)
#  |-- Fare_Amt: double (nullable = true)
#  |-- Total_Amt: double (nullable = true)
#  |-- PULocationID: long (nullable = true)
#  |-- DOLocationID: long (nullable = true)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_without_outliers = (
    df_without_outliers
    .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
    .withColumn("dropoff_date", F.to_date("tpep_dropoff_datetime"))
    .withColumn("pickup_day", F.date_format("tpep_pickup_datetime", "E"))
    .withColumn("dropoff_day", F.date_format("tpep_dropoff_datetime", "E"))
    .withColumn("pickup_day_no", F.dayofweek("tpep_pickup_datetime"))
    .withColumn("dropoff_day_no", F.dayofweek("tpep_dropoff_datetime"))
    .withColumn("is_weekend", F.col("pickup_day_no").isin(1, 7))       # Sun or Sat
    .withColumn("pickup_time", F.date_format("tpep_pickup_datetime", "HH:mm"))
    .withColumn("dropoff_time", F.date_format("tpep_dropoff_datetime", "HH:mm"))
    .withColumn("date_key", F.date_format("pickup_date", "yyyyMMdd").cast("int"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_without_outliers.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Making the same standard with columns as in green

# CELL ********************

df_without_outliers = (
    df_without_outliers
    .withColumnRenamed("tpep_pickup_datetime", "lpep_pickup_datetime")
    .withColumnRenamed("tpep_dropoff_datetime", "lpep_dropoff_datetime")
    .withColumnRenamed("Passenger_Count", "passenger_count")
    .withColumnRenamed("Trip_Distance", "trip_distance")
    .withColumnRenamed("Fare_Amt", "fare_amount")
    .withColumnRenamed("Total_Amt", "total_amount")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_without_outliers = (
    df_without_outliers
    .withColumn("year", F.year("lpep_pickup_datetime"))
    .withColumn("month", F.month("lpep_pickup_datetime"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_without_outliers.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ordered_cols = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "pickup_date",
    "dropoff_date",
    "date_key",
    "year",
    "month",
    "pickup_day",
    "pickup_day_no",
    "dropoff_day",
    "dropoff_day_no",
    "is_weekend",
    "pickup_time",
    "dropoff_time",

    "PULocationID",
    "DOLocationID",

    "trip_distance",
    "duration_sec",
    "duration_min",

    "fare_amount",
    "total_amount",
    "payment_type",

    "passenger_count"
]

existing_cols = [c for c in ordered_cols if c in df_without_outliers.columns]

df_yellow_final = df_without_outliers.select(*existing_cols)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_yellow_final.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

len(df_without_outliers.columns), len(df_yellow_final.columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Adding taxi look up tables(Borough, zones, service_zone)

# CELL ********************

zone_path = "Files/Taxi_zone_lookup/taxi_zone_lookup.csv"

df_zone = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(zone_path)
)

df_zone.printSchema()
df_zone.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_enriched = (
    df_yellow_final
      # pickup zone
      .join(
          df_zone.selectExpr(
              "LocationID as PUZoneKey",
              "Borough as PU_Borough",
              "Zone as PU_Zone",
              "service_zone as PU_service_zone"
          ),
          df_yellow_final.PULocationID == F.col("PUZoneKey"),
          "left"
      ))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_enriched.show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_enriched_2 = (
    df_enriched
      # pickup zone
      .join(
          df_zone.selectExpr(
              "LocationID as DOZoneKey",
              "Borough as DO_Borough",
              "Zone as DO_Zone",
              "service_zone as DO_service_zone"
          ),
          df_enriched.DOLocationID == F.col("DOZoneKey"),
          "left"
      ))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_enriched_2.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Saving yellow taxi to silver layer

# CELL ********************

silver_yellow_path = "Files/silver/nyc_taxi/yellow"

(
    df_enriched_2
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("year", "month")
    .save(silver_yellow_path)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
