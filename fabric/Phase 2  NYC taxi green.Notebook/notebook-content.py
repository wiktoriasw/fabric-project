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

green_path  = "Files/bronze/nyc_taxi/green/"

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

def normalize_one_file(fp):
    df = spark.read.parquet(fp)

    if "passenger_count" in df.columns:
        df = df.withColumn(
            "passenger_count",
            F.col("passenger_count").cast("double").cast("long")
        )

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

files = list_parquet_files(green_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

files[:5]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfs = [normalize_one_file(fp) for fp in files]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_green_fixed = reduce(
    lambda a, b: a.unionByName(b, allowMissingColumns=True),
    dfs
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_green_fixed.printSchema()
df_green_fixed.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_green_fixed = (
    df_green_fixed
    .select(
        "lpep_pickup_datetime",
        "lpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "trip_distance",
        "fare_amount",
        "total_amount",
        "passenger_count",
    ))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_green_fixed.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_green_fixed.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_green_fixed = (
    df_green_fixed
    .withColumn("year", F.year("lpep_pickup_datetime"))
    .withColumn("month", F.month("lpep_pickup_datetime"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_green_fixed.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Fixing date range

# CELL ********************

df_green_fixed.groupBy("year").agg(
    F.min("month").alias("min_month"),
    F.max("month").alias("max_month"),
    F.countDistinct("month").alias("month_count")
).orderBy("year").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_green_fixed.filter(F.col("year") >= 2030).groupBy("year").count().orderBy("year").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_green_fixed.filter(F.col("year") == 2062).select(
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_distance",
    "total_amount",
    "passenger_count"
).show(50, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_green_fixed.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_green_years_fixed = df_green_fixed.filter((F.col("year") >= 2014) & (F.col("year") <= 2025))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_green_years_fixed.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_green_years_fixed.groupBy("year").agg(
    F.min("month").alias("min_month"),
    F.max("month").alias("max_month"),
    F.countDistinct("month").alias("month_count")
).orderBy("year").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Fixing dropoffs < pickups

# CELL ********************

df_green_years_fixed.filter(
    F.col("lpep_dropoff_datetime") < F.col("lpep_pickup_datetime")
).count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_green_years_fixed.filter(
    F.col("lpep_dropoff_datetime") < F.col("lpep_pickup_datetime")
).groupBy("year").count().orderBy("year").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_green_years_fixed.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_green_years_fixed.filter(
    F.col("lpep_dropoff_datetime") < F.col("lpep_pickup_datetime")
).select(
    "year",
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "trip_distance",
    "total_amount",
    "passenger_count"
).orderBy(F.rand()).show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_green_years_lpep_fixed = df_green_years_fixed.filter(
    F.col("lpep_dropoff_datetime") >= F.col("lpep_pickup_datetime")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_green_years_lpep_fixed.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_green_years_lpep_fixed.show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Checking location values

# CELL ********************

df_green_years_lpep_fixed.select(
    F.min("PULocationID").alias("min_PULocationID"),
    F.max("PULocationID").alias("max_PULocationID"),

).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_green_years_lpep_fixed.select(

    F.min("DOLocationID").alias("min_DOLocationID"),
    F.max("DOLocationID").alias("max_DOLocationID"),
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Checking same start and end location


# CELL ********************

df_green_years_lpep_fixed.filter(
    F.col("PULocationID") == F.col("DOLocationID")
).count()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_green_years_lpep_fixed = (
    df_green_years_lpep_fixed
    .withColumn(
        "duration_sec",
        F.unix_timestamp("lpep_dropoff_datetime") -
        F.unix_timestamp("lpep_pickup_datetime")
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

df_green_years_lpep_fixed.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_green_years_lpep_loc_fixed = df_green_years_lpep_fixed.filter(
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

# MARKDOWN ********************

# ## Checking trip distance

# CELL ********************

df_green_years_lpep_loc_fixed.select(
    F.min("trip_distance").alias("min_trip_distance"),
    F.max("trip_distance").alias("max_trip_distance"),
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_green_years_lpep_loc_fixed.filter(
    F.col("trip_distance") < 0
).select(
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "trip_distance",
    "fare_amount",
    "total_amount",
    "passenger_count"
).show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_green_years_lpep_loc_fixed.filter(
    F.col("trip_distance") <= 0
).count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_green_years_lpep_loc_fixed.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Checking fare amount

# CELL ********************

df_green_years_lpep_loc_fixed.select(
    F.min("fare_amount").alias("min_fare_amount"),
    F.max("fare_amount").alias("max_fare_amount"),
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_green_years_lpep_loc_fixed.filter(
    F.col("fare_amount") < 0
).select(
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "trip_distance",
    "fare_amount",
    "total_amount",
    "passenger_count"
).show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Chceking total amount

# CELL ********************

df_green_years_lpep_loc_fixed.select(
    F.min("total_amount").alias("min_total_amount"),
    F.max("total_amount").alias("max_total_amount"),

).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_green_years_lpep_loc_fixed.filter(
    F.col("total_amount") == 989970.39
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Checking passenger count

# CELL ********************

df_green_years_lpep_loc_fixed.select(
    F.min("passenger_count").alias("min_passenger_count"),
    F.max("passenger_count").alias("max_passenger_count"),

).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_green_years_lpep_loc_fixed.filter(
    F.col("passenger_count") == 48
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Deleting outliers

# CELL ********************

df_green_years_lpep_loc_fixed.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_green_years_lpep_loc_fixed.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_without_outliers = df_green_years_lpep_loc_fixed.filter(
    (F.col("trip_distance") > 0) & (F.col("trip_distance") <= 50) &
    (F.col("fare_amount") >= 2) & (F.col("fare_amount") <= 300) &
    (F.col("total_amount") >= 1) & (F.col("total_amount") <= 500) &
    (F.col("passenger_count") >= 1) & (F.col("passenger_count") <= 6) &
    F.col("duration_min").between(2, 120)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

before = df_green_years_lpep_loc_fixed.count()
after = df_without_outliers.count()

before, after, (before - after), (before - after)/before*100

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_without_outliers.select(
    "trip_distance",
    "fare_amount",
    "total_amount",
    "passenger_count",
    "year"
).summary("count", "min", "max").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Checking nulls

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

# ## Adding columns

# CELL ********************

df_without_outliers.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_without_outliers = (
    df_without_outliers
    .withColumn("pickup_date", F.to_date("lpep_pickup_datetime"))
    .withColumn("dropoff_date", F.to_date("lpep_dropoff_datetime"))
    .withColumn("pickup_day", F.date_format("lpep_pickup_datetime", "E"))
    .withColumn("dropoff_day", F.date_format("lpep_dropoff_datetime", "E"))
    .withColumn("pickup_day_no", F.dayofweek("lpep_pickup_datetime"))
    .withColumn("dropoff_day_no", F.dayofweek("lpep_dropoff_datetime"))
    .withColumn("is_weekend", F.col("pickup_day_no").isin(1, 7))
    .withColumn("pickup_time", F.date_format("lpep_pickup_datetime", "HH:mm"))
    .withColumn("dropoff_time", F.date_format("lpep_dropoff_datetime", "HH:mm"))
    .withColumn("date_key", F.date_format("pickup_date", "yyyyMMdd").cast("int"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_without_outliers.show(20)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Checking duplicated values

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

key_cols = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_distance",
    "total_amount",
    "passenger_count"
]

dup_keys = (
    df_without_outliers.groupBy(*key_cols)
        .count()
        .filter("count > 1")
)

dup_keys.show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dup_key_rows = dup_keys.count()
print("duplicate keys:", dup_key_rows)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Making finale df before saving

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

df_green_final = df_without_outliers.select(*existing_cols)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_green_final.show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

len(df_without_outliers.columns), len(df_green_final.columns)

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
    df_green_final
      .join(
          df_zone.selectExpr(
              "LocationID as PUZoneKey",
              "Borough as PU_Borough",
              "Zone as PU_Zone",
              "service_zone as PU_service_zone"
          ),
          df_green_final.PULocationID == F.col("PUZoneKey"),
          "left"
      ))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 

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

df_enriched_2.show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Saving green taxi to silver layer

# CELL ********************

silver_green_path = "Files/silver/nyc_taxi/green"

(
    df_enriched_2
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("year", "month")
    .save(silver_green_path)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
