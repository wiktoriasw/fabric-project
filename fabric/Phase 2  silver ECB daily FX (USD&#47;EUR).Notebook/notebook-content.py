# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "64e04aa9-1d1a-4e60-864a-cf577f69cd39",
# META       "default_lakehouse_name": "lh_bronze",
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ecb = spark.table("ECB")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.catalog.listTables()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_ecb)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ecb = (
    df_ecb.select(
        F.col("TIME_PERIOD").alias("date"),
        F.col("OBS_VALUE").alias("usd_eur_rate"),
        F.col("OBS_STATUS").alias("obs_status"),
        F.col("FREQ").alias("freq"),
        F.col("CURRENCY").alias("base_ccy"),
        F.col("CURRENCY_DENOM").alias("quote_ccy")
    ))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ecb = (
    df_ecb.withColumn("date", F.to_date("date"))
    .withColumn("usd_eur_rate", F.col("usd_eur_rate").cast(T.DecimalType(18, 6)))
    .filter(F.col("obs_status") == F.lit("A"))
    .withColumn("year", F.year("date").cast("int"))
    .withColumn("pair", F.concat_ws("/", F.col("base_ccy"), F.col("quote_ccy")))
    .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ecb.select("date").distinct().count(), df_ecb.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ecb.filter(F.col("date").isNull()).count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ecb.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df_ecb.columns
]).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_ecb)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ecb.describe().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ecb.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(df_ecb.write
.format("delta")
.mode("append")
.saveAsTable("silver.ecb_fx_usd_eur_daily"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
