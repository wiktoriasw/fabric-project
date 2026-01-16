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

from pyspark.sql.functions import col
import json
import requests
from datetime import datetime


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DROPBOX_TOKEN = mssparkutils.credentials.getSecret('https://discorduser.vault.azure.net/', 'dropbox')

timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
file_name = f"OAquality_{timestamp}.json"
dropbox_path = f"/fabric_exports/{file_name}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.table("silver.openaq_air_quality")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

start_date = "2025-04-01"
end_date = "2025-04-30"

df = (
    df
    .filter(
        (col("date") >= start_date) &
        (col("date") <= end_date)
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rows = [row.asDict() for row in df.collect()]

export_data = {
    "period": {
        "start": start_date,
        "end": end_date
    },
    "row_count": len(rows),
    "rows": rows
}

json_string = json.dumps(export_data, indent=2, default=str)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# print(json_string)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

headers = {
    "Authorization": f"Bearer {DROPBOX_TOKEN}",
    "Content-Type": "application/octet-stream",
    "Dropbox-API-Arg": json.dumps({
        "path": dropbox_path,
        "mode": "add",
        "autorename": True
    })
}

response = requests.post(
    "https://content.dropboxapi.com/2/files/upload",
    headers=headers,
    data=json_string.encode("utf-8")
)

response.status_code


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
