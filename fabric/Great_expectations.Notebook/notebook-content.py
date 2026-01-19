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
# META     },
# META     "environment": {
# META       "environmentId": "171d9a49-8912-8cca-4e35-0be03553aafb",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

import great_expectations as gx
from great_expectations.expectations.expectation_configuration import ExpectationConfiguration
import requests

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

report_title = "OpenAQ — Data Quality Report"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DISCORD_HOOK = mssparkutils.credentials.getSecret('https://discorduser.vault.azure.net/', 'discord')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_openaq = spark.sql("SELECT * FROM Lakehouse.dbo.openaq_days_yearly_raw")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

data_source_name = "OpenAQ"
data_asset_name = "openaq_days_yearly_raw"
batch_definition_name = "my_batch_definition"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Functions

# CELL ********************

def ge_quality_report(validation_result,report_title, *, max_failures=10, max_examples=8) -> str:
    d = validation_result.to_json_dict()
    status = "✅ PASSED" if d.get("success") else "❌ FAILED"

    suite_name = (
        d.get("suite_name")
    )

    stats = d.get("statistics", {})
    evaluated = stats.get("evaluated_expectations", 0)
    failed = stats.get("unsuccessful_expectations", 0)
    successful = stats.get("successful_expectations", 0)


    lines = []
    lines.append(f"**{report_title}**")
    lines.append(f"{status}")
    lines.append(f"Suite: `{suite_name}`")
    lines.append(f"Expectations: {successful}/{evaluated} passed | Failed: {failed}")

    # show failures (if any)
    if not d.get("success"):
        lines.append("")
        lines.append("Failures:")
        failures = [r for r in d.get("results", []) if not r.get("success")]
        for r in failures[:max_failures]:
            etype = r.get("expectation_config", {}).get("type", "unknown")
            kwargs = r.get("expectation_config", {}).get("kwargs", {})
            result = r.get("result", {})

            unexpected = result.get("unexpected_count", 0)
            examples = result.get("partial_unexpected_list", [])[:max_examples]

            lines.append(f"- `{etype}` {kwargs} | unexpected={unexpected} | examples={examples}")

    return "\n".join(lines)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def send_to_discord(webhook_url: str, message: str):
    r = requests.post(webhook_url, json={"content": message}, timeout=15)
    r.raise_for_status()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Settings up Great Expectations

# CELL ********************

context = gx.get_context()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

suite_name = "Bronze_Open_AQ_measurements"

try:
    suite = context.suites.get(suite_name)
    print("Loaded existing suite")
except Exception:
    suite = gx.ExpectationSuite(suite_name)
    suite = context.suites.add(suite)
    print("Created new suite")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

expectations = [
    gx.expectations.ExpectColumnValuesToBeOfType(
    column="utc", type_= 'TimestampType'),
    gx.expectations.ExpectColumnValuesToBeOfType(
    column="value", type_= 'DoubleType'),
    gx.expectations.ExpectColumnValuesToBeOfType(
    column="id", type_= 'LongType'),
    gx.expectations.ExpectColumnValuesToBeOfType(
    column="name", type_= 'StringType'),
    gx.expectations.ExpectColumnValuesToBeOfType(
    column="units", type_= 'StringType'),
    gx.expectations.ExpectColumnValuesToBeOfType(
    column="sensor_id", type_= 'LongType'),
    
    gx.expectations.ExpectColumnToExist(
         column="value"),
    gx.expectations.ExpectColumnToExist(
         column="id"),
    gx.expectations.ExpectColumnToExist(
         column="name"),
    gx.expectations.ExpectColumnToExist(
         column="units"),
    gx.expectations.ExpectColumnToExist(
         column="sensor_id"),

    gx.expectations.ExpectColumnValuesToNotBeNull(
        column="value"),
    gx.expectations.ExpectColumnValuesToNotBeNull(
        column="id"),
    gx.expectations.ExpectColumnValuesToNotBeNull(
        column="name"),
    gx.expectations.ExpectColumnValuesToNotBeNull(
        column="units"),
    gx.expectations.ExpectColumnValuesToNotBeNull(
        column="sensor_id")
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for exp in expectations:
    suite.add_expectation(exp)

context.suites.add_or_update(suite)
print("Suite saved")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

suite

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Running validation

# MARKDOWN ********************

# ## Running validation

# CELL ********************

data_source = context.data_sources.add_spark(data_source_name) 
data_asset = data_source.add_dataframe_asset(data_asset_name) 
batch_definition = data_asset.add_batch_definition_whole_dataframe(batch_definition_name) 
batch = batch_definition.get_batch(batch_parameters={"dataframe": bronze_openaq}) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Printing validation results

# CELL ********************

validation_result = batch.validate(suite)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

validation_result.to_json_dict()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sending discord message

# CELL ********************

report = ge_quality_report(validation_result, report_title)

send_to_discord(DISCORD_HOOK, report)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
