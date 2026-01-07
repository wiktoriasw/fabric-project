# Urban Mobility, Air Quality & Economic Analytics (Microsoft Fabric)

## Project Overview
This project develops a unified analytics platform on **Microsoft Fabric** that integrates datasets from three domains:
- **Urban mobility** - [NYC Taxi trip records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- **Environmental monitoring** - [OpenAQ air quality measurements](https://docs.openaq.org/about/about)
- **Macroeconomic indicators** - [World Bank GDP](https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.MKTP.CD?format=json) and [ECB foreign exchange rates](https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?format=csvdata)

The primary objective is to analyze the relationships between **urban mobility patterns, air quality, and economic trends** using a **medallion architecture (Bronze → Silver → Gold)** and analytics-ready data models. A secondary objective is to evaluate Microsoft Fabric’s end-to-end analytics capabilities, including data ingestion, transformation, modeling, and governance.

---

## Architecture Summary
- **Ingestion**: Fabric Pipelines and Dataflows Gen2  
- **Storage**: Fabric Lakehouse (Bronze and Silver layers) 
- **Transformation**: PySpark Notebooks  
- **Analytics**: Fabric Warehouse (star schema)  
- **Semantic Layer**: Fabric Semantic Model (facts, dimensions, measures)  
- **Visualization**: Power BI  

---

## Documentation Index
- [Data Dictionary](docs/data_dictionary.md)
- [Lineage Diagrams](docs/lineage_diagrams.md)

---

## Implemented Outcomes
- Ingested NYC Taxi (green and yellow) data covering the period 2014–2025
- Ingested OpenAQ air quality location metadata, selected locations relevant to sensors in New York City, and ingested measurement data for 2025
- Ingested macroeconomic data from the World Bank (GDP) and the European Central Bank (foreign exchange rates)
- Enriched taxi data using a zone lookup table derived from NYC Taxi trip records
- Processed and standardized data using PySpark notebooks and persisted results as Delta tables in the Silver layer
- Created fact and dimension tables in the Fabric Warehouse
- Implemented a star schema and exposed it through a semantic model
- Prepared Power BI report for analytical exploration