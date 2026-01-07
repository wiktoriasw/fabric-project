# Data Dictionary

This document describes the **analytics-ready (Gold layer)** datasets produced by the project. All analytical queries and reports are expected to consume data from the **Gold layer only**.

---

## Naming Conventions
- **Bronze**: raw source data
- **Silver**: cleaned, standardized, and conformed Delta tables
- **Gold**: fact and dimension tables modeled for analytics (Fabric Warehouse)

---

## Fact Tables

### FactTaxiDaily
**Description:**  
Daily aggregated NYC taxi activity by pickup zone and service type.

**Grain:**  
1 row per **date × pickup taxi zone × service type**

| Column | Description | Source / Derivation |
|------|------------|--------------------|
| `date_key` | Surrogate date key (YYYYMMDD) | Derived from `pickup date` |
| `service_date` | Calendar date of taxi pickup | `pickup_date` |
| `year` | Calendar year | Derived from `pickup date` |
| `month` | Calendar month | Derived from `pickup date` |
| `is_weekend` | Weekend indicator (Saturday/Sunday) | Derived from `pickup date`|
| `service_type` | Taxi service type (e.g. yellow, green) | NYC TLC |
| `PUZoneKey` | Pickup taxi zone identifier | NYC TLC zone lookup table |
| `trip_count` | Total number of completed trips | `COUNT(*)` |
| `total_trip_distance` | Total distance traveled (miles) | `SUM(trip_distance)` |
| `avg_trip_distance` | Average trip distance (miles) | `AVG(trip_distance)` |
| `total_duration_sec` | Total trip duration (seconds) | `SUM(duration_sec)` |
| `avg_duration_min` | Average trip duration (minutes) | `AVG(duration_min)` |
| `total_fare_amount` | Total fare amount (USD) | `SUM(fare_amount)` |
| `avg_fare_amount` | Average fare per trip (USD) | `AVG(fare_amount)` |
| `total_amount_sum` | Total charged amount (USD) | `SUM(total_amount)` |
| `avg_total_amount` | Average charged amount per trip (USD) | `AVG(total_amount)` |
| `total_passengers` | Total number of passengers | `SUM(passenger_count)` |
| `avg_passenger_count` | Average passengers per trip | `AVG(passenger_count)` |


**Data quality rules:**
- Trips with non-positive or implausibly large trip distances are excluded (0 < distance ≤ 50 miles)
- Trips with implausible fare amounts are excluded (USD 2 ≤ fare ≤ USD 300)
- Trips with implausible total charged amounts are excluded (USD 1 ≤ total amount ≤ USD 500)
- Trips with invalid passenger counts are excluded (1 ≤ passengers ≤ 6)
- Trips with implausibly short or long durations are excluded (2 ≤ duration ≤ 120 minutes)
- Drop-off timestamp must be later than pickup timestamp
- Records with missing mandatory keys (date, pickup zone) are excluded

---
## Added Key Metrics in Star Schema for FactTaxiDaily

This section documents additional analytical metrics defined in the semantic model.  
These metrics extend the base `FactTaxiDaily` table with domain-specific logic to support economic contextualization and trend analysis.

| Metric Name | Description | Detection Rule / Threshold |
|------------|-------------|----------------------------|
| **Taxi Total Revenue (USD)** | Total taxi revenue expressed in USD for the selected aggregation level. | Sum of `total_amount_sum` |
| **Taxi Total Revenue (EUR)** | Total taxi revenue converted from USD to EUR using the corresponding daily foreign exchange rate. | USD revenue × daily FX rate |
| **Taxi Revenue as % of GDP** | Ratio of total taxi revenue to gross domestic product (GDP), expressed as a percentage, used to contextualize taxi activity within macroeconomic indicators. | Taxi revenue ÷ annual GDP (USA) |
| **Total Trips** | Total number of completed taxi trips for the selected aggregation level. | Sum of `trip_count` |
| **Trips 7-Day Moving Average** | Seven-day rolling average of total taxi trips, used to smooth short-term fluctuations and highlight trends. | Rolling 7-day average |


### FactAirQualityDaily
**Description:**  
Daily aggregated air quality measurements by monitoring location and pollutant parameter.

**Grain:**  
1 row per **date × monitoring location × pollutant parameter**

| Column | Description | Source / Derivation |
|------|------------|--------------------|
| `date` | Calendar date of measurement | Source field |
| `date_key` | Surrogate date key (YYYYMMDD) | Derived from `date` |
| `location_id` | Air quality monitoring location identifier | OpenAQ |
| `parameter_name` | Pollutant parameter name (e.g., pm25, no2) | OpenAQ |
| `parameter_units` | Measurement unit (e.g.ppm, µg/m³) | OpenAQ |
| `avg_value` | Daily average pollutant value | `AVG(value)` |
| `q98_value` | 98th percentile pollutant value | `MAX(q98)` |
| `observations_count` | Number of observations used in aggregation | `COUNT(*)` |

**Data quality rules:**
- Data were filtered for NO₂ and PM2.5
- Pollutant measurement values are expected to be non-negative

## Added Key Metrics in Star Schema for FactAirQualityDaily

This section documents additional analytical metrics defined in the semantic model. These metrics extend the base FactAirQualityDaily table with domain-specific logic to support environmental analysis and anomaly detection.

| Metric Name | Description | Detection Rule / Threshold |
|------------|-------------|----------------------------|
| **98th Percentile Pollutant Level (Q98)** | The 98th percentile of pollutant concentration values for a given date and monitoring location, used to identify elevated pollution levels while reducing sensitivity to extreme outliers. | Computed per pollutant, date, and location |
| **PM2.5 Daily Average** | Daily average concentration of PM2.5 particulate matter for a given date and monitoring location. | Aggregated daily mean of PM2.5 |
| **PM2.5 Q98** | 98th percentile value of PM2.5 concentration, representing elevated PM2.5 pollution levels. | Q98 of PM2.5 values |
| **PM2.5 Spike Indicator** | Boolean indicator identifying whether a PM2.5 pollution spike occurred on a given day and monitoring location. | Daily average PM2.5 ≥ **30 µg/m³** |
| **PM2.5 Spike Value** | PM2.5 concentration value associated with a detected PM2.5 spike; returns the daily average value when a spike is detected, otherwise null. | Returned when PM2.5 ≥ **30 µg/m³** |
| **NO₂ Daily Average** | Daily average concentration of nitrogen dioxide (NO₂) for a given date and monitoring location. | Aggregated daily mean of NO₂|
| **NO₂ Q98** | 98th percentile value of NO₂ concentration, highlighting high pollution episodes. | Q98 of NO₂ values |
| **NO₂ Spike Indicator** | Boolean indicator identifying whether an NO₂ pollution spike occurred on a given day and monitoring location. | Daily average NO₂ ≥ **0.03 ppm** |
| **NO₂ Spike Value** | NO₂ concentration value associated with a detected NO₂ spike; returns the daily average value when a spike is detected, otherwise null. | Returned when NO₂ ≥ **0.03 ppm** |
| **Observations Count** | Number of individual air quality measurements used to compute daily aggregated values. | Count of observations per date, location, and pollutant |

### Data Availability Indicators

The following helper metrics are used in the semantic model to support reporting logic and visualization behavior in Power BI.

| Metric Name | Description | Rule |
|------------|-------------|------|
| **PM2.5 Availability Indicator** | Boolean indicator identifying whether PM2.5 measurements are available for a given date and monitoring location. | PM2.5 daily average is not null |
| **NO₂ Availability Indicator** | Boolean indicator identifying whether NO₂ measurements are available for a given date and monitoring location. | NO₂ daily average is not null |

---

## Dimension Tables

### DimDate
**Description:**  
Calendar date dimension providing common temporal attributes for analytical reporting.

**Grain:**  
1 row per **calendar date**

| Column | Description |
|------|------------|
| `DateKey` | Surrogate date key in YYYYMMDD format |
| `Date` | Calendar date |
| `Year` | Calendar year |
| `Quarter` | Calendar quarter (1–4) |
| `Month` | Calendar month number (1–12) |
| `MonthName` | Full month name |
| `DayOfMonth` | Day number within the month (1–31) |
| `DayOfWeek` | Day of week number |
| `DayName` | Full day name |
| `WeekOfYear` | Week number within the year |
| `IsWeekend` | Weekend indicator (1 = Saturday/Sunday, 0 = weekday) |

**Notes:**
- `DateKey` is derived as `YYYYMMDD` from the calendar date
- The dimension covers the period from **2014-01-01 to 2025-12-31**
- The table is generated programmatically to ensure continuous date coverage


---

### DimZone
**Description:**  
Taxi zone lookup dimension containing pickup and drop-off taxi zones.

**Grain:**  
1 row per **taxi zone**

| Column | Description |
|------|------------|
| `ZoneKey` | Taxi zone surrogate key (pickup or drop-off zone) |
| `Borough` | Borough name |
| `Zone` | Taxi zone name |
| `service_zone` | Service zone classification |

**Notes:**
- The dimension consolidates both pickup (PU) and drop-off (DO) taxi zones into a single lookup table
- Zone identifiers are sourced from NYC TLC taxi trip data


### DimGDP
**Description:**  
Annual gross domestic product (GDP) dimension by country.

**Grain:**  
1 row per **country × year**

| Column | Description |
|------|------------|
| `CountryCode` | Country code (e.g., USA) |
| `Year` | Calendar year |
| `DateKey` | Surrogate date key (YYYYMMDD) representing the reference year |
| `Date` | Reference date for the GDP record (typically year-based) |
| `GDP_USD` | Gross domestic product in current USD |

**Notes:**
- GDP values are sourced from the World Bank and reported on an annual basis
- `Date` and `DateKey` are included to enable joins with the date dimension and support time-based analysis
- Each record represents a unique combination of country and year


---

### DimFX
**Description:**  
Daily foreign exchange rate dimension for the USD/EUR currency pair.

**Grain:**  
1 row per **date × currency pair**

| Column | Description |
|------|------------|
| `DateKey` | Surrogate date key (YYYYMMDD) |
| `Date` | Calendar date of the exchange rate |
| `CurrencyPair` | Currency pair identifier (e.g., USD/EUR) |
| `BaseCurrency` | Base currency code (USD) |
| `QuoteCurrency` | Quote currency code (EUR) |
| `FXRate` | Daily foreign exchange rate |
| `Frequency` | Data frequency (e.g., daily) |
| `Status` | Observation status provided by the source |
| `Year` | Calendar year |

**Notes:**
- Exchange rates are sourced from the European Central Bank (ECB)
- The dimension supports time-based joins via `DateKey` and `Date`
- Each record represents a unique combination of date and currency pair
- Derived metrics are calculated at query time within the semantic model and do not alter the underlying Gold-layer tables.


## Units and Conventions
- Monetary values are expressed in **USD**, unless stated otherwise
- Distances are measured in **miles**
- Air quality values use units provided by OpenAQ, which were validated for consistency during ingestion (PM2.5 in **µg/m³**, NO₂ in **ppm**)
- Dates follow ISO 8601 format (YYYY-MM-DD)

---

## Data Coverage
- NYC Taxi trip data: **2014–2025**
- OpenAQ air quality data: **2025**
- World Bank GDP data (annual): **1975-2024**
- ECB foreign exchange rates (daily): **1999-2025**

---

## Usage Guidelines
- Gold tables are the **only supported source** for reporting and analytics
- Silver tables may be used for validation or exploratory analysis
- Bronze tables are not intended for analytical consumption

---

## Scope Note
This data dictionary reflects the **implemented scope** of the project.  
Additional attributes, metrics, or datasets may be introduced as future extensions.