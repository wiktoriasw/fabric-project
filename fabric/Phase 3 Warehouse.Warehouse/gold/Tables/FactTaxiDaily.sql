CREATE TABLE [gold].[FactTaxiDaily] (

	[date_key] int NULL, 
	[service_date] date NULL, 
	[year] int NULL, 
	[month] int NULL, 
	[is_weekend] bit NULL, 
	[service_type] varchar(8000) NULL, 
	[PUZoneKey] bigint NULL, 
	[trip_count] int NULL, 
	[total_trip_distance] float NULL, 
	[avg_trip_distance] float NULL, 
	[total_duration_sec] bigint NULL, 
	[avg_duration_min] float NULL, 
	[total_fare_amount] float NULL, 
	[avg_fare_amount] float NULL, 
	[total_amount_sum] float NULL, 
	[avg_total_amount] float NULL, 
	[total_passengers] int NULL, 
	[avg_passenger_count] float NULL
);