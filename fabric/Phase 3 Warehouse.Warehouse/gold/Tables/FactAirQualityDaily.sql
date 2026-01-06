CREATE TABLE [gold].[FactAirQualityDaily] (

	[date] date NULL, 
	[date_key] int NULL, 
	[location_id] bigint NULL, 
	[parameter_name] varchar(8000) NULL, 
	[parameter_units] varchar(8000) NULL, 
	[avg_value] float NULL, 
	[q98_value] float NULL, 
	[observations_count] int NULL
);