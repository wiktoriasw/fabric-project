CREATE TABLE [gold].[DimLocation] (

	[location_id] bigint NULL, 
	[location_name] varchar(8000) NULL, 
	[country_id] bigint NULL, 
	[country_code] varchar(8000) NULL, 
	[country_name] varchar(8000) NULL, 
	[latitude] float NULL, 
	[longitude] float NULL
);