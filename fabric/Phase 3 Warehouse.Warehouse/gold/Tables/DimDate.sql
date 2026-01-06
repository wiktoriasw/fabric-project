CREATE TABLE [gold].[DimDate] (

	[DateKey] int NOT NULL, 
	[Date] date NOT NULL, 
	[Year] smallint NOT NULL, 
	[Quarter] int NOT NULL, 
	[Month] int NOT NULL, 
	[MonthName] varchar(20) NOT NULL, 
	[DayOfMonth] int NOT NULL, 
	[DayOfWeek] int NOT NULL, 
	[DayName] varchar(20) NOT NULL, 
	[WeekOfYear] int NOT NULL, 
	[IsWeekend] bit NOT NULL
);