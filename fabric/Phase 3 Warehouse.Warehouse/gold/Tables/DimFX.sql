CREATE TABLE [gold].[DimFX] (

	[DateKey] int NULL, 
	[Date] date NULL, 
	[CurrencyPair] varchar(8000) NULL, 
	[BaseCurrency] varchar(8000) NULL, 
	[QuoteCurrency] varchar(8000) NULL, 
	[FXRate] decimal(18,6) NULL, 
	[Frequency] varchar(8000) NULL, 
	[Status] varchar(8000) NULL, 
	[Year] int NULL
);