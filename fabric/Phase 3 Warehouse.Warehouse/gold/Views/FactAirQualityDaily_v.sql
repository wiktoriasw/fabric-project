-- Auto Generated (Do not modify) 03981BD3763B1C4E60126790196DE177959AA4BE397DD91A1AB4767E8D19AB82
CREATE   VIEW gold.FactAirQualityDaily_v AS
SELECT
    d.DateKey,
    f.ParameterID,
    f.AvgValue,
    f.MinValue,
    f.MaxValue,
    f.ObsCount
FROM lh_bronze.dbo.factairqualitydaily f
JOIN gold.DimDate d
    ON d.[Date] = f.[Date];