-- Auto Generated (Do not modify) 67BA8D6F0A714991CCA550327AB1417F3C06C313F547CB632E447378607B14B3
CREATE   VIEW gold.FactTaxiDaily_v AS
SELECT
    d.DateKey,
    f.ZoneID,
    f.ServiceType,
    f.Trips,
    f.TotalAmount,
    f.TotalFare,
    f.TotalTip,
    f.AvgTripDistance,
    f.AvgPassengerCount
FROM lh_bronze.dbo.facttaxidaily f
JOIN gold.DimDate d
    ON d.[Date] = f.[Date];