CREATE OR REPLACE VIEW experiment2 AS
SELECT
    DATE(a.datetime_iso) AS datetime_iso,
    'reseda' AS station,
    a."co",
    a."no2",
    a."o3",
    a."pm25",
    NULL AS "so2",
    w."TMAX",
    w."TMIN",
    w."PRCP",
    e."value" AS cenergy
FROM trusted_airQuality_reseda_agg a
LEFT JOIN trusted_weather_reseda w
    ON DATE(a.datetime_iso) = DATE(w.datetime_iso)
LEFT JOIN trusted_electricity_data e
    ON DATE(a.datetime_iso) = DATE(e.datetime_iso)

UNION

SELECT
    DATE(a.datetime_iso) AS datetime_iso,
    'downtown' AS station,
    a."co",
    a."no2",
    a."o3",
    a."pm25",
    a."so2",
    w."TMAX",
    w."TMIN",
    w."PRCP",
    e."value" AS cenergy
FROM trusted_airQuality_downtown_agg a
LEFT JOIN trusted_weather_downtown w
    ON DATE(a.datetime_iso) = DATE(w.datetime_iso)
LEFT JOIN trusted_electricity_data e
    ON DATE(a.datetime_iso) = DATE(e.datetime_iso)

UNION

SELECT
    DATE(a.datetime_iso) AS datetime_iso,
    'long_beach' AS station,
    NULL AS "co",
    a."no2",
    a."o3",
    NULL AS "pm25",
    a."so2",
    w."TMAX",
    w."TMIN",
    w."PRCP",
    e."value" AS cenergy
FROM trusted_airQuality_long_beach_agg a
LEFT JOIN trusted_weather_long_beach w
    ON DATE(a.datetime_iso) = DATE(w.datetime_iso)
LEFT JOIN trusted_electricity_data e
    ON DATE(a.datetime_iso) = DATE(e.datetime_iso)