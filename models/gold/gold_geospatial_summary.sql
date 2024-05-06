{{
    config(
        materialized='materialized_view',
        tblproperties={"delta.enableChangeDataFeed":"true"},
        zorder=['ArrDate','airline_name']
    )
}}

WITH

geospatial_aggregation AS (

    SELECT
    ArrDate
    ,airline_name
    ,origin_airport_name
    ,origin_city
    ,origin_coordinates_array[0] as Longitude
    ,origin_coordinates_array[1] as Latitude
    ,COUNT(*) AS Total_Flights
    ,COALESCE(ROUND(AVG(DepDelay),0),0) as Average_Departure_Delay_mins
    ,COALESCE(ROUND(MAX(DepDelay),0),0) as Max_Departure_Delay_mins
    ,ROUND(SUM(IFF(IsDepdelayed = 'YES',1,0)) / COUNT(*),2)*100 AS Percentage_Delayed_Departures
    FROM {{ ref('airline_trips_silver') }}
    GROUP BY ALL

),

final AS (
    SELECT
    *
    ,CASE
        WHEN Percentage_Delayed_Departures < 10 THEN 'Very Low'
        WHEN Percentage_Delayed_Departures BETWEEN 10 AND 30 THEN 'Low'
        WHEN Percentage_Delayed_Departures BETWEEN 30 AND 50 THEN 'Medium'
        WHEN Percentage_Delayed_Departures BETWEEN 50 AND 70 THEN 'High'
        WHEN Percentage_Delayed_Departures >= 70 THEN 'Severe'
    END AS Delay_Categories
    FROM geospatial_aggregation

)

SELECT * FROM final