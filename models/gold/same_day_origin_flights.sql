{{
    config(
        materialized='streaming_table'
        ,enabled=False
    )
}}

SELECT 
    origin_airport_name
    ,window
    ,COUNT(*) AS no_same_day_origin_flights
FROM STREAM({{ ref('airline_trips_silver') }})
WATERMARK ArrTimestamp DELAY OF INTERVAL 10 seconds airline_trips
GROUP BY
    origin_airport_name
    ,window(ArrTimestamp,"1 day")    