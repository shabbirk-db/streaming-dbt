{{
    config(
        materialized='streaming_table'
    )
}}

SELECT 
    dest_airport_name
    ,window
    ,COUNT(*) AS no_same_day_dest_flights
FROM STREAM({{ ref('airline_trips_silver') }})
WATERMARK to_timestamp(date) as flight_timestamp DELAY OF INTERVAL 10 seconds airline_trips
GROUP BY
    dest_airport_name
    ,window(flight_timestamp,"1 day")