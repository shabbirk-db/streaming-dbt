{{
    config(
        materialized='streaming_table',
        liquid_clustered_by=['ArrDate','airline_name'],
        enabled=False
    )
}}

WITH airline_trips_silver AS (

    SELECT * FROM STREAM({{ ref('airline_trips_silver') }})

),


same_day_origin_flights AS (

    SELECT * FROM STREAM({{ ref('same_day_origin_flights') }})
)

SELECT 
  delay_id
  ,ArrDelay + DepDelay AS TotalDelayed
  ,airline_name
  ,airline_trips.origin_airport_name
  ,airline_trips.dest_airport_name
  ,ActualElapsedTime
  ,Distance
  ,no_same_day_origin_flights
  
FROM STREAM(airline_trips_silver)

WATERMARK ArrTimestamp DELAY OF INTERVAL 10 seconds airline_trips 

INNER JOIN STREAM(same_day_origin_flights) origin
    ON airline_trips.origin_airport_name = origin.origin_airport_name
    AND airline_trips.ArrTimestamp BETWEEN origin.window.start AND origin.window.end