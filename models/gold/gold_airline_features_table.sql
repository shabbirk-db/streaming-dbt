{{
    config(
        materialized='streaming_table',
        liquid_clustered_by=['date','airline_name']
    )
}}

WITH airline_trips_silver AS (

    SELECT * FROM STREAM({{ ref('airline_trips_silver') }})

),


same_day_origin_flights AS (

    SELECT * FROM STREAM({{ ref('same_day_origin_flights') }})
),

same_day_dest_flights AS (

    SELECT * FROM STREAM({{ ref('same_day_dest_flights') }})
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
  ,no_same_day_dest_flights
  
FROM STREAM(airline_trips_silver)

WATERMARK to_timestamp(date) as flight_timestamp DELAY OF INTERVAL 10 seconds airline_trips 

LEFT JOIN STREAM(same_day_origin_flights) origin
    ON airline_trips.origin_airport_name = origin.origin_airport_name
    AND airline_trips.date BETWEEN origin.window.start AND origin.window.end

LEFT JOIN STREAM(same_day_dest_flights) dest
    ON airline_trips.dest_airport_name = dest.dest_airport_name
    AND airline_trips.date BETWEEN dest.window.start AND dest.window.end    