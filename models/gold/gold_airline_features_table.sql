{{
    config(
        materialized='materialized_view'
    )
}}

SELECT 
  Date
  ,airline_name
  ,COUNT(*) AS no_flights
  ,SUM(Distance) AS tot_distance
FROM {{ref("airline_trips_silver")}}
GROUP BY ALL