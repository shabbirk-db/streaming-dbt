{{
    config(
        materialized='materialized_view'
    )
}}

SELECT 
  airline_name
  ,COUNT(*) AS no_flights
  ,SUM(IF(isarrdelayed = TRUE,1,0)) AS tot_delayed
  ,tot_delayed*100/no_flights AS perc_delayed
FROM {{ref("flight_delays_scd1")}}
GROUP BY airline_name