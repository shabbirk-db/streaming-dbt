{{
    config(
        materialized='view'
    )
}}

SELECT
    *
FROM event_log(TABLE({{ ref('airline_trips_bronze') }}))
WHERE event_type = 'flow_progress'
ORDER BY timestamp  