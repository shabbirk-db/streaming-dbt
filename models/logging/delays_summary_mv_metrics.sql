{{
    config(
        materialized='view'
    )
}}

SELECT
    timestamp,
    message
FROM event_log(TABLE({{ ref('gold_delays_summary') }}))
WHERE
  event_type = 'planning_information'
ORDER BY timestamp  