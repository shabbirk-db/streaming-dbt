{{
    config(
        materialized='view'
    )
}}

SELECT
    timestamp,
    message
FROM event_log(TABLE({{ ref('gold_geospatial_summary') }}))
WHERE
  event_type = 'planning_information'