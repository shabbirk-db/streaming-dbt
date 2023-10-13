{{
    config(
        materialized='view'
    )
}}

SELECT
    *
FROM event_log(TABLE({{ ref('gold_delays_summary') }}))