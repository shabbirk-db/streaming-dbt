{{
    config(
        materialized="incremental",
        unique_key="unique_id",
        incremental_strategy="merge",
        tblproperties = {
        'delta.enableChangeDataFeed': 'true'
        }
    )
}}

select
    airline_name
    , origin_city
    , dest_city
    , arrtime
    , isarrdelayed
    , airline_name||'-'||origin_city||'-'||dest_city as unique_id
from {{ ref("airline_trips_silver") }}