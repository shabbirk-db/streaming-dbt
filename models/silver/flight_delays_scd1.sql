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

{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where Date > DATEADD(DAY, -3, (select max(Date) as max_date from {{ ref("airline_trips_silver") }})) 
{% endif %}