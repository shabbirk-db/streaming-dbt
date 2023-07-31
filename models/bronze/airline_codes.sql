{{
    config(
        materialized='table'
    )
}}

select * 
from read_files('{{var("input_path")}}/iata_data/airline_codes.json')
