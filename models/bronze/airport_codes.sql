{{
    config(
        materialized='table'
    )
}}

select * 
from read_files('{{var("input_path")}}/iata_data/airport_codes.json')
