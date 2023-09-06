{{
    config(
        materialized='streaming_table'
    )
}}

select * 
from stream read_files('{{var("input_path")}}/airlines', format=>'json')
