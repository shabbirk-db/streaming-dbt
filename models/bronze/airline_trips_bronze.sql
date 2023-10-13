{{
    config(
        materialized='streaming_table'
    )
}}

select 
    * 
    ,_metadata.file_modification_time as file_modification_time
from stream read_files('{{var("input_path")}}/airlines', format=>'json')
