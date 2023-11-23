# Streaming Tables and Materialized Views Demo for dbt on Databricks

### The demo environment:

You are receiving a feed of airline trips across the United States over time, and want to stream this into your Databricks Lakehouse to generate live insights for a BI tool, as well as live updating features for a ML model to predict delays of future trips.

This dbt project includes a helper notebook to simulate a stream of airlines data (packaged as example data in all Databricks environments), and a series of transformations to enrich this data and maintain the layer of Materialized Views needed for your downstream BI.

### What this demonstrates:

- How to ingest any files directly from your cloud storage with the new read_files syntax
- How to configure Streaming Tables and Materialized Views in dbt on Databricks
- How to access streaming logs (akin to Delta Live Tables logs) for both Streaming Tables and Materialized Views
- Advanced streaming concepts like watermarking, stream-stream joins and windowed aggregations

### To get started:
1. Clone the repo into a Databricks environment and run the helper notebook to generate a stream of .json source files.
   - Configure the Volumes path to land the files in and use this as the `input_path` var in your dbt_project.yml to simulate Autoloader streaming ingestion
  
2. Setup the dbt project:
  - Replace the `input_path` in the dbt_project.yml vars with the configured path above.
  - Run the following dbt commands: 
    - `dbt deps`
    - `dbt build`
   
3. To try an incremental refresh, return to the helper notebook and stream additional files, before kicking off the dbt job again!

### The data model:

![image](https://github.com/shabbirk-db/streaming-dbt/assets/91239704/5110232c-93b6-42d8-ba3c-500f255f229b)


