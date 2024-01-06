# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Instructions
# MAGIC
# MAGIC - Attach a multi-node, UC enabled cluster
# MAGIC
# MAGIC - Designate a catalog and schema as a landing zone for all simulated stream data
# MAGIC   - Enter these into the notebook widget, e.g. Catalog: `uc_shabbirkhanbhai` and Schema: `airlines_source`
# MAGIC   - The script will use this location to create several UC Volumes for the demo
# MAGIC     - i.e. The volumes would be created in the path: `dbfs:/Volumes/uc_shabbirkhanbhai/airlines_source`
# MAGIC     - Take note of the path created, as this will be needed in the `dbt_project.yml` as a data source
# MAGIC
# MAGIC - Initialise the stream (may take ~6 minutes)
# MAGIC
# MAGIC - Simulate a stream of .json files, either through arrival('once') or 'continuous'
# MAGIC   - There are up to 200 files to stream, I recommend keeping it small at first, up to 10, and then potentially trying larger after the dbt project successfully runs

# COMMAND ----------

dbutils.widgets.text("input_catalog", "hive_metastore")
dbutils.widgets.text("input_schema", "airlines_source")

input_catalog = dbutils.widgets.get("input_catalog")
input_schema = dbutils.widgets.get("input_schema")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Initialise Folder structure

# COMMAND ----------

# MAGIC %run ./_resources/autoloader-setup-uc-volumes $mode = "initialise" $input_catalog=$input_catalog $input_schema=$input_schema

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Simulate single new file

# COMMAND ----------

simulate.arrival('once')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Simulate continuous new files
# MAGIC
# MAGIC - Will run up to 200 file parts
# MAGIC - Cancel the query to pause the simulated streaming

# COMMAND ----------

simulate.arrival('continuous')
