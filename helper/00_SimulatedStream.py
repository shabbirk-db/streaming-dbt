# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Instructions
# MAGIC
# MAGIC - Create a Volume in Unity Catalog first that you can access, to be used as a landing zone for all simulated stream data
# MAGIC - Enter the full Volume path in the widget of the format: `<dbfs:/Volumes/your_example/pathhere>`, for example: `dbfs:/Volumes/uc_shabbirkhanbhai/airlines_source`
# MAGIC - Initialise the stream (may take ~6 minutes)
# MAGIC - Simulate a stream of files, either through arrival('once') or 'continuous'
# MAGIC   - There are up to 200 files to stream, I recommend keeping it small at first, up to 10, and then potentially trying larger after the dbt project successfully runs

# COMMAND ----------

dbutils.widgets.text("input_volume", "<dbfs:/Volumes/your_example/pathhere>")
input_volume = dbutils.widgets.get("input_volume")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Initialise Folder structure

# COMMAND ----------

# MAGIC %run ./_resources/autoloader-setup-uc-volumes $mode = "initialise" $input_volume=$input_volume

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
