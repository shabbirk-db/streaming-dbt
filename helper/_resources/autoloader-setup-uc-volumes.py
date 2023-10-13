# Databricks notebook source
dbutils.widgets.text("input_volume", "<dbfs:/Volumes/your_example/pathhere>")
input_volume = dbutils.widgets.get("input_volume")

# COMMAND ----------

for stream in spark.streams.active:
    stream.stop()

# COMMAND ----------

import pyspark.sql.functions as F
import re

course = "sql_workshop"

username = spark.sql("SELECT current_user()").collect()[0][0]
userhome = f"dbfs:/Volumes/uc_shabbirkhanbhai/airlines_source"
database = f"""{course}_{re.sub("[^a-zA-Z0-9]", "_", username)}_db"""


print(f"""
username: {username}
userhome: {userhome}
database: {database}""")

dbutils.widgets.text("mode", "cleanup")
mode = dbutils.widgets.get("mode")

if mode == "initialise":
    print("Resetting workspace...")
    spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    dbutils.fs.rm(f'{userhome}/raw', True)
    
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
spark.sql(f"USE {database}")

fileIndex = 3
totalFiles = 200

print("Generating datasets...")

#Copy airlines data to user folder and modify to be streamable
header = (spark.read.option("inferSchema","true").option("header","true").csv("dbfs:/databricks-datasets/airlines/part-00000"))
body = (spark.read.option("inferSchema","true").csv("dbfs:/databricks-datasets/airlines/part-000{0[1-9],[1-4][0-9]}"))
union = header.union(body)

for c in union.columns:
  union = union.withColumn(c,F.when(F.col(c) == 'NA',None).otherwise(F.col(c)))
  
(
  union
     .withColumn('Year',F.col('Year')+28) #Updating to more recent dates for more interesting demo
     .repartition(totalFiles)
     .write
     .mode("overwrite")
     .format("json")
     .save(f"{userhome}/raw/")
)

print("Airline dataset ready!")

#Import some enrichment data from online sources

#Airport codes
try:
  url = "https://pkgstore.datahub.io/core/airport-codes/airport-codes_json/data/9ca22195b4c64a562a0a8be8d133e700/airport-codes_json.json"
  from pyspark import SparkFiles
  sc.addFile(url)
  path  = SparkFiles.get('airport-codes_json.json')
  dbutils.fs.cp(f'file://{path}',f'{userhome}/iata_data/airport_codes.json')
except Exception as e:
  print(f"Cannot access data at: {url}")
  
#Airline codes
try:
  url2 = "https://raw.githubusercontent.com/npow/airline-codes/master/airlines.json"
  sc.addFile(url2)
  path  = SparkFiles.get('airlines.json')
  dbutils.fs.cp(f'file://{path}',f'{userhome}/iata_data/airline_codes.json')
except Exception as e:
  print(f"Cannot access data at: {url2}")
  
print("Airport/Airline codes datasets ready!")

print("All datasets ready!")

# COMMAND ----------

class StreamSimulation:
    def __init__(self):
        self.source = f"{userhome}/raw"
        self.target = f"{userhome}/airlines"
        self.fileIndex = fileIndex
        self.totalFiles = totalFiles
            
    def arrival(self,mode='once'):
       
        if mode == 'once':
          source_file = dbutils.fs.ls(self.source)[self.fileIndex][0]
          target_location = f"{self.target}/part-{self.fileIndex - 3}"
          dbutils.fs.cp(source_file,target_location)
          print(f'New file saved as: {target_location}')
          self.fileIndex+=1
          
        elif mode == 'continuous':
          while self.fileIndex <= self.totalFiles:
            source_file = dbutils.fs.ls(self.source)[self.fileIndex][0]
            target_location = f"{self.target}/part-{self.fileIndex - 3}"
            dbutils.fs.cp(source_file,target_location)
            print(f'New file saved as: {target_location}')
            self.fileIndex+=1
          

# COMMAND ----------

simulate = StreamSimulation()

# COMMAND ----------

if mode == "cleanup":
    print("Deleting workspace...")
    spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    dbutils.fs.rm(userhome, True)
