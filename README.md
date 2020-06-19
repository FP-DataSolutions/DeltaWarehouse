# DeltaWarehouse (Beta)
Spark ETL Framework based on Delta  

Spark ETL framework is a tool to automate ETL processes.
Current version requires Spark cluster on Databricks.

## Architecture

Spark ETL framework supports the building of a data warehouse on Spark with SCD Type2.
It consists of:

- a library installed on the spark cluster

- data sources configuration file

- etl configuration file

  

The framework based on data source configuration file and etl configuration file creates data warehouse on spark and sends changes into sql databases.
The following diagram presents general etl concept.

![Archiecture](.\docs\imgs\architecture.png)



## How to run

### Installation

- Install Azure Databricks and create cluster
- Install **etl_sparkdw_tools-*.*.*_ds-py3.7.egg** library
- Mount data lake storage
- Create Azure Sql Database
  - Create schema: **stage**

### Configuration

- Prepare datasources configuration file
- Prapare views for dims and facts
- Prapre etlconfig file
- Copy configurations files (and views) to data lake storage
- Install **etl_sparkdw_tools-*.*.*_ds-py3.7.egg** library

See 
[Configurations]: ./docs/Configurations.md

### Running

- Create databricks notebook and run it 

```python
from etl.etl_process import ETLProcess
from utils.config import Config
def run_ddls(ddl_scripts):
  print("Running ddl scripts...")
  files = dbutils.fs.ls(ddl_scripts)
  for file in files:
    print(file.name)
    if file.name.endswith(".sql"):
      ddl= spark.read.text(file.path,wholetext=True).rdd.collect()[0]['value']
      spark.sql(ddl)
  print("All ddl scrips have been successfully executed...")
  
#Set configuration
Config.IsSparkDataBrick=True
Config.DATABASE_HOSTNAME = 'deltasql'
Config.DATABASE_PORT = 1433
Config.DATABASE_NAME = 'PocDW'
Config.DATABASE_USERNAME = 'user'
Config.DATABASE_PASSWORD = 'password'
Config.DATABASE_ENCRYPT = 'true'

base_path="/mnt/datalake/DW/Demo/"
ddl_scripts_dims=base_path+"SqlViews/Dims"
ddl_scripts_facts=base_path+"SqlViews/Facts"
data_source_configurations="datasourcesconfig.csv"
etl_configurations="config.csv"
dw_path="PocDW"
dw_name="PocDW"
etl_proc = ETLProcess(base_path,
                      data_source_configurations,
                      etl_configurations,
                      dw_path,
                      dw_name)
etl_proc.init()
#etl_proc.get_dw().drop_all_tables()
etl_proc.register_data_sources()
run_ddls(ddl_scripts_dims)
etl_proc.run_dimensions()
#run_ddls(ddl_scripts_facts)
#etl_proc.run_facts()
```



## Examples

[Demo]: ./docs/DemoPeople.md

