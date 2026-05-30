# Databricks notebook source
# MAGIC %md 
# MAGIC ### Build drivers dimension
# MAGIC 1. Read "silver" drivers table
# MAGIC 2. Read "gold" ref_nationality_region table
# MAGIC 3. Join data from drivers with ref_nationality_region using nationality
# MAGIC 4. Select required columns
# MAGIC     -   drivers.driver_id
# MAGIC     -   drivers.driver_name
# MAGIC     -   drivers.date_of_birth
# MAGIC     -   drivers.nationality
# MAGIC     -   ref_nationality_region.region
# MAGIC 7. Write transformed data to gold dim_drivers table

# COMMAND ----------

# MAGIC %run ../Workspace/common/configuration_environment

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_drivers"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Read source table
# MAGIC     a)   drivers
# MAGIC     b)   ref_nationality_region

# COMMAND ----------

drivers_df = spark.table(f"{catalog_name}.{silver_schema}.drivers")
ref_nationality_region_df = spark.table(f"{catalog_name}.{gold_schema}.ref_nationality_region")

# COMMAND ----------

display(drivers_df)
display(ref_nationality_region_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Join data from drivers with ref_nationality_region using nationality
# MAGIC 4. Select required columns
# MAGIC     -   drivers.driver_id
# MAGIC     -   drivers.driver_name
# MAGIC     -   drivers.date_of_birth
# MAGIC     -   drivers.nationality
# MAGIC     -   ref_nationality_region.region

# COMMAND ----------

# DBTITLE 1,Cell 9
dim_drivers_df = (
    drivers_df
    .join(
        ref_nationality_region_df,
        drivers_df.nationality == ref_nationality_region_df.nationality,
        'left'
    )
    .select(
        drivers_df.driver_id,
        drivers_df.driver_name,
        drivers_df.date_of_birth,
        drivers_df.nationality,
        ref_nationality_region_df.region.alias('nationality_region')
    )
)

# COMMAND ----------

display(dim_drivers_df)

# COMMAND ----------

(
    dim_drivers_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table)    
)

# COMMAND ----------

display (dim_drivers_df)