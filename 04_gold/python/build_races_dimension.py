# Databricks notebook source
# MAGIC %md 
# MAGIC ### Build races dimension
# MAGIC 1. Read "silver" races table
# MAGIC 2. Read "silver" circuit table
# MAGIC 3. Join data from races with circuits using circuits_id
# MAGIC 4. Select required columns
# MAGIC     -   races.season
# MAGIC     -   races.round
# MAGIC     -   races.race_name
# MAGIC     -   races.race_date
# MAGIC     -   circuits.circuit_name
# MAGIC     -   circuits.locality
# MAGIC     -   circuits.country
# MAGIC 7. Write transformed data to gold dim_races table

# COMMAND ----------

# MAGIC %run ../Workspace/common/configuration_environment

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_races"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Read source table
# MAGIC     a)   races
# MAGIC     b)   circuit
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 6
circuits_df = spark.table(f"{catalog_name}.{silver_schema}.circuits")
races_df = spark.table(f"{catalog_name}.{silver_schema}.races")


# COMMAND ----------

display(circuits_df)
display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Join data from races with circuits using circuits_id
# MAGIC Select required columns
# MAGIC 1. races.season
# MAGIC 2. races.round
# MAGIC 3. races.race_name
# MAGIC 4. races.race_date
# MAGIC 5. circuits.circuit_name
# MAGIC 6. circuits.locality
# MAGIC 7. circuits.country

# COMMAND ----------

dim_races_df = (
    races_df
        .join(circuits_df, 
              races_df.circuit_id == circuits_df.circuit_id, "inner")
        .select(
            races_df.season,
            races_df.round,
            races_df.race_name,
            races_df.date,
            circuits_df.circuit_name,
            circuits_df.locality,
            circuits_df.country_name
        )
)

display(dim_races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 7. Write transformed data to gold dim_races table

# COMMAND ----------

(
    dim_races_df
        .write
        .format("delta")
        .saveAsTable(target_table)    
)

#table have been created, can't overwrite it

# COMMAND ----------

display(spark.table(target_table))