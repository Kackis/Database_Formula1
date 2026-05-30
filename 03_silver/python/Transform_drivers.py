# Databricks notebook source
# MAGIC %md 
# MAGIC ### Transform drivers data
# MAGIC 1. Read "bronze" drivers table
# MAGIC 2. Keep ony columns for analitycs, drop column url
# MAGIC 3. Standarize column name using snake_case (driverId -> driver_id, dateOfBirth -> date_of_birth)
# MAGIC 4. Concatenate name.givenName and name.familyName to create a new column called driver_name and transform the value to Title Case.
# MAGIC 5. Remove duplicate records
# MAGIC 7. Transform values of columns nationality to Title Case
# MAGIC 8. Write transformed data to silver drivers table

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### 1. Read "bronze" drivers table

# COMMAND ----------

# MAGIC %run ../Workspace/common/configuration_environment

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.drivers"
silver_table = f"{catalog_name}.{silver_schema}.drivers"

# COMMAND ----------

drivers_df = spark.read.table(bronze_table)

# COMMAND ----------

drivers_df = spark.table(bronze_table)
display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Keep ony columns for analitycs, drop column url

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

drivers_selected_df = drivers_df.select(
    "dateOfBirth",
    "driverId",
    "name",
    "nationality",
    "url",
    "ingestion_timestamp",
    "source_file"
)

display(drivers_selected_df)

# COMMAND ----------

drivers_dropped_df = drivers_selected_df.drop(F.col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Standarize column name using snake_case (driverId -> driver_id, dateOfBirth -> date_of_birth)

# COMMAND ----------

drivers_renamed_df = (
    drivers_dropped_df
        .withColumnsRenamed({"driverId": "driver_id",
                             "dateOfBirth": "date_of_birth" })
)

# COMMAND ----------

display(drivers_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Concatenate name.givenName and name.familyName to create a new column called driver_name and transform the value to Title Case.
# MAGIC ##### 6. Transform values of columns nationality to Title Case

# COMMAND ----------

# DBTITLE 1,Cell 15
drivers_concatenated_df = (
    drivers_renamed_df
        .withColumn("driver_name", F.concat_ws(" ", F.col("name.givenName"), F.col("name.familyName")))
)

display(drivers_concatenated_df)

# COMMAND ----------

drivers_distinct_df = (
    drivers_concatenated_df
    .withColumn('nationality', F.initcap(F.col('nationality')))
    .withColumn('driver_name', F.initcap(F.col('driver_name')))
)

display(drivers_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. Remove duplicate records

# COMMAND ----------

drivers_final_df = drivers_distinct_df.dropDuplicates(["driver_id"])
display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 7. Write transformed data to silver circuits table

# COMMAND ----------

(
    drivers_final_df
        .write
        .mode("overwrite")
        .format("delta")
        .saveAsTable(silver_table)    
)

# COMMAND ----------

display(
    spark.read.table(silver_table)
)