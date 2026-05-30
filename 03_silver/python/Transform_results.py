# Databricks notebook source
# MAGIC %md 
# MAGIC ### Transform results data
# MAGIC 1. Read "bronze" results table
# MAGIC 2. Keep ony columns for analitycs, drop column url
# MAGIC 3. Standarize column name using snake_case (constructorId -> constructor_id, driverId -> driver_id, raceId -> race_id, positionText -> finish_position_text)
# MAGIC 4. Rename column (date -> race_date, grid -> grid_position, laps -> completed_laps, number -> car_number, position -> finish position)
# MAGIC 5. Filter out rows where season, round, constructor_id, driver_id is null (business key validation)
# MAGIC 6. Remove duplicate records
# MAGIC 7. Transform values of columns race_name and locality to Title Case
# MAGIC 8. Write transformed data to silver results table

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### 1. Read "bronze" results table

# COMMAND ----------

# MAGIC %run ../Workspace/common/configuration_environment

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.results"
silver_table = f"{catalog_name}.{silver_schema}.results"

# COMMAND ----------

results_df = spark.read.table(bronze_table)

# COMMAND ----------

results_df = spark.table(bronze_table)
display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Keep ony columns for analitycs, drop column url

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

results_selected_df = results_df.select(
    "constructorId",
    "date",
    "driverId",
    "grid",
    "laps",
    "number",
    "points",
    "position",
    "positionText",
    "raceName",
    "round",
    "season",
    "status",
    "ingestion_timestamp",
    "source_file"
    )

# display(results_selected_df)
# memory transfer limit exceeded

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Standarize column name using snake_case (constructorId -> constructor_id, driverId -> driver_id, raceId -> race_id, positionText -> finish_position_text)
# MAGIC ##### 4. Rename column (date -> race_date, grid -> grid_position, laps -> completed_laps, number -> car_number, position -> finish position)

# COMMAND ----------

results_renamed_df = (
    results_selected_df
    .withColumnRenamed("constructorId", "constructor_id")
    .withColumnRenamed("driverId", "driver_id")
    .withColumnRenamed("date", "race_date")
    .withColumnRenamed("grid", "grid_position")
    .withColumnRenamed("laps", "completed_laps")
    .withColumnRenamed("number", "car_number")
    .withColumnRenamed("position", "finish_position")
    .withColumnRenamed("positionText", "finish_position_text")                  
    .withColumnRenamed("raceName", "race_name")
)

display(results_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. Filter out rows where season, round, constructor_id, driver_id is null (business key validation)
# MAGIC ##### 6. Remove duplicate records

# COMMAND ----------

# DBTITLE 1,Cell 13
# !!! all where circuit is null
results_valid_df = (
    results_renamed_df
    .filter(
        F.col("constructor_id").isNotNull() &
        F.col("season").isNotNull() &
        F.col("driver_id").isNotNull() &
        F.col("round").isNotNull()
    )
    .dropDuplicates(["season", "round", "driver_id", "constructor_id"])
)

# display(results_valid_df)
# memory transfer limit exceeded

# COMMAND ----------

display(results_df.count() - results_valid_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 7. Transform values of columns race_name and locality to Title Case
# MAGIC ##### 8. Write transformed data to silver results table

# COMMAND ----------

results_final_df = results_valid_df.withColumn('race_name', F.initcap(F.col('race_name')))

# display(results_final_df)
# memory transfer limit exceeded

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 8. Write transformed data to silver results table

# COMMAND ----------

(
    results_final_df
        .write
        .mode("overwrite")
        .format("delta")
        .saveAsTable(silver_table)    
)

# COMMAND ----------

display(
    spark.read.table(silver_table)
)