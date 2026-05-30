# Databricks notebook source


# COMMAND ----------

# MAGIC %md 
# MAGIC ### Transform Sprints data
# MAGIC 1. Read "bronze" sprints table
# MAGIC 2. Keep ony columns for analitycs, drop column url
# MAGIC 3. Standarize column name using snake_case (constructorId -> constructor_id, driverid -> driver_id, raceName -> race_name, positionText -> finish_position_text)
# MAGIC 4. Rename column (date -> race_date, grid -> grid_position, laps -> completed_laps, number -> car_number, position => finish_position)
# MAGIC 5. Filter out rows where season, round, constructor_id, driver_id is null (business key validation)
# MAGIC 6. Remove duplicate records
# MAGIC 7. Transform values of columns race_name to Title Case
# MAGIC 8. Write transformed data to silver sprints table

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### 1. Read "bronze" circuits table

# COMMAND ----------

# MAGIC %run ../Workspace/common/configuration_environment

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.sprints"
silver_table = f"{catalog_name}.{silver_schema}.sprints"

# COMMAND ----------

sprints_df = spark.read.table(bronze_table)

# COMMAND ----------

sprints_df = spark.table(bronze_table)
display(sprints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Keep ony columns for analitycs, drop column url
# MAGIC ##### 3. Standarize column name using snake_case (constructorId -> constructor_id, driverid -> driver_id, raceName -> race_name, positionText -> finish_position_text)
# MAGIC ##### 4. Rename column (date -> race_date, grid -> grid_position, laps -> completed_laps, number -> car_number, position -> finish_position)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

sprints_selected_df = ( 
    spark.table(bronze_table)
    .select(
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
        "ingestion_timestamp", 
        "source_file"
    )
    .withColumnsRenamed({
        "constructorId": "constructor_id",
        "date": "race_date",
        "driverId": "driver_id",
        "grid": "grid_position",
        "raceName": "race_name",
        "positionText": "finish_position_text",
        "laps": "completed_laps",
        "number": "car_number",
        "position": "finish_position",
        "grid": "grid_position",
    })
)

display(sprints_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. Filter out rows where season, round, constructor_id, driver_id is null (business key validation)
# MAGIC ##### 6. Remove duplicate records

# COMMAND ----------

sprints_valid_df = (
    sprints_selected_df.filter(
        F.col("season").isNotNull() &
        F.col("round").isNotNull() &
        F.col("constructor_id").isNotNull() &
        F.col("driver_id").isNotNull()
        )
        .dropDuplicates(["season", "round", "constructor_id", "driver_id"])
)

display(sprints_df.count() - sprints_valid_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 7. Transform values of columns race_name to Title Case

# COMMAND ----------

sprints_final_df = sprints_valid_df.withColumn('race_name', F.initcap(F.col('race_name')))

display(sprints_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 8. Write transformed data to silver circuits table

# COMMAND ----------

(
    sprints_final_df
        .write
        .mode("overwrite")
        .format("delta")
        .saveAsTable(silver_table)    
)

# COMMAND ----------

display(
    spark.read.table(silver_table)
)