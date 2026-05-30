# Databricks notebook source
# MAGIC %md 
# MAGIC ### Transform Circuits data
# MAGIC 1. Read "bronze" races table
# MAGIC 2. Keep ony columns for analitycs, drop column url
# MAGIC 3. Standarize column name using snake_case (circuitId -> circuit_id, raceName -> race_name)
# MAGIC 4. Rename column (date -> race_date)
# MAGIC 5. Remove duplicate records
# MAGIC 6. Transform values of columns race_name and locality to Title Case
# MAGIC 7. Write transformed data to silver races table

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### 1. Read "bronze" circuits table

# COMMAND ----------

# MAGIC %run ../Workspace/common/configuration_environment

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.races"
silver_table = f"{catalog_name}.{silver_schema}.races"

# COMMAND ----------

races_df = spark.read.table(bronze_table)

# COMMAND ----------

races_df = spark.table(bronze_table)
display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Keep ony columns for analitycs, drop column url

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

races_selected_df = races_df.select(
    "season",
    "round",
    "url",
    "raceName",
    "date",
    "circuitId",
    "ingestion_timestamp", 
    "source_file"
    )

# COMMAND ----------

races_selected_df = races_df.select(
    F.col("season"),
    F.col("round"),
    F.col("url"),
    F.col("raceName"),
    F.col("date"),
    F.col("circuitId"),
    F.col("ingestion_timestamp"), 
    F.col("source_file"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Standarize column name using snake_case (circuitId -> circuit_id)
# MAGIC ##### 4. Rename column (lat -> latitude, long -> longitude)

# COMMAND ----------

races_renamed_df = (
    races_selected_df
    .withColumnRenamed("raceName", "race_name")
    .withColumnRenamed("circuitId", "circuit_id")
)

# COMMAND ----------

# DBTITLE 1,Cell 13
races_renamed_df = (
    races_selected_df
        .withColumnsRenamed({"raceName": "race_name",
                             "circuitId": "circuit_id"
                             })
)

# COMMAND ----------

display(races_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. Remove duplicate records

# COMMAND ----------

races_distinct_df = races_renamed_df.distinct()

# COMMAND ----------

# DBTITLE 1,Cell 17
# 2nd method
races_distinct_df = races_renamed_df.dropDuplicates(["season", "round"])

# COMMAND ----------

display(races_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 7. Transform values of columns circuit_name and locality to Title Case

# COMMAND ----------

races_final_df = (
    races_distinct_df
    .withColumn('race_name', F.initcap(F.col('race_name')))
)

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 8. Write transformed data to silver circuits table

# COMMAND ----------

(
    races_final_df
        .write
        .mode("overwrite")
        .format("delta")
        .saveAsTable(silver_table)    
)

# COMMAND ----------

display(
    spark.read.table(silver_table)
)