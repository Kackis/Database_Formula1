# Databricks notebook source
# MAGIC %md 
# MAGIC ### Transform Circuits data
# MAGIC 1. Read "bronze" circuits table
# MAGIC 2. Keep ony columns for analitycs, drop column url
# MAGIC 3. Standarize column name using snake_case (circuitId -> circuit_id)
# MAGIC 4. Rename column (lat -> latitude, long -> longitude)
# MAGIC 5. Filter out rows where circuit_id is null (business key validation)
# MAGIC 6. Remove duplicate records
# MAGIC 7. Transform values of columns circuit_name and locality to Title Case
# MAGIC 8. Write transformed data to silver circuits table

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### 1. Read "bronze" circuits table

# COMMAND ----------

# MAGIC %run ../Workspace/common/configuration_environment

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.circuit"
silver_table = f"{catalog_name}.{silver_schema}.circuits"

# COMMAND ----------

circuits_df = spark.read.table(bronze_table)

# COMMAND ----------

circuits_df = spark.table(bronze_table)
display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Keep ony columns for analitycs, drop column url

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

circuits_selected_df = circuits_df.select(
    "circuitId",
    "circuitName",
    "lat",
    "long",
    "locality",
    "country",
    "ingestion_timestamp", 
    "source_file"
    )

# COMMAND ----------

circuits_selected_df = circuits_df.select(
    F.col("circuitId"),
    F.col("circuitName"),
    F.col("lat"),
    F.col("long"),
    F.col("locality"),
    F.col("country").alias("country_name") ,
    F.col("ingestion_timestamp"), 
    F.col("source_file")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Standarize column name using snake_case (circuitId -> circuit_id)
# MAGIC ##### 4. Rename column (lat -> latitude, long -> longitude)

# COMMAND ----------

circuits_renamed_df = (
    circuits_selected_df
    .withColumnRenamed("circuitId", "circuit_id")
    .withColumnRenamed("circuitName", "circuit_name")
    .withColumnRenamed("lat", "latitude")
    .withColumnRenamed("long", "longitude")
)

# COMMAND ----------

circuits_renamed_df = (
    circuits_selected_df
        .withColumnsRenamed({"circuitId": "circuit_id",
                             "circuitName": "circuit_name",
                             "lat": "latitude",
                             "long": "longitude"
                             })
)

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. Filter out rows where circuit_id is null (business key validation)

# COMMAND ----------

# !!! all where circuit is null
circuit_valid_df = circuits_renamed_df.filter(
    F.col("circuit_id").isNotNull()
)

display(circuit_valid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 6. Remove duplicate records

# COMMAND ----------

circuit_distinct_df = circuit_valid_df.distinct()

# COMMAND ----------

# 2nd method
circuit_distinct_df = circuit_valid_df.dropDuplicates(["circuit_id"])

# COMMAND ----------

display(circuit_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 7. Transform values of columns circuit_name and locality to Title Case

# COMMAND ----------

circuit_final_df = (
    circuit_distinct_df.withColumn('circuit_name', F.initcap(F.col('circuit_name')))
    .withColumn('locality', F.initcap(F.col('locality')))
)

# COMMAND ----------

display(circuit_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 8. Write transformed data to silver circuits table

# COMMAND ----------

(
    circuit_final_df
        .write
        .mode("overwrite")
        .format("delta")
        .saveAsTable(silver_table)    
)

# COMMAND ----------

display(
    spark.read.table(silver_table)
)