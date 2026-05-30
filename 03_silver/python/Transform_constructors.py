# Databricks notebook source
# MAGIC %md 
# MAGIC ### Transform Constructors data
# MAGIC 1. Read "bronze" constructors table
# MAGIC 2. Keep ony columns for analitycs, drop column url
# MAGIC 3. Standarize column name using snake_case (constructorId -> constructor_id)
# MAGIC 4. Rename column (name -> constructor_name)
# MAGIC 5. Remove duplicate records
# MAGIC 6. Transform values of columns nationality to Title Case
# MAGIC 7. Write transformed data to silver constructors table

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### 1. Read "bronze" constructors table

# COMMAND ----------

# MAGIC %run ../Workspace/common/configuration_environment

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.constructors"
silver_table = f"{catalog_name}.{silver_schema}.constructors"

# COMMAND ----------

constructors_df = spark.read.table(bronze_table)

# COMMAND ----------

constructors_df = spark.table(bronze_table)
display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Keep ony columns for analitycs, drop column url

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

constructors_selected_df = constructors_df.select(
    "constructorId",
    "name",
    "nationality",
    "url",
    "ingestion_timestamp", 
    "source_file"
    )

# COMMAND ----------

constructors_dropped_df = constructors_selected_df.drop("url")
display(constructors_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Standarize column name using snake_case (constructorId -> constructor_id)
# MAGIC ##### 4. Rename column (name -> constructor_name)

# COMMAND ----------

constructors_renamed_df = (
    constructors_dropped_df
    .withColumnRenamed("constructorId", "constructor_id")
    .withColumnRenamed("name", "constructor_name")
)

# COMMAND ----------

display(constructors_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. Remove duplicate records

# COMMAND ----------

# 2nd method
constructors_distinct_df  = constructors_renamed_df.dropDuplicates(["constructor_id"]) 
# constructor_id is main record in the table and it is unique
display(constructors_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 6. Transform values of columns nationality to Title Case

# COMMAND ----------

constructors_final_df = (
    constructors_distinct_df.withColumn('nationality', F.initcap(F.col('nationality')))
)

display (constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 8. Write transformed data to silver circuits table

# COMMAND ----------

(
    constructors_final_df
        .write
        .mode("overwrite")
        .format("delta")
        .saveAsTable(silver_table)    
)

# COMMAND ----------

display(
    spark.read.table(silver_table)
)