# Databricks notebook source
# MAGIC %md 
# MAGIC ### Build results
# MAGIC 1. Read "silver" results table
# MAGIC 2. Read "silver" sprint table
# MAGIC 3. Add columns session_type with values RACE or SPRINT
# MAGIC 4. Derive additional columns
# MAGIC     -   is_win -> driver who own the race
# MAGIC     -   is_podium -> drivers scored podiumm result
# MAGIC     -   has_points -> driver scored points
# MAGIC 5. Write transformed data to gold fact_results table

# COMMAND ----------

# MAGIC %run ../Workspace/common/configuration_environment

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.fact_results"

# COMMAND ----------

# checking content of tables
sprint_df = spark.table(f"{catalog_name}.{silver_schema}.sprints")
results_df = spark.table(f"{catalog_name}.{silver_schema}.results")
display(sprint_df)
display(results_df)

# COMMAND ----------

results_df = (
    spark.table(f"{catalog_name}.{silver_schema}.results")
        .withColumn("session_type", F.lit("RACE"))
        .drop("race_name", "race_date", "ingestion_timestamp", "source_file")     
)

# COMMAND ----------

sprint_df = (
    spark.table(f"{catalog_name}.{silver_schema}.sprints")
    .withColumn("session_type", F.lit("SPRINT"))
    .drop("race_name", "race_date", "ingestion_timestamp", "source_file")    
)

# COMMAND ----------

# DBTITLE 1,Cell 7
#union results and sprints
results_sprint_df = (
    results_df.unionByName(sprint_df, allowMissingColumns=True) 
    #There are some columns in sprints that are not in results, in other wersion of code throws errors
)

display(results_sprint_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Derive additional columns
# MAGIC     -   is_win -> driver who own the race
# MAGIC     -   is_podium -> drivers scored podiumm result
# MAGIC     -   has_points -> driver scored points

# COMMAND ----------

session_results_df = (
    results_sprint_df
        .withColumn("is_win", F.col("Finish_position") == 1)
        .withColumn("is_podium", F.col("Finish_position").between(1,3))
        .withColumn("has_points", F.col("points") > 0)    
)

display (session_results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. Write transformed data to gold fact_results table

# COMMAND ----------

(
    session_results_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table)    
)

# COMMAND ----------

display(spark.table(target_table))