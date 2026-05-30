# Databricks notebook source
# MAGIC %md 
# MAGIC ### Build races dimension
# MAGIC 1. Read "silver" constructors table
# MAGIC 2. Read "gold" ref_nationality_table table
# MAGIC 3. Join data from "constructors" with "ref_nationality_region" using "nationality"
# MAGIC 4. Select required columns
# MAGIC     -   constructors.constructor_id
# MAGIC     -   constructors.constructors_name
# MAGIC     -   constructors.nationality
# MAGIC     -   ref_nationality_region.region
# MAGIC 7. Write transformed data to gold dim_constructors table

# COMMAND ----------

# MAGIC %run ../Workspace/common/configuration_environment

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_constructors"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read source tables
# MAGIC Read "silver" constructors table, Read "gold" ref_nationality_table table

# COMMAND ----------

constructors_df = spark.table(f"{catalog_name}.{silver_schema}.constructors")
ref_nationality_region_df = spark.table(f"{catalog_name}.{gold_schema}.ref_nationality_region")


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Join data from "constructors" with "ref_nationality_region" using "nationality"
# MAGIC     Select required columns
# MAGIC     -   constructors.constructor_id
# MAGIC     -   constructors.constructor_name
# MAGIC     -   constructors.nationality
# MAGIC     -   ref_nationality_region.region

# COMMAND ----------

dim_constructors_df = (
    constructors_df
    .join(
            ref_nationality_region_df,
            constructors_df.nationality == ref_nationality_region_df.nationality,
            "left"
        )
    .select(
        constructors_df.constructor_id,
        constructors_df.constructor_name,
        constructors_df.nationality,
        ref_nationality_region_df.region
    )
    
)

display(dim_constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write transformed data to gold dim_constructors table

# COMMAND ----------

(
    dim_constructors_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table)    
)

display(spark.table(target_table))