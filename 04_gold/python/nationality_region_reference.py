# Databricks notebook source
# MAGIC %md 
# MAGIC ### Build nationality region reference
# MAGIC 1. Create dataframe with list of nationalities and corresponding geographic regions
# MAGIC 2. Write dataframe to gold ref_nationality_region table

# COMMAND ----------

# MAGIC %run ../Workspace/common/configuration_environment

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.ref_nationality_region"

# COMMAND ----------

from pyspark.sql import Row

#Created nationality -> region mapping

nationality_regions = [
    
    #Europe
    Row(nationality = 'Austrian', region = 'Europe'),
    Row(nationality = 'Belgian', region = 'Europe'),
    Row(nationality = 'British', region = 'Europe'),
    Row(nationality = 'Canadian', region = 'Europe'),
    Row(nationality = 'Danish', region = 'Europe'),
    Row(nationality = 'Dutch', region = 'Europe'),
    Row(nationality = 'Finnish', region = 'Europe'),
    Row(nationality = 'French', region = 'Europe'),
    Row(nationality = 'German', region = 'Europe'),
    Row(nationality = 'Italian', region = 'Europe'),
    Row(nationality = 'Norwegian', region = 'Europe'),
    Row(nationality = 'Polish', region = 'Europe'),
    Row(nationality = 'Russian', region = 'Europe'),
    Row(nationality = 'Scandinavian', region = 'Europe'),
    Row(nationality = 'Swedish', region = 'Europe'),
    Row(nationality = 'Switzerland', region = 'Europe'),
    Row(nationality = 'Spanish', region = 'Europe'),
    Row(nationality = 'Portuguese', region = 'Europe'),
    Row(nationality = 'Hungarian', region = 'Europe'),
    Row(nationality = 'Czech', region = 'Europe'),
    Row(nationality = 'Slovak', region = 'Europe'),
    Row(nationality = 'Slovenian', region = 'Europe'),
    Row(nationality = 'Croatian', region = 'Europe'),
    Row(nationality = 'Serbian', region = 'Europe'),
    Row(nationality = 'Albanian', region = 'Europe'),
    Row(nationality = 'Bosnian', region = 'Europe'),
    Row(nationality = 'Macedonian', region = 'Europe'),
    Row(nationality = 'Bulgarian', region = 'Europe'),
    Row(nationality = 'Romanian', region = 'Europe'),   
    Row(nationality = 'Greek', region = 'Europe'),
    Row(nationality = 'Irish', region = 'Europe'),
    Row(nationality = 'Monakish', region = 'Europe'),
    Row(nationality = 'East German', region = 'Europe'),
    Row(nationality = 'Swiss', region = 'Europe'),

    #North America
    Row(nationality = 'American', region = 'North America'),
    Row(nationality = 'Mexican', region = 'North America'),
    Row(nationality = 'Canadian', region = 'North America'),
    Row(nationality = 'Puerto Rican', region = 'North America'),
    Row(nationality = 'Haitian', region = 'North America'),
    Row(nationality = 'Jamaican', region = 'North America'),
    Row(nationality = 'Puerto Rican', region = 'North America'),
    
    #Asia
    Row(nationality = 'Chinese', region = 'Asia'),
    Row(nationality = 'Japanese', region = 'Asia'),
    Row(nationality = 'Korean', region = 'Asia'),
    Row(nationality = 'Indian', region = 'Asia'),
    Row(nationality = 'Indonesian', region = 'Asia'),
    Row(nationality = 'Malaysian', region = 'Asia'),
    Row(nationality = 'Singaporean', region = 'Asia'),
    Row(nationality = 'Thai', region = 'Asia'),
    Row(nationality = 'Vietnamese', region = 'Asia'),
    Row(nationality = 'Cambodian', region = 'Asia'),
    Row(nationality = 'Japanese', region = 'Asia'),
    Row(nationality = 'Hong Kong', region = 'Asia'),

    #Oceania
    Row(nationality = 'Australian', region = 'Oceania'),
    Row(nationality = 'New Zealander', region = 'Oceania'),
    Row(nationality = 'Fiji', region = 'Oceania'),

    #South America
    Row(nationality = 'Brazilian', region = 'South America'),
    Row(nationality = 'Argentinian', region = 'South America'),
    Row(nationality = 'Chilean', region = 'South America'),
    Row(nationality = 'Colombian', region = 'South America'),
    Row(nationality = 'Peruvian', region = 'South America'),
    Row(nationality = 'Uruguayan', region = 'South America'),
    Row(nationality = 'Venezuelan', region = 'South America'),
    Row(nationality = 'Argentine', region = 'South America'),

    #Africa
    Row(nationality = 'South African', region = 'Africa'),
    Row(nationality = 'Egyptian', region = 'Africa'),
    Row(nationality = 'Kenyan', region = 'Africa'),
    Row(nationality = 'Moroccan', region = 'Africa'),
    Row(nationality = 'Nigerian', region = 'Africa'),
    Row(nationality = 'Tunisian', region = 'Africa'),
    Row(nationality = 'Zambian', region = 'Africa'),
    Row(nationality = 'Zimbabwean', region = 'Africa'),
    Row(nationality = 'Algerian', region = 'Africa'),
    Row(nationality = 'South African', region = 'Africa'),
    Row(nationality = 'Rhodesian', region = 'Africa')

]

ref_nationality_region_df = spark.createDataFrame(nationality_regions)


# COMMAND ----------

(
    ref_nationality_region_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table)    
)

display(spark.table(target_table))