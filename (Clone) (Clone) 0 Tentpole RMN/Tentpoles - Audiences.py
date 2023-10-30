# Databricks notebook source
import pyspark.sql.functions as F
import pyspark.sql.types as T
import datetime as dt
spark.conf.set("spark.databricks.delta.formatCheck.enabled","false")
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
client='ulvr'
brand_id_list=[1,99]
current_date="2023-10-11"
year_filter="2022-10-11"
category_list=['Beauty and Wellness','Nutrition and Ice Cream','Personal Care']
print(year_filter,current_date)

# COMMAND ----------

df=spark.read.table(f"{client}_prod_public_works.lm_tentpoles_rmn_growth_v2")
aud=df.fillna(0)
aud=aud.withColumn('growth1_1',((aud.event_avg_spend_per_order-aud.overall_excl_event_avg_spend_per_order)/(aud.overall_avg_spend_per_order))).withColumn('growth2_1',((aud.event_avg_spend_per_order-aud.overall_avg_spend_per_order)/(aud.overall_avg_spend_per_order)))

# COMMAND ----------

cat_overall=aud.select('category','individual_identity_key','overall_spend','overall_frequency').distinct()

# COMMAND ----------

# DBTITLE 1,Event shoppers (excluding event only shoppers)
aud1=aud.filter(aud.overall_excl_event_flag!=0)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Increased Spenders

# COMMAND ----------

increased_spenders_1=aud1.filter(aud.growth1>=0.15).select(F.lit("INCREASED SPENDERS").alias('audience_name'),'event_name','category','individual_identity_key')
increased_spenders_2=aud.filter(aud.overall_excl_event_flag==0).select(F.lit("INCREASED SPENDERS").alias('audience_name'),'event_name','category','individual_identity_key')
increased_spenders=increased_spenders_1.union(increased_spenders_2)
increased_spenders=increased_spenders.distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Non-Increased Spenders

# COMMAND ----------

non_increased_spenders1=aud1.filter(aud.growth1<0.15).select(F.lit("NON INCREASED SPENDERS").alias('audience_name'),'event_name','category','individual_identity_key')
non_increased_spenders=non_increased_spenders1.distinct()

# COMMAND ----------

event_list=["SUPER BOWL","MARCH MADNESS","EASTER","MOTHER'S DAY","MEMORIAL DAY","FATHER'S DAY"]
aud_list=increased_spenders.union(non_increased_spenders)
aud_list=aud_list.filter(aud_list.event_name.isin(event_list))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Audience List

# COMMAND ----------

display(aud_list.groupBy('audience_name','event_name','category').count())

# COMMAND ----------

import datetime as dt
update_ver=dt.datetime.today().strftime("%Y%m%d")
print(update_ver)

# COMMAND ----------

# tentpoles_rmn_event_name_category_[increased_spenders/non_increased_spenders]_yyyymmdd
aud_list=aud_list.withColumn("table_name",F.concat_ws('_',F.lit("tentpoles_rmn"),F.lower(aud_list.event_name),F.lower(aud_list.category),F.lower(aud_list.audience_name),F.lit(f"{update_ver}")))
aud_list=aud_list.withColumn("final_table_name",F.regexp_replace( F.regexp_replace( "table_name", r"[\s-]", "_" ),  r"\W", "_"))
table_name_list=aud_list.select("final_table_name").distinct().collect()
for table in table_name_list:
  table=table[0]
  table_write=aud_list.filter(aud_list.final_table_name==table).select("individual_identity_key").distinct()
  # print(table,table_write.count())
  table_name=f'{client}_prod_audience_xfer.'+table
  print(table_name,table_write.count())
  table_write.write.format('parquet').option("header","true").mode("overwrite").saveAsTable(f"{table_name}")

# COMMAND ----------


