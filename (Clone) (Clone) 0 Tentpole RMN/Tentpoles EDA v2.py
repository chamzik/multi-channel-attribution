# Databricks notebook source
import pyspark.sql.functions as F
import pyspark.sql.types as T
import datetime as dt
spark.conf.set("spark.databricks.delta.formatCheck.enabled","false")
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

# MAGIC %md
# MAGIC ##### Metrics - last 52 weeks

# COMMAND ----------

cat_overall=aud.select('category','individual_identity_key','overall_spend','overall_frequency').distinct()

# COMMAND ----------

print(aud.count(),cat_overall.count())

# COMMAND ----------

display(cat_overall.groupBy("category").agg(F.countDistinct("individual_identity_key").alias("Total_shoppers"),F.sum("overall_spend").alias("Total_spend"),F.sum("overall_frequency").alias("Total_frequency"),(F.sum("overall_spend")/F.countDistinct("individual_identity_key")).alias("Avg. spend"),(F.sum("overall_frequency")/F.countDistinct("individual_identity_key")).alias("Avg. frequency"),F.sum("overall_frequency").alias("Total_frequency")))

# COMMAND ----------

display(aud.groupBy('event_name','category','event_start_date','event_end_date','date_filter').agg(F.countDistinct("individual_identity_key").alias("Total_shoppers"),F.sum("overall_spend").alias("Total_spend"),F.sum("overall_frequency").alias("Total_frequency"),(F.sum("overall_spend")/F.countDistinct("individual_identity_key")).alias("Avg. spend"),(F.sum("overall_frequency")/F.countDistinct("individual_identity_key")).alias("Avg. frequency")))

# COMMAND ----------

cat_overall=aud.select('category','individual_identity_key','event_spend','event_frequency')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Metrics - Event Period

# COMMAND ----------

display(cat_overall.groupBy("category").agg(F.countDistinct("individual_identity_key").alias("Total_shoppers"),F.sum("event_spend").alias("Total_spend"),F.sum("event_frequency").alias("Total_frequency"),(F.sum("event_spend")/F.countDistinct("individual_identity_key")).alias("Avg. spend"),(F.sum("event_frequency")/F.countDistinct("individual_identity_key")).alias("Avg. frequency")))

# COMMAND ----------

display(aud.groupBy('event_name','category','event_start_date','event_end_date','date_filter').agg(F.countDistinct("individual_identity_key").alias("Total_shoppers"),F.sum("event_spend").alias("Total_spend"),F.sum("event_frequency").alias("Total_frequency"),(F.sum("event_spend")/F.countDistinct("individual_identity_key")).alias("Avg. spend"),(F.sum("event_frequency")/F.countDistinct("individual_identity_key")).alias("Avg. frequency")))

# COMMAND ----------

aud1=aud.filter(aud.overall_excl_event_flag!=0)

# COMMAND ----------

display(aud1.groupBy('event_name','category','event_start_date','event_end_date','date_filter').agg(
  F.expr('percentile(growth2, array(0.25))')[0].alias('25th'),
  F.expr('percentile(growth2, array(0.50))')[0].alias('50th'),
  F.expr('percentile(growth2, array(0.75))')[0].alias('75th'),
  F.expr('percentile(growth2, array(0.90))')[0].alias('90th'),
  F.expr('percentile(growth2, array(0.95))')[0].alias('95th'),
  F.expr('percentile(growth2, array(0.99))')[0].alias('99th')))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Increased Spenders

# COMMAND ----------

increased_spenders_1=aud1.filter(aud.growth1>=0.15).select(F.lit("INCREASED SPENDERS").alias('audience_name'),'event_name','category','event_start_date','event_end_date','date_filter','individual_identity_key','overall_spend','overall_frequency','growth2','event_spend','event_frequency')
increased_spenders_2=aud.filter(aud.overall_excl_event_flag==0).select(F.lit("INCREASED SPENDERS").alias('audience_name'),'event_name','category','event_start_date','event_end_date','date_filter','individual_identity_key','overall_spend','overall_frequency','growth2','event_spend','event_frequency')
increased_spenders=increased_spenders_1.union(increased_spenders_2)
increased_spenders=increased_spenders.distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### QC

# COMMAND ----------

print(increased_spenders.count(),increased_spenders_1.count(),increased_spenders_2.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Metrics - last 52 weeks

# COMMAND ----------

display(increased_spenders.groupBy('audience_name','event_name','category','event_start_date','event_end_date','date_filter').agg(F.countDistinct("individual_identity_key").alias("Total_shoppers"),F.sum("overall_spend").alias("Total_spend"),F.sum("overall_frequency").alias("Total_frequency"),(F.sum("overall_spend")/F.countDistinct("individual_identity_key")).alias("Avg. spend"),(F.sum("overall_frequency")/F.countDistinct("individual_identity_key")).alias("Avg. frequency")))

# COMMAND ----------

display(increased_spenders.groupBy('audience_name','event_name','category','event_start_date','event_end_date','date_filter').agg(
  F.expr('percentile(growth2, array(0.25))')[0].alias('25th'),
  F.expr('percentile(growth2, array(0.50))')[0].alias('50th'),
  F.expr('percentile(growth2, array(0.75))')[0].alias('75th'),
  F.expr('percentile(growth2, array(0.90))')[0].alias('90th'),
  F.expr('percentile(growth2, array(0.95))')[0].alias('95th'),
  F.expr('percentile(growth2, array(0.99))')[0].alias('99th')))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Metrics - Event period

# COMMAND ----------

display(increased_spenders.groupBy('audience_name','event_name','category','event_start_date','event_end_date','date_filter').agg(F.countDistinct("individual_identity_key").alias("Total_shoppers"),F.sum("event_spend").alias("Total_spend"),F.sum("event_frequency").alias("Total_frequency"),(F.sum("event_spend")/F.countDistinct("individual_identity_key")).alias("Avg. spend"),(F.sum("event_frequency")/F.countDistinct("individual_identity_key")).alias("Avg. frequency")))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Non-Increased Spenders

# COMMAND ----------

non_increased_spenders1=aud1.filter(aud.growth1<0.15).select(F.lit("NON INCREASED SPENDERS").alias('audience_name'),'event_name','category','event_start_date','event_end_date','date_filter','individual_identity_key','overall_spend','overall_frequency','growth2','event_spend','event_frequency')
non_increased_spenders=non_increased_spenders1.distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### QC

# COMMAND ----------

print(non_increased_spenders.count(),non_increased_spenders1.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Metrics - last 52 weeks

# COMMAND ----------

display(non_increased_spenders.groupBy('audience_name','event_name','category','event_start_date','event_end_date','date_filter').agg(F.countDistinct("individual_identity_key").alias("Total_shoppers"),F.sum("overall_spend").alias("Total_spend"),F.sum("overall_frequency").alias("Total_frequency"),(F.sum("overall_spend")/F.countDistinct("individual_identity_key")).alias("Avg. spend"),(F.sum("overall_frequency")/F.countDistinct("individual_identity_key")).alias("Avg. frequency")))

# COMMAND ----------

display(non_increased_spenders.groupBy('audience_name','event_name','category','event_start_date','event_end_date','date_filter').agg(
  F.expr('percentile(growth2, array(0.25))')[0].alias('25th'),
  F.expr('percentile(growth2, array(0.50))')[0].alias('50th'),
  F.expr('percentile(growth2, array(0.75))')[0].alias('75th'),
  F.expr('percentile(growth2, array(0.90))')[0].alias('90th'),
  F.expr('percentile(growth2, array(0.95))')[0].alias('95th'),
  F.expr('percentile(growth2, array(0.99))')[0].alias('99th')))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Metrics - Event period

# COMMAND ----------

display(non_increased_spenders.groupBy('audience_name','event_name','category','event_start_date','event_end_date','date_filter').agg(F.countDistinct("individual_identity_key").alias("Total_shoppers"),F.sum("event_spend").alias("Total_spend"),F.sum("event_frequency").alias("Total_frequency"),(F.sum("event_spend")/F.countDistinct("individual_identity_key")).alias("Avg. spend"),(F.sum("event_frequency")/F.countDistinct("individual_identity_key")).alias("Avg. frequency")))

# COMMAND ----------


