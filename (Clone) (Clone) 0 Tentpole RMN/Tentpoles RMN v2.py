# Databricks notebook source
import pyspark.sql.functions as F
import pyspark.sql.types as T
import datetime as dt
spark.conf.set("spark.databricks.delta.formatCheck.enabled","false")
client='ulvr'
brand_id_list=[1,99]
current_date=dt.date.today()
year_filter=current_date - dt.timedelta(days=365)
category_list=['Beauty and Wellness','Nutrition and Ice Cream','Personal Care']
print(year_filter,current_date)

# COMMAND ----------

op_cols = ["event","event_start_date","event_end_date"]

event_list=[("SUPER BOWL","11-02-2023","11-02-2023"),
         ("EASTER","31-03-2023","31-03-2023"),
         ("MARCH MADNESS","19-03-2023","08-04-2023"),
         ("MOTHER'S DAY","12-05-2023","12-05-2023"),
         ("MEMORIAL DAY","27-05-2023","27-05-2023"),
         ("FATHER'S DAY","16-06-2023","16-06-2023"),
         ("JULY 4TH","04-07-2023","04-07-2023"),
         ("BACK TO SCHOOL","02-09-2023","02-09-2023"),
         ("THANKSGIVING","28-11-2022","28-11-2022"),
         ("HOLIDAY","25-12-2022","25-12-2022")]

#event_list=[("SUPER BOWL","11-02-2023","11-02-2023")]

op_schema = T.StructType([
  T.StructField("event_name", T.StringType(), False),
  T.StructField("event_start_date_temp", T.StringType(), False),
  T.StructField("event_end_date_temp", T.StringType(), False)])
df1 = spark.createDataFrame(event_list,op_schema)
events_df=df1.select("event_name", F.to_date("event_start_date_temp", "dd-MM-yyyy").alias("event_start_date"), F.to_date("event_end_date_temp", "dd-MM-yyyy").alias("event_end_date"))
events_df=events_df.withColumn('date_filter',F.expr("CASE WHEN event_name='HOLIDAY' THEN (DateAdd(DAY,1-(DAY(event_start_date)),event_start_date)) ELSE DateAdd(DAY,-14,event_start_date) END"))

# COMMAND ----------

display(events_df)

# COMMAND ----------

min_date=events_df.select(F.min("date_filter")).collect()[0][0]
print(min_date) 

# COMMAND ----------

custom_df=spark.read.table(f"{client}_prod_public_works.custom_dim_product_revised")
conv_df=spark.read.table(f"{client}_prod_coremodel.fact_conversion_detail")
conv_df=conv_df.filter(conv_df.order_date>=year_filter)

# COMMAND ----------

conv_custom_cap=conv_df.join(custom_df,on=['brand_id','sku_id'],how="inner")

# df1 --- updated with purchased_product_total_cnt_final using item_cnt_cap
df1=conv_custom_cap.withColumn("purchased_product_total_cnt_final",F.expr("CASE WHEN purchased_product_total_cnt<0 THEN 1 WHEN purchased_product_total_cnt>item_cnt_cap THEN item_cnt_cap ELSE purchased_product_total_cnt END"))

# final --- updated with purchased_product_total_cnt_final and purchased_product_total_usd_final
final=df1.withColumn('price',(df1.purchased_product_total_usd/df1.purchased_product_total_cnt_final)).withColumn("purchased_product_total_usd_final",F.expr("CASE WHEN purchased_product_total_usd<0 THEN 0 WHEN price>price_cap THEN (price_cap*purchased_product_total_cnt_final) ELSE purchased_product_total_usd END"))
conv_custom_df=final.drop("purchased_product_total_usd","purchased_product_total_cnt").withColumnRenamed("purchased_product_total_usd_final","purchased_product_total_usd").withColumnRenamed("purchased_product_total_cnt_final","purchased_product_total_cnt")

# COMMAND ----------

conv_custom_df=conv_custom_df.filter(conv_custom_df.purchased_product_total_usd>0)

# COMMAND ----------

# Fred Meyer included in Kroger
#conv_geo_df=spark.read.table(f"{client}_prod_coremodel.fact_conversion")
#conv_geo_df=conv_geo_df.filter(conv_geo_df.brand_id.isin(brand_id_list))
# conv_geo_df=conv_geo_df.withColumn("retailer",F.expr("case\
#   WHEN ((LCASE(purchase_location_attr) LIKE '%instacart%') OR (LCASE(purchase_location_attr) LIKE '%insta cart%')) THEN 'INSTACART' \
#   WHEN LCASE(purchase_location_attr) LIKE '%walmart%' THEN 'WALMART' \
#   WHEN LCASE(purchase_location_attr) LIKE '%albertsons%' THEN 'ALBERTSONS'\
#   WHEN LCASE(purchase_location_attr) LIKE '%bj''s%' THEN 'BJ_S'\
#   WHEN (\
#       ((LCASE(purchase_location_attr) NOT LIKE '%donut%') AND\
#         (LCASE(purchase_location_attr) NOT LIKE '%pizza%') AND\
#         (LCASE(purchase_location_attr) NOT LIKE '%burger%') AND\
#         (LCASE(purchase_location_attr) NOT LIKE '%dozen%')) AND\
#       ((LCASE(purchase_location_attr) LIKE '%kroger%') OR\
#       ((LCASE(purchase_location_attr) LIKE 'city market %') OR \
#         (LCASE(purchase_location_attr) LIKE '% city market %') OR \
#         (LCASE(purchase_location_attr) LIKE '% city market')) OR\
#       (LCASE(purchase_location_attr) LIKE '%dillons%') OR\
#       ((LCASE(purchase_location_attr) LIKE '%food 4 less%') OR\
#           (LCASE(purchase_location_attr) LIKE '%food4less%')) OR\
#       ((LCASE(purchase_location_attr) LIKE 'foods co %') OR\
#           (LCASE(purchase_location_attr) LIKE '% foods co %') OR\
#           (LCASE(purchase_location_attr) LIKE '% foods co')) OR\
#       (LCASE(purchase_location_attr) LIKE '%fred meyer%') OR\
#       ((LCASE(purchase_location_attr) LIKE '%fry\\'s%') OR\
#           (LCASE(purchase_location_attr) LIKE '%frys%')) OR\
#       (LCASE(purchase_location_attr) LIKE '%gerbes%') OR\
#       (LCASE(purchase_location_attr) LIKE '%harris teeter%') OR\
#       ((LCASE(purchase_location_attr) LIKE 'jayc %') OR\
#           (LCASE(purchase_location_attr) LIKE '% jayc %') OR\
#           (LCASE(purchase_location_attr) LIKE '% jayc') OR\
#           (LCASE(purchase_location_attr) LIKE 'jay c %') OR\
#           (LCASE(purchase_location_attr) LIKE '% jay c %') OR\
#           (LCASE(purchase_location_attr) LIKE '% jay c')) OR\
#       (LCASE(purchase_location_attr) LIKE '%king sooper%') OR\
#       ((LCASE(purchase_location_attr) LIKE '%mariano\\'s %') OR\
#           (LCASE(purchase_location_attr) LIKE '%mariano%')) OR\
#       (LCASE(purchase_location_attr) LIKE '%metro market%') OR\
#       ((LCASE(purchase_location_attr) LIKE '%pay less super market%') OR\
#           (LCASE(purchase_location_attr) LIKE '%pay-less super market%')) OR\
#       ((LCASE(purchase_location_attr) LIKE '%pick\\'n save%') OR\
#           (LCASE(purchase_location_attr) LIKE '%pickn save%')) OR\
#       (LCASE(purchase_location_attr) LIKE '%qfc%') OR\
#       (LCASE(purchase_location_attr) LIKE '%quality food center%') OR\
#       ((LCASE(purchase_location_attr) LIKE '%roundy\\'s%') OR\
#       (LCASE(purchase_location_attr) LIKE '%roundys%')) OR\
#       (LCASE(purchase_location_attr) LIKE '%ralphs%') OR\
#       (LCASE(purchase_location_attr) LIKE '%ruler food%') OR\
#       ((LCASE(purchase_location_attr) LIKE 'smith\\'s %') OR\
#           (LCASE(purchase_location_attr) LIKE '% smith\\'s %') OR\
#           (LCASE(purchase_location_attr) LIKE '% smith\\'s') OR\
#           (LCASE(purchase_location_attr) LIKE 'smiths %') OR\
#           (LCASE(purchase_location_attr) LIKE '% smiths %') OR\
#           (LCASE(purchase_location_attr) LIKE '% smiths')) OR\
#       (LCASE(purchase_location_attr) LIKE '%the little clinic%')) OR\
#       (((LCASE(purchase_location_attr) NOT LIKE '%bakersfield%') AND (LCASE(purchase_location_attr) NOT LIKE '%bakers field%')) AND\
#           ((LCASE(purchase_location_attr) LIKE 'baker\\'s %') OR\
#           (LCASE(purchase_location_attr) LIKE '% baker\\'s %') OR\
#           (LCASE(purchase_location_attr) LIKE '% baker\\'s') OR\
#           (LCASE(purchase_location_attr) LIKE 'bakers %') OR\
#           (LCASE(purchase_location_attr) LIKE '% bakers %') OR\
#           (LCASE(purchase_location_attr) LIKE '% bakers')))\
#     )\
#   THEN 'KROGER'\
#   WHEN LCASE(purchase_location_attr) LIKE '%cvs%' THEN 'CVS'\
#   WHEN LCASE(purchase_location_attr) LIKE '%walgreens%' THEN 'WALGREENS'\
#   WHEN LCASE(purchase_location_attr) LIKE '%target%' THEN 'TARGET'\
#   WHEN LCASE(purchase_location_attr) LIKE '%ulta %' THEN 'ULTA'\
#   WHEN LCASE(purchase_location_attr) LIKE '%amazon%' THEN 'AMAZON'\
#   WHEN LCASE(purchase_location_attr) LIKE '%whole foods%' THEN 'WHOLE_FOODS'\
#   WHEN LCASE(purchase_location_attr) LIKE '%dollar general%' THEN 'DOLLAR_GENERAL'\
#   WHEN LCASE(purchase_location_attr) LIKE '%costco%' THEN 'COSTCO'\
#   WHEN LCASE(purchase_location_attr) LIKE '%wegmans%' THEN 'WEGMANS'\
#   WHEN LCASE(purchase_location_attr) LIKE '%food lion%' THEN 'FOOD_LION'\
#   WHEN LCASE(purchase_location_attr) LIKE '%meijer%' THEN 'MEIJER'\
#   WHEN LCASE(purchase_location_attr) LIKE '%heb %' THEN 'HEB'\
#   WHEN purchase_location_attr LIKE '%super value%' THEN 'SUPER_VALUE'\
#   WHEN LCASE(purchase_location_attr) LIKE '%sams club%' THEN 'SAMS_CLUB'\
#   WHEN LCASE(purchase_location_attr) LIKE '%publix%' THEN 'PUBLIX'\
#   ELSE 'OTHERS' END"))
#conv_geo_df=conv_geo_df.select("brand_id","individual_identity_key","order_sk","retailer")
#conv_geo_df=conv_geo_df.select("brand_id","individual_identity_key","order_sk")
#conv_custom_geo_df=conv_custom_df.join(conv_geo_df,on=["brand_id","individual_identity_key","order_sk"],how="inner")
# ['Beauty and Wellness','Nutrition and Ice Cream','Personal Care']
#conv_custom_geo_df=conv_custom_geo_df.filter(conv_custom_geo_df.category_code.isin(category_list)).withColumn("category",F.expr("CASE WHEN category_code IN ('Beauty and Wellness','Personal Care') THEN 'BWPC' ELSE category_code END")

# COMMAND ----------

conv_custom_geo_df=conv_custom_df.filter(conv_custom_df.category_code.isin(category_list)).withColumn("category",F.expr("CASE WHEN category_code IN ('Beauty and Wellness','Personal Care') THEN 'BWPC' ELSE category_code END"))
#display(conv_custom_geo_df.limit(5))

# COMMAND ----------

display(conv_custom_geo_df.select("category_code", "brand_attr").distinct())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Total Shoppers

# COMMAND ----------

master1=conv_custom_geo_df.filter(conv_custom_geo_df.order_date>min_date).join(events_df,(conv_custom_df.order_date>=events_df.date_filter) & (conv_custom_df.order_date<events_df.event_end_date),'inner')

# COMMAND ----------

#display(master1.limit(3))

# COMMAND ----------

# threshold1 --- event_avg_spend_per_order
calc1=master1.groupBy('category','event_name','event_start_date','event_end_date','date_filter').agg(F.countDistinct('individual_identity_key').alias('total_shoppers'),(F.sum('purchased_product_total_usd')/F.countDistinct('order_sk')).alias('threshold1'))
# threshold1 --- event_avg_spend_per_order
calc1_1=master1.groupBy('category','event_name','event_start_date','event_end_date','date_filter').agg(F.sum('purchased_product_total_usd').alias('total_spend'),F.sum('purchased_product_total_cnt').alias('total_items'),F.countDistinct('order_sk').alias('frequency'))

# COMMAND ----------

display(calc1)

# COMMAND ----------

display(calc1_1)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Increased Spenders

# COMMAND ----------

id_event_spend=master1.groupBy('category','event_name','event_start_date','event_end_date','date_filter','individual_identity_key').agg((F.sum('purchased_product_total_usd')/F.countDistinct('order_sk')).alias('event_avg_spend_per_order'),F.sum('purchased_product_total_usd').alias('event_spend'),F.countDistinct('order_sk').alias('event_frequency')).withColumn('event_flag',F.lit(1))
id_order_df=master1.select("category","individual_identity_key","order_date").distinct()
id_df=id_order_df.select("category","individual_identity_key")

master2=conv_custom_geo_df.join(id_order_df,['category','individual_identity_key','order_date'],'leftanti')

master3=master2.join(id_df,['category','individual_identity_key'],'inner')

master4=conv_custom_geo_df.join(id_df,['category','individual_identity_key'],'inner')

id_overall_spend1=master3.groupBy('category','individual_identity_key').agg((F.sum('purchased_product_total_usd')/F.countDistinct('order_sk')).alias('overall_excl_event_avg_spend_per_order'),F.countDistinct('order_sk').alias('overall_excl_event_frequency'),F.sum('purchased_product_total_usd').alias('overall_excl_event_spend')).withColumn('overall_excl_event_flag',F.lit(1))

id_overall_spend2=master4.groupBy('category','individual_identity_key').agg((F.sum('purchased_product_total_usd')/F.countDistinct('order_sk')).alias('overall_avg_spend_per_order'),F.countDistinct('order_sk').alias('overall_frequency'),F.sum('purchased_product_total_usd').alias('overall_spend')).withColumn('overall_flag',F.lit(1))

# COMMAND ----------

display(master2.select(F.min('order_date'),F.max('order_date')))

# COMMAND ----------

print(master2.select('individual_identity_key').distinct().count(), id_df.count())

# COMMAND ----------

df1=id_overall_spend1.join(id_event_spend,['category','individual_identity_key'],'fullouter')
df2=id_overall_spend2.join(df1,['category','individual_identity_key'],'fullouter')
calc2=df2.withColumn('growth1',((df2.event_avg_spend_per_order-df2.overall_excl_event_avg_spend_per_order)/(df2.overall_avg_spend_per_order))).withColumn('growth2',((df2.event_avg_spend_per_order-df2.overall_avg_spend_per_order)/(df2.overall_avg_spend_per_order)))

# COMMAND ----------

display(calc2.limit(100))

# COMMAND ----------

display(calc2.groupBy('category','event_name','event_start_date','event_end_date','date_filter').agg(
  F.expr('percentile(growth1, array(0.25))')[0].alias('25th'),
  F.expr('percentile(growth1, array(0.50))')[0].alias('50th'),
  F.expr('percentile(growth1, array(0.75))')[0].alias('75th'),
  F.expr('percentile(growth1, array(0.90))')[0].alias('90th'),
  F.expr('percentile(growth1, array(0.95))')[0].alias('95th'),
  F.expr('percentile(growth1, array(0.99))')[0].alias('99th')))

# COMMAND ----------

# display(calc2.groupBy('event_name','event_start_date','event_end_date','date_filter').agg(
#   F.expr('percentile(growth2, array(0.25))')[0].alias('25th'),
#   F.expr('percentile(growth2, array(0.50))')[0].alias('50th'),
#   F.expr('percentile(growth2, array(0.75))')[0].alias('75th'),
#   F.expr('percentile(growth2, array(0.90))')[0].alias('90th'),
#   F.expr('percentile(growth2, array(0.95))')[0].alias('95th'),
#   F.expr('percentile(growth2, array(0.99))')[0].alias('99th')))

# COMMAND ----------

# df2=calc2.groupBy('event_name','retailer','event_start_date','event_end_date','date_filter').agg(
#   F.expr('percentile(growth2, array(0.25))')[0].alias('25th'),
#   F.expr('percentile(growth2, array(0.50))')[0].alias('50th'),
#   F.expr('percentile(growth2, array(0.75))')[0].alias('75th'),
#   F.expr('percentile(growth2, array(0.90))')[0].alias('90th'),
#   F.expr('percentile(growth2, array(0.95))')[0].alias('95th'),
#   F.expr('percentile(growth2, array(0.99))')[0].alias('99th'))

# COMMAND ----------

#calc2.write.format('delta').option("header","true").mode("overwrite").saveAsTable(f"{client}_prod_public_works.lm_tentpoles_rmn_growth_v2")
