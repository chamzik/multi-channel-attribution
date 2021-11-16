# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Methodology/Steps
# MAGIC  - Translate online_identity_key to individual_identity_key for Impression table
# MAGIC  - Join Impression and Conversion table (left join to Impression table)
# MAGIC  - Keep Impressions that came before the Conversion date or IDs with Impressions but no Conversion
# MAGIC  - Keep first Conversion date (each ID now only has one conversion)
# MAGIC  

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import *

# COMMAND ----------

xref = spark.read.table("signet_prod_coremodel.xref_individual_to_child").withColumnRenamed('user_identity_key', 'online_identity_key').select("individual_identity_key", "online_identity_key")
display(xref.limit(5))

# COMMAND ----------

conversion = spark.read.table("signet_prod_coremodel.fact_conversion").select("individual_identity_key", "order_date").distinct()
display(conversion.limit(5))

# COMMAND ----------

impression = spark.read.table("signet_prod_coremodel.fact_impression").select("online_identity_key", "parent_template_name", "impression_date")\
               .filter(F.col("parent_template_name").isNotNull())\
               .withColumnRenamed("parent_template_name", "message").distinct()
display(impression.limit(5))

# COMMAND ----------

impression = impression.join(xref, "online_identity_key", "inner").select("individual_identity_key", "message", "impression_date").distinct()
display(impression.limit(5))

# COMMAND ----------

df = impression.join(conversion, "individual_identity_key", "left")
display(df.limit(5))

# COMMAND ----------

df_filt = df.filter((F.col("impression_date")<F.col("order_date"))|F.col("order_date").isNull())
display(df_filt)

# COMMAND ----------

display(df_filt.filter(F.col("individual_identity_key") == 'BB0DBwIBAgMEBQAAAAQABAMCBAQz'))

# COMMAND ----------

df_filt.select("individual_identity_key").distinct().count()

# COMMAND ----------

display(df.filter(F.col("individual_identity_key") == 'BB0DBwIBAgMEBQAAAAABAgcHDwcz'))

# COMMAND ----------

conversion2 = df_filt.filter(F.col("order_date").isNotNull())\
                .withColumn("message", F.lit("conversion"))\
                .withColumn("interaction", F.lit("conversion"))\
                .withColumn("conversion", F.lit(1))\
                .select("individual_identity_key", "message", "order_date", "interaction", "conversion")\
                .withColumnRenamed("order_date", "date").distinct()
display(conversion2.limit(5))                                           

# COMMAND ----------

conversion3 = conversion2.groupBy("individual_identity_key").agg(F.min("date").alias("order_date"))

# COMMAND ----------

display(conversion3.filter(F.col("individual_identity_key") == 'BB0DBwIBAgMEBQAAAAQABAMCBAQz'))

# COMMAND ----------

df_filt = df_filt.drop("order_date")
new_df = df_filt.join(conversion3, "individual_identity_key", "left")

# COMMAND ----------

display(new_df)

# COMMAND ----------

display(new_df.filter(F.col("individual_identity_key") == 'BB0DBwIBAgMEBQAAAAQABAMCBAQz'))

# COMMAND ----------

new_df.select("individual_identity_key").distinct().count()

# COMMAND ----------

new_df.filter(F.col("order_date").isNotNull()).select("individual_identity_key").distinct().count()

# COMMAND ----------

#new_df = new_df.filter(F.col("impression_date")<F.col("order_date"))
new_df = new_df.filter((F.col("impression_date")<F.col("order_date"))|F.col("order_date").isNull())
display(new_df.limit(5))

# COMMAND ----------

display(new_df.filter(F.col("individual_identity_key") == 'BB0DBwIBAgMEBQAAAAQABAMCBAQz'))

# COMMAND ----------

impression2 = new_df.withColumn("interaction", F.lit("impression")).withColumn("conversion", F.lit(0)).select("individual_identity_key", "message", "impression_date","interaction", "conversion")\
                .withColumnRenamed("impression_date", "date").distinct()
display(impression2)

# COMMAND ----------

impression2.select("individual_identity_key").distinct().count()

# COMMAND ----------

conversion4.select("individual_identity_key").distinct().count()

# COMMAND ----------

conversion4 = new_df.filter(F.col("order_date").isNotNull())\
                .withColumn("interaction", F.lit("conversion")).withColumn("conversion", F.lit(1)).withColumn("message", F.lit("Conversion"))\
                .select("individual_identity_key", "message", "order_date","interaction", "conversion")\
                .withColumnRenamed("order_date", "date").distinct()
display(conversion4)

# COMMAND ----------

final = impression2.union(conversion4)
display(final)

# COMMAND ----------

display(final.filter(F.col("individual_identity_key") == 'BB0DBwIBAgMEBQAAAAQABAMCBAQz'))

# COMMAND ----------

final = final.withColumn("date", to_timestamp(col("date"),"yyyy-MM-dd HH:mm:ss"))\
              .withColumn("conversion", col("conversion").cast("int"))

# COMMAND ----------

final.write.mode("overwrite").format("delta").save("/mnt/signet-prod-us-east-1-data/analytics/audience_xfer/multi-touch-attribution/test4/raw/attribution_data")

# COMMAND ----------

final = spark.read.format("delta").load("/mnt/signet-prod-us-east-1-data/analytics/audience_xfer/multi-touch-attribution/test3/raw/attribution_data")

# COMMAND ----------

display(final.filter(F.col("individual_identity_key") == 'BB0DBwIBAgMEBQAAAAQABAMCBAQz'))

# COMMAND ----------



# COMMAND ----------

display(impression.groupBy('parent_template_name').count())

# COMMAND ----------

display(impression.groupBy('parent_message_name', "parent_template_name").count())

# COMMAND ----------

display(impression.groupBy('msg_campaign_name').count())

# COMMAND ----------

display(impression.filter(F.col("clicked_flag") == 1))

# COMMAND ----------

display(impression.filter(F.col("online_identity_key") == 'Bh0DCwQIAgABAQUDCQsFBwYAAAMz').select('online_identity_key', 'parent_template_name', 'clicked_flag', 'impression_date').distinct())

# COMMAND ----------

impression = impression.select('online_identity_key', 'parent_template_name', 'clicked_flag', 'impression_date').withColumnRenamed("clicked_flag", "interaction").where(F.col("interaction").isNotNull()).distinct()

display(impression)

# COMMAND ----------

impression.createOrReplaceTempView("imp")

max_click_dt = spark.sql("""

select online_identity_key, min(impression_date) as min_click_date
from 
(select online_identity_key, impression_date
from imp
where interaction = 1
group by 1,2)

group by 1

""")

display(max_click_dt)

# COMMAND ----------

imp2 = impression.join(max_click_dt, 'online_identity_key', 'left')
display(imp2)

# COMMAND ----------

imp2.count()

# COMMAND ----------

imp2.createOrReplaceTempView("imp2")

imp3 = spark.sql("""

select online_identity_key, parent_template_name as channel, interaction as conversion, max_date as date, conversion1 as interaction 
from 
(select *, 
       case when min_click_date is null then impression_date else min_click_date end as max_date,
       case when interaction = 1 then "conversion" else "impression" end as conversion1
from imp2)
where impression_date <= max_date

""")

display(imp3)

# COMMAND ----------

dbutils.fs.rm("/mnt/signet-prod-us-east-1-data/analytics/audience_xfer/multi-touch-attribution/test1/raw/attribution_data",recurse=True)

# COMMAND ----------

imp3.write.format("delta").save("/mnt/signet-prod-us-east-1-data/analytics/audience_xfer/multi-touch-attribution/test1/raw/attribution_data")

# COMMAND ----------

imp3.count()

# COMMAND ----------

display(imp3.select('date').distinct())

# COMMAND ----------


