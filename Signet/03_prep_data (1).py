# Databricks notebook source
# MAGIC %md
# MAGIC <div >
# MAGIC   <img src="https://cme-solution-accelerators-images.s3-us-west-2.amazonaws.com/toxicity/solution-accelerator-logo.png"; width="50%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC 
# MAGIC ### In this notebook you:
# MAGIC * Create a gold_user_journey table
# MAGIC * Optimize the gold_user_journey table using z-ordering
# MAGIC * Create gold_attribution table
# MAGIC * View first touch vs. last touch by channel
# MAGIC * Upsert data into gold_user_journey and gold_attribution
# MAGIC * Review Delta Lake table history for auditing & governance

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configure the Environment
# MAGIC 
# MAGIC In this step, we will:
# MAGIC   1. Import libraries
# MAGIC   2. Run `utils` notebook to gain access to the functions `get_params`
# MAGIC   3. `get_params` and store values in variables
# MAGIC   4. Set the current database so that it doesn't need to be manually specified each time it's used.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1.1: Import libraries

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1.2: Run the `99_utils` notebook to gain access to the function `get_params`
# MAGIC * `%run` is a magic command provided within Databricks that enables you to run notebooks from within other notebooks.
# MAGIC * `get_params` is a helper function that returns a few parameters used throughout this solution accelerator. Usage of these parameters will be explicit.

# COMMAND ----------

# MAGIC %run ./99_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1.3: `get_params` and store values in variables
# MAGIC 
# MAGIC * Three of the parameters returned by `get_params` are used in this notebook. For convenience, we will store the values for these parameters in new variables. 
# MAGIC   * **database_name:** the name of the database created in notebook `02_load_data`. The default value can be overridden in the notebook `99_config`
# MAGIC   * **gold_user_journey_tbl_path:** the path used in `03_prep_data` to write out gold-level user journey data in delta format.
# MAGIC   * **gold_attribution_tbl_path:** the path used in `03_prep_data` to write out gold-level attribution data in delta format.

# COMMAND ----------

params = get_params()
database_name = params['database_name']
gold_user_journey_tbl_path = params['gold_user_journey_tbl_path']
gold_attribution_tbl_path = params['gold_attribution_tbl_path']

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1.4: Set the current database so that it doesn't need to be manually specified each time it's used.
# MAGIC * Please note that this is a completely optional step. An alternative approach would be to use the syntax `database_name`.`table_name` when querying the respective tables.

# COMMAND ----------

_ = spark.sql("use {}".format(database_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create a Gold-level User Journey Table
# MAGIC 
# MAGIC In this step, we will:
# MAGIC 1. Create a user journey temporary view
# MAGIC 2. View the user journey data
# MAGIC 3. Create and view the gold_user_journey table

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2.1: Create a user journey temporary view
# MAGIC * The query below aggregates each user's journey into a single row. This includes
# MAGIC   * `uid`: the user identifier for a given user.
# MAGIC   * `path`: the list of channels that impressions for a given campaign have been delivered on to a given user.
# MAGIC   * `first_interaction`: the first channel that an impression for a given campaign was delivered on for a given user.
# MAGIC   * `last_interaction`: the last channel that an impression for a given campaign was delivered on for a given user.
# MAGIC   * `conversion`: boolean indicating whether the given user has converted (1) or not (0).
# MAGIC 
# MAGIC * This query is used to create a temporary view. The temporary view will be used in `Step 2.3` to create a table.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from signet_prod_audience_xfer.bronze where individual_identity_key = 'BB0DBwIBAgMEBQAAAAMEBwcCBAEz'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from user_journey_view where individual_identity_key = "BB0DBwIBAgMEBQAAAAMEBwcCBAEz"

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TEMP VIEW user_journey_view AS
# MAGIC SELECT
# MAGIC   sub2.individual_identity_key AS individual_identity_key,CASE
# MAGIC     WHEN sub2.conversion == 1 then concat('Start > ', sub2.path)
# MAGIC     ELSE concat('Start > ', sub2.path, ' > Null')
# MAGIC   END AS path,
# MAGIC   sub2.first_interaction AS first_interaction,
# MAGIC   --sub2.last_interaction AS last_interaction,
# MAGIC   --sub2.last_interaction_conv AS last_interaction_conv,
# MAGIC   CASE WHEN sub2.conversion == 1 THEN sub2.last_interaction_conv ELSE sub2.last_interaction END AS last_interaction, 
# MAGIC   sub2.conversion AS conversion,
# MAGIC   sub2.visiting_order AS visiting_order
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       sub.individual_identity_key AS individual_identity_key,
# MAGIC       concat_ws(' > ', collect_list(sub.message)) AS path,
# MAGIC       element_at(collect_list(sub.message), 1) AS first_interaction,
# MAGIC       element_at(collect_list(sub.message), -1) AS last_interaction,
# MAGIC       element_at(collect_list(sub.message), -2) AS last_interaction_conv,
# MAGIC       element_at(collect_list(sub.conversion), -1) AS conversion,
# MAGIC       collect_list(sub.visit_order) AS visiting_order
# MAGIC     FROM
# MAGIC       (
# MAGIC         SELECT
# MAGIC           individual_identity_key,
# MAGIC           message,
# MAGIC           date,
# MAGIC           conversion,
# MAGIC           dense_rank() OVER (
# MAGIC             PARTITION BY individual_identity_key
# MAGIC             ORDER BY
# MAGIC               date asc
# MAGIC           ) as visit_order
# MAGIC         FROM
# MAGIC           signet_prod_audience_xfer.bronze
# MAGIC       ) AS sub
# MAGIC     GROUP BY
# MAGIC       sub.individual_identity_key
# MAGIC   ) AS sub2;

# COMMAND ----------

bronze = spark.read.table("signet_prod_audience_xfer.bronze")
bronze.createOrReplaceTempView("bronze")

# COMMAND ----------

temp1 = spark.sql("""

SELECT
          individual_identity_key,
          message,
          date,
          conversion,
          dense_rank() OVER (
            PARTITION BY individual_identity_key
            ORDER BY
              date asc
          ) as visit_order
        FROM
          bronze
          order by visit_order asc

""")

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import *

temp2 = temp1.orderBy("date", ascending=True).groupBy("individual_identity_key").agg(F.collect_list("visit_order"))

# COMMAND ----------

display(temp1.filter(F.col("individual_identity_key")=="BB0DBwIBAgMEBQAAAAMEBwcCBAEz"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import concat_ws,col

window = Window.partitionBy(col("individual_identity_key")).orderBy(col("visit_order"))
sortedDF = temp1.withColumn("visiting_order", collect_list("visit_order").over(window))
sort2 = sortedDF.withColumn("path", collect_list("message").over(window))

sort2 = sortedDF.withColumn("path_list", collect_list("message").over(window))\
            .select("individual_identity_key", "message", "date", "conversion", "visit_order", "visiting_order", "path_list", concat_ws('>', collect_list("message").over(window)).alias("path"))

window2 = Window.partitionBy(col("individual_identity_key")).orderBy(col("visit_order").desc())
sort3 = sort2.withColumn("row", row_number().over(window2)).filter(F.col("row")==1).select("individual_identity_key", "conversion", "visiting_order", "path", "path_list")

display(sort3.filter(F.col("individual_identity_key")=="BB0DBwIBAgMEBQAABAUEDAwPBwUz"))

# COMMAND ----------

display(sort3.filter(F.col("individual_identity_key")=="BB0DBwIBAgMEBQAAAAMEBwcCBAEz"))

# COMMAND ----------

sort3.createOrReplaceTempView("s3")

temp = spark.sql("""

select
    individual_identity_key AS individual_identity_key,
    CASE WHEN conversion == 1 then concat('Start > ', path)
    ELSE concat('Start > ', path, ' > Null')
  END AS path,
  first_interaction AS first_interaction,
  CASE WHEN conversion == 1 THEN last_interaction_conv ELSE last_interaction END AS last_interaction, 
  conversion AS conversion,
  visiting_order AS visiting_order
  from (
        select 
              individual_identity_key AS individual_identity_key,
              path,
              element_at(path_list, 1) AS first_interaction,
              element_at(path_list, -1) AS last_interaction,
              element_at(path_list, -2) AS last_interaction_conv,
              conversion,
              visiting_order
        from s3)

""")

display(temp)

# COMMAND ----------

display(temp.filter(F.col("conversion")==1))

# COMMAND ----------

display(temp.filter(F.col("individual_identity_key")=="BB0DBwIBAgMEBQAAAAMEBwcCBAEz"))

# COMMAND ----------

raw_data_df.write.mode("overwrite").format("delta").save("/mnt/signet-prod-us-east-1-data/analytics/audience_xfer/multi-touch-attribution/test4/bronze")

# COMMAND ----------

# Create bronze table
temp = spark.sql('''
  CREATE TABLE signet_prod_audience_xfer.bronze
  USING DELTA 
  LOCATION 'dbfs:/mnt/signet-prod-us-east-1-data/analytics/audience_xfer/multi-touch-attribution/test4/bronze'
  ''')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW temp2 AS
# MAGIC     SELECT
# MAGIC       sub.individual_identity_key AS individual_identity_key,
# MAGIC       concat_ws(' > ', collect_list(sub.message)) AS path,
# MAGIC       element_at(collect_list(sub.message), 1) AS first_interaction,
# MAGIC       element_at(collect_list(sub.message), -1) AS last_interaction,
# MAGIC       --element_at(collect_list(sub.message), -2) AS last_interaction_conv,
# MAGIC       element_at(collect_list(sub.conversion), -1) AS conversion,
# MAGIC       collect_list(sub.visit_order) AS visiting_order
# MAGIC     FROM
# MAGIC       (
# MAGIC         temp1
# MAGIC       ) AS sub
# MAGIC     GROUP BY
# MAGIC       sub.individual_identity_key

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from temp2 WHERE individual_identity_key = 'BB0DBwIBAgMEBQAABAUEDAwPBwUz'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from temp2 WHERE individual_identity_key = 'BB0DBwIBAgMEBQAAAAAEBQIFDggz'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   sub2.individual_identity_key AS individual_identity_key,CASE
# MAGIC     WHEN sub2.conversion == 1 then concat('Start > ', sub2.path)
# MAGIC     ELSE concat('Start > ', sub2.path, ' > Null')
# MAGIC   END AS path,
# MAGIC   sub2.first_interaction AS first_interaction,
# MAGIC   --sub2.last_interaction AS last_interaction,
# MAGIC   --sub2.last_interaction_conv AS last_interaction_conv,
# MAGIC   CASE WHEN sub2.conversion == 1 THEN sub2.last_interaction_conv ELSE sub2.last_interaction END AS last_interaction, 
# MAGIC   sub2.conversion AS conversion,
# MAGIC   sub2.visiting_order AS visiting_order
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       sub.individual_identity_key AS individual_identity_key,
# MAGIC       concat_ws(' > ', collect_list(sub.message)) AS path,
# MAGIC       element_at(collect_list(sub.message), 1) AS first_interaction,
# MAGIC       element_at(collect_list(sub.message), -1) AS last_interaction,
# MAGIC       element_at(collect_list(sub.message), -2) AS last_interaction_conv,
# MAGIC       element_at(collect_list(sub.conversion), -1) AS conversion,
# MAGIC       collect_list(sub.visit_order) AS visiting_order
# MAGIC     FROM
# MAGIC       (
# MAGIC         SELECT
# MAGIC           individual_identity_key,
# MAGIC           message,
# MAGIC           date,
# MAGIC           conversion,
# MAGIC           dense_rank() OVER (
# MAGIC             PARTITION BY individual_identity_key
# MAGIC             ORDER BY
# MAGIC               date asc
# MAGIC           ) as visit_order
# MAGIC         FROM
# MAGIC           signet_prod_audience_xfer.bronze WHERE individual_identity_key = 'BB0DBwIBAgMEBQAABAUEDAwPBwUz'
# MAGIC       ) AS sub
# MAGIC     GROUP BY
# MAGIC       sub.individual_identity_key
# MAGIC   ) AS sub2

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2.2: View the user journey data

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from signet_prod_audience_xfer.bronze where individual_identity_key = 'BB0DBwIBAgMEBQAAAAAEBQIFDggz'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from signet_prod_audience_xfer.bronze where individual_identity_key = 'BB0DBwIBAgMEBQAABAUEDAwPBwUz'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM user_journey_view2 where individual_identity_key = 'BB0DBwIBAgMEBQAAAAAEBQIFDggz'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM user_journey_view where individual_identity_key = 'BB0DBwIBAgMEBQAAAAMEAAYBDgUz'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM user_journey_view where conversion = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2.3: Create and view the gold_user_journey table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC drop table if exists signet_prod_audience_xfer.gold_user_journey;

# COMMAND ----------

user_journey_view = spark.sql('''
  CREATE TABLE IF NOT EXISTS `{}`.gold_user_journey
  USING DELTA 
  LOCATION '{}'
  AS SELECT * from user_journey_view
  '''.format(database_name, gold_user_journey_tbl_path))

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/signet-prod-us-east-1-data/analytics/audience_xfer/multi-touch-attribution/test4/gold_user_journey",recurse=True)

# COMMAND ----------

temp = spark.sql('''
  CREATE TABLE IF NOT EXISTS `{}`.gold_user_journey
  USING DELTA 
  LOCATION '{}'
  AS SELECT * from t1
  '''.format(database_name, gold_user_journey_tbl_path))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from signet_prod_audience_xfer.gold_user_journey where individual_identity_key = "BB0DBwIBAgMEBQAAAAMEBwcCBAEz"

# COMMAND ----------

temp.createOrReplaceTempView("t1")

spark.sql("select * from t1 where path like '%Conversion >%'").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from signet_prod_audience_xfer.gold_user_journey where path like "%Conversion >%"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from signet_prod_audience_xfer.gold_user_journey where conversion = 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Optimize the gold_user_journey table
# MAGIC * [Z-Ordering](https://docs.databricks.com/delta/optimizations/file-mgmt.html#z-ordering-multi-dimensional-clustering) is a technique used to co-locate related information into the same set of files. This co-locality is automatically used by Delta Lake's data-skipping algorithms to dramatically reduce the amount of data that needs to be read. The less data that needs to be read, the quicker that query results are returned.
# MAGIC 
# MAGIC * In practice, Z-ordering is most suitable for high-cardinality columns that you frequently want to filter on.
# MAGIC 
# MAGIC * Please note that the data set we are using here is relatively small and Z-ordering is likely unncessary. It has been included, however, for illustration purposes.

# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE signet_prod_audience_xfer.gold_user_journey ZORDER BY individual_identity_key

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create gold-level attribution summary table
# MAGIC 
# MAGIC In the table, `gold_user_journey`, that we just created in the previous step, we captured the values for `first_interaction` and `last_interaction` in their own respective columns. With this data now in place, let's take a look at attribution using the heuristic methods `first-touch` and `last-touch`. 
# MAGIC 
# MAGIC In this step, we will:
# MAGIC 1. Create a temporary view for first-touch and last-touch attribution metrics
# MAGIC 2. Use the temporary view to create the gold_attribution table
# MAGIC 3. Use the gold_attribution table to view first touch vs. last touch by channel
# MAGIC 
# MAGIC After we build our Markov model in the next notebook, `04_markov_chains`, we will then take a look at how attribution using a data-driven method compares to these heuristic methods.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4.1: Create temporary view for first-touch and last-touch attribution metrics

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TEMP VIEW attribution_view AS
# MAGIC SELECT
# MAGIC   'first_touch' AS attribution_model,
# MAGIC   first_interaction AS message,
# MAGIC   round(count(*) / (
# MAGIC      SELECT COUNT(*)
# MAGIC      FROM signet_prod_audience_xfer.gold_user_journey
# MAGIC      WHERE conversion = 1),2) AS attribution_percent
# MAGIC FROM signet_prod_audience_xfer.gold_user_journey
# MAGIC WHERE conversion = 1
# MAGIC GROUP BY first_interaction
# MAGIC UNION
# MAGIC SELECT
# MAGIC   'last_touch' AS attribution_model,
# MAGIC   last_interaction AS message,
# MAGIC   round(count(*) /(
# MAGIC       SELECT COUNT(*)
# MAGIC       FROM signet_prod_audience_xfer.gold_user_journey
# MAGIC       WHERE conversion = 1),2) AS attribution_percent
# MAGIC FROM signet_prod_audience_xfer.gold_user_journey
# MAGIC WHERE conversion = 1
# MAGIC GROUP BY last_interaction

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4.2: Use the temporary view to create the gold_attribution table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC drop table if exists signet_prod_audience_xfer.gold_attribution;

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/signet-prod-us-east-1-data/analytics/audience_xfer/multi-touch-attribution/test3/gold_attribution",recurse=True)

# COMMAND ----------

gold_user_journey = spark.sql('''
CREATE TABLE IF NOT EXISTS signet_prod_audience_xfer.gold_attribution
USING DELTA
LOCATION '{}'
AS
SELECT * FROM attribution_view'''.format(gold_attribution_tbl_path))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM signet_prod_audience_xfer.gold_attribution

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4.3: Use the gold_attribution table to view first touch vs. last touch by channel

# COMMAND ----------

attribution_pd = spark.table('signet_prod_audience_xfer.gold_attribution').toPandas()

sns.set(font_scale=1.1)
sns.catplot(x='message',y='attribution_percent',hue='attribution_model',data=attribution_pd, kind='bar', aspect=2).set_xticklabels(rotation=15)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Appendix: Production
# MAGIC 
# MAGIC In this appendix, we will:
# MAGIC * Demonstrate that Delta Lake brings ACID transaction and full DML support to data lakes (e.g. delete, update, merge into)
# MAGIC * Demonstrate how auditing and governance is enabled by Delta Lake

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Example 1: Upsert data into the gold_user_journey table

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO signet_prod_audience_xfer.gold_user_journey
# MAGIC USING user_journey_view
# MAGIC ON user_journey_view.individual_identity_key = gold_user_journey.individual_identity_key
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Example 2: Propogate updates made to the gold_user_journey table to the gold_attribution table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW attribution_view AS
# MAGIC SELECT 'first_touch' AS attribution_model, first_interaction AS message, 
# MAGIC         round(count(*)/(SELECT COUNT(*) FROM signet_prod_audience_xfer.gold_user_journey WHERE conversion =1), 2)AS attribution_percent 
# MAGIC FROM signet_prod_audience_xfer.gold_user_journey 
# MAGIC WHERE conversion =1 
# MAGIC GROUP BY first_interaction
# MAGIC UNION
# MAGIC SELECT 'last_touch' AS attribution_model, last_interaction AS message, 
# MAGIC         round(count(*)/(SELECT COUNT(*) FROM signet_prod_audience_xfer.gold_user_journey WHERE conversion =1), 2)AS attribution_percent 
# MAGIC FROM signet_prod_audience_xfer.gold_user_journey 
# MAGIC WHERE conversion =1 
# MAGIC GROUP BY last_interaction

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO signet_prod_audience_xfer.gold_attribution
# MAGIC USING attribution_view
# MAGIC ON attribution_view.attribution_model = gold_attribution.attribution_model AND attribution_view.message = gold_attribution.message
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Example 3: Review Delta Lake table history for auditing & governance
# MAGIC * All of the transactions made on this table are stored and can be easily queried.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC describe history signet_prod_audience_xfer.gold_user_journey

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC * Create Markov Chain Attribution Model

# COMMAND ----------

# MAGIC %md
# MAGIC Copyright Databricks, Inc. [2021]. The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC 
# MAGIC |Library Name|Library license | Library License URL | Library Source URL |
# MAGIC |---|---|---|---|
# MAGIC |Matplotlib|Python Software Foundation (PSF) License |https://matplotlib.org/stable/users/license.html|https://github.com/matplotlib/matplotlib|
# MAGIC |Numpy|BSD-3-Clause License|https://github.com/numpy/numpy/blob/master/LICENSE.txt|https://github.com/numpy/numpy|
# MAGIC |Pandas|BSD 3-Clause License|https://github.com/pandas-dev/pandas/blob/master/LICENSE|https://github.com/pandas-dev/pandas|
# MAGIC |Python|Python Software Foundation (PSF) |https://github.com/python/cpython/blob/master/LICENSE|https://github.com/python/cpython|
# MAGIC |Seaborn|BSD-3-Clause License|https://github.com/mwaskom/seaborn/blob/master/LICENSE|https://github.com/mwaskom/seaborn|
# MAGIC |Spark|Apache-2.0 License |https://github.com/apache/spark/blob/master/LICENSE|https://github.com/apache/spark|
