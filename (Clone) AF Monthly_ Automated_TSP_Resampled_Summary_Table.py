# Databricks notebook source
# Databricks notebook source
# MAGIC %md ### Census Data EDA

import pyspark.sql.functions as F
import datetime as dt
from pyspark.sql.functions import *
from datetime import date, timedelta

# MAGIC %md ##### Reading the data

# COMMAND ----------
division_mapping = spark.createDataFrame(schema = ["StateName","Division"],
                                         data = [('Alabama', 'East South Central'), ('Alaska', 'Pacific'), ('Arizona', 'Mountain'), ('Arkansas', 'West South Central'), ('California', 'Pacific'), ('Colorado', 'Mountain'), ('Connecticut', 'New England'), ('Delaware', 'South Atlantic'), ('District of Columbia', 'South Atlantic'), ('Washington DC', 'South Atlantic'),('Florida', 'South Atlantic'), ('Georgia', 'South Atlantic'), ('Hawaii', 'Pacific'), ('Idaho', 'Mountain'), ('Illinois', 'East North Central'), ('Indiana', 'East North Central'), ('Iowa', 'West North Central'), ('Kansas', 'West North Central'), ('Kentucky', 'East South Central'), ('Louisiana', 'West South Central'), ('Maine', 'New England'), ('Maryland', 'South Atlantic'), ('Massachusetts', 'New England'), ('Michigan', 'East North Central'), ('Minnesota', 'West North Central'), ('Mississippi', 'East South Central'), ('Missouri', 'West North Central'), ('Montana', 'Mountain'), ('Nebraska', 'West North Central'), ('Nevada', 'Mountain'), ('New Hampshire', 'New England'), ('New Jersey', 'Middle Atlantic'), ('New Mexico', 'Mountain'), ('New York', 'Middle Atlantic'), ('North Carolina', 'South Atlantic'), ('North Dakota', 'West North Central'), ('Ohio', 'East North Central'), ('Oklahoma', 'West South Central'), ('Oregon', 'Pacific'), ('Pennsylvania', 'Middle Atlantic'), ('Rhode Island', 'New England'), ('South Carolina', 'South Atlantic'), ('South Dakota', 'West North Central'), ('Tennessee', 'East South Central'), ('Texas', 'West South Central'), ('Utah', 'Mountain'), ('Vermont', 'New England'), ('Virginia', 'South Atlantic'), ('Washington', 'Pacific'), ('West Virginia', 'South Atlantic'), ('Wisconsin', 'East North Central'), ('Wyoming', 'Mountain')])

# COMMAND ----------

census = spark.read.table("mcdonalds_prod_public_works.census_2023")
census = census.join(division_mapping, on="StateName",how="inner")

# COMMAND ----------

# MAGIC %md #### Age-Gender-Ethnicity

# COMMAND ----------

req_cols = ['StateName', 'Division', 'White_M_Age0_4', 'White_M_Age5_9', 'White_M_Age10_14', 'White_M_Age15_17', 'White_M_Age18_20', 'White_M_Age21_24', 'White_M_Age25_34', 'White_M_Age35_44', 'White_M_Age45_54', 'White_M_Age55_64', 'White_M_Age65_74', 'White_M_Age75_84', 'White_M_Age85p', 'White_M_Age18p', 'White_M_Age21p', 'White_M_Age65p', 'White_F_Age0_4', 'White_F_Age5_9', 'White_F_Age10_14', 'White_F_Age15_17', 'White_F_Age18_20', 'White_F_Age21_24', 'White_F_Age25_34', 'White_F_Age35_44', 'White_F_Age45_54', 'White_F_Age55_64', 'White_F_Age65_74', 'White_F_Age75_84', 'White_F_Age85p', 'White_F_Age18p', 'White_F_Age21p', 'White_F_Age65p', 'B_AA_M_Age0_4', 'B_AA_M_Age5_9', 'B_AA_M_Age10_14', 'B_AA_M_Age15_17', 'B_AA_M_Age18_20', 'B_AA_M_Age21_24', 'B_AA_M_Age25_34', 'B_AA_M_Age35_44', 'B_AA_M_Age45_54', 'B_AA_M_Age55_64', 'B_AA_M_Age65_74', 'B_AA_M_Age75_84', 'B_AA_M_Age85p', 'B_AA_M_Age18p', 'B_AA_M_Age21p', 'B_AA_M_Age65p', 'B_AA_F_Age0_4', 'B_AA_F_Age5_9', 'B_AA_F_Age10_14', 'B_AA_F_Age15_17', 'B_AA_F_Age18_20', 'B_AA_F_Age21_24', 'B_AA_F_Age25_34', 'B_AA_F_Age35_44', 'B_AA_F_Age45_54', 'B_AA_F_Age55_64', 'B_AA_F_Age65_74', 'B_AA_F_Age75_84', 'B_AA_F_Age85p', 'B_AA_F_Age18p', 'B_AA_F_Age21p', 'B_AA_F_Age65p', 'AI_AN_M_Age0_4', 'AI_AN_M_Age5_9', 'AI_AN_M_Age10_14', 'AI_AN_M_Age15_17', 'AI_AN_M_Age18_20', 'AI_AN_M_Age21_24', 'AI_AN_M_Age25_34', 'AI_AN_M_Age35_44', 'AI_AN_M_Age45_54', 'AI_AN_M_Age55_64', 'AI_AN_M_Age65_74', 'AI_AN_M_Age75_84', 'AI_AN_M_Age85p', 'AI_AN_M_Age18p', 'AI_AN_M_Age21p', 'AI_AN_M_Age65p', 'AI_AN_F_Age0_4', 'AI_AN_F_Age5_9', 'AI_AN_F_Age10_14', 'AI_AN_F_Age15_17', 'AI_AN_F_Age18_20', 'AI_AN_F_Age21_24', 'AI_AN_F_Age25_34', 'AI_AN_F_Age35_44', 'AI_AN_F_Age45_54', 'AI_AN_F_Age55_64', 'AI_AN_F_Age65_74', 'AI_AN_F_Age75_84', 'AI_AN_F_Age85p', 'AI_AN_F_Age18p', 'AI_AN_F_Age21p', 'AI_AN_F_Age65p', 'Asian_M_Age0_4', 'Asian_M_Age5_9', 'Asian_M_Age10_14', 'Asian_M_Age15_17', 'Asian_M_Age18_20', 'Asian_M_Age21_24', 'Asian_M_Age25_34', 'Asian_M_Age35_44', 'Asian_M_Age45_54', 'Asian_M_Age55_64', 'Asian_M_Age65_74', 'Asian_M_Age75_84', 'Asian_M_Age85p', 'Asian_M_Age18p', 'Asian_M_Age21p', 'Asian_M_Age65p', 'Asian_F_Age0_4', 'Asian_F_Age5_9', 'Asian_F_Age10_14', 'Asian_F_Age15_17', 'Asian_F_Age18_20', 'Asian_F_Age21_24', 'Asian_F_Age25_34', 'Asian_F_Age35_44', 'Asian_F_Age45_54', 'Asian_F_Age55_64', 'Asian_F_Age65_74', 'Asian_F_Age75_84', 'Asian_F_Age85p', 'Asian_F_Age18p', 'Asian_F_Age21p', 'Asian_F_Age65p', 'NH_PI_M_Age0_4', 'NH_PI_M_Age5_9', 'NH_PI_M_Age10_14', 'NH_PI_M_Age15_17', 'NH_PI_M_Age18_20', 'NH_PI_M_Age21_24', 'NH_PI_M_Age25_34', 'NH_PI_M_Age35_44', 'NH_PI_M_Age45_54', 'NH_PI_M_Age55_64', 'NH_PI_M_Age65_74', 'NH_PI_M_Age75_84', 'NH_PI_M_Age85p', 'NH_PI_M_Age18p', 'NH_PI_M_Age21p', 'NH_PI_M_Age65p', 'NH_PI_F_Age0_4', 'NH_PI_F_Age5_9', 'NH_PI_F_Age10_14', 'NH_PI_F_Age15_17', 'NH_PI_F_Age18_20', 'NH_PI_F_Age21_24', 'NH_PI_F_Age25_34', 'NH_PI_F_Age35_44', 'NH_PI_F_Age45_54', 'NH_PI_F_Age55_64', 'NH_PI_F_Age65_74', 'NH_PI_F_Age75_84', 'NH_PI_F_Age85p', 'NH_PI_F_Age18p', 'NH_PI_F_Age21p', 'NH_PI_F_Age65p', 'Other_M_Age0_4', 'Other_M_Age5_9', 'Other_M_Age10_14', 'Other_M_Age15_17', 'Other_M_Age18_20', 'Other_M_Age21_24', 'Other_M_Age25_34', 'Other_M_Age35_44', 'Other_M_Age45_54', 'Other_M_Age55_64', 'Other_M_Age65_74', 'Other_M_Age75_84', 'Other_M_Age85p', 'Other_M_Age18p', 'Other_M_Age21p', 'Other_M_Age65p', 'Other_F_Age0_4', 'Other_F_Age5_9', 'Other_F_Age10_14', 'Other_F_Age15_17', 'Other_F_Age18_20', 'Other_F_Age21_24', 'Other_F_Age25_34', 'Other_F_Age35_44', 'Other_F_Age45_54', 'Other_F_Age55_64', 'Other_F_Age65_74', 'Other_F_Age75_84', 'Other_F_Age85p', 'Other_F_Age18p', 'Other_F_Age21p', 'Other_F_Age65p', 'pRaces_M_Age0_4', 'pRaces_M_Age5_9', 'pRaces_M_Age10_14', 'pRaces_M_Age15_17', 'pRaces_M_Age18_20', 'pRaces_M_Age21_24', 'pRaces_M_Age25_34', 'pRaces_M_Age35_44', 'pRaces_M_Age45_54', 'pRaces_M_Age55_64', 'pRaces_M_Age65_74', 'pRaces_M_Age75_84', 'pRaces_M_Age85p', 'pRaces_M_Age18p', 'pRaces_M_Age21p', 'pRaces_M_Age65p', 'pRaces_F_Age0_4', 'pRaces_F_Age5_9', 'pRaces_F_Age10_14', 'pRaces_F_Age15_17', 'pRaces_F_Age18_20', 'pRaces_F_Age21_24', 'pRaces_F_Age25_34', 'pRaces_F_Age35_44', 'pRaces_F_Age45_54', 'pRaces_F_Age55_64', 'pRaces_F_Age65_74', 'pRaces_F_Age75_84', 'pRaces_F_Age85p', 'pRaces_F_Age18p', 'pRaces_F_Age21p', 'pRaces_F_Age65p', 'H_L_M_Age0_4', 'H_L_M_Age5_9', 'H_L_M_Age10_14', 'H_L_M_Age15_17', 'H_L_M_Age18_20', 'H_L_M_Age21_24', 'H_L_M_Age25_34', 'H_L_M_Age35_44', 'H_L_M_Age45_54', 'H_L_M_Age55_64', 'H_L_M_Age65_74', 'H_L_M_Age75_84', 'H_L_M_Age85p', 'H_L_M_Age18p', 'H_L_M_Age21p', 'H_L_M_Age25p', 'H_L_M_Age65p', 'H_L_F_Age0_4', 'H_L_F_Age5_9', 'H_L_F_Age10_14', 'H_L_F_Age15_17', 'H_L_F_Age18_20', 'H_L_F_Age21_24', 'H_L_F_Age25_34', 'H_L_F_Age35_44', 'H_L_F_Age45_54', 'H_L_F_Age55_64', 'H_L_F_Age65_74', 'H_L_F_Age75_84', 'H_L_F_Age85p', 'H_L_F_Age18p', 'H_L_F_Age21p', 'H_L_F_Age25p', 'H_L_F_Age65p']

# COMMAND ----------

census_sub = census.select(req_cols)
state_data = census_sub.selectExpr('StateName' ,'Division', "stack({}, {})".format(len(req_cols[2:]), ', '.join(("'{}', {}".format(i, i) for i in req_cols[2:]))))
state_data = state_data.withColumnRenamed("col0","Variable").withColumnRenamed("col1","Count")

# COMMAND ----------

# MAGIC %md ##### State Level Data

# COMMAND ----------

state_data = state_data.withColumn("Ethnicity",F.split("Variable","_M_|_F_").getItem(0)).withColumn("Age",F.split("Variable","Age").getItem(1)).withColumn("Gender",F.when(F.col("Variable").contains("_M_Age"),"Male").otherwise("Female"))

# COMMAND ----------

# MAGIC %md ##### Division Level Data

# COMMAND ----------

division_data = state_data.groupBy("Division","Variable","Ethnicity","Age","Gender").agg(F.sum("Count").alias("Count"))
census_div_tot =  division_data.groupBy("Division").agg(F.sum("Count").alias("div_total"))
census_div_data = census_div_tot.join(division_data, on="Division", how="inner").withColumn("Ethnicity", F.when(F.col("Ethnicity")=="AI_AN", "American Indian/Alaskan Native").when(F.col("Ethnicity")=="B_AA", "Black/African American").when(F.col("Ethnicity")=="H_L", "Hispanic/Latino").when(F.col("Ethnicity")=="NH_PI", "Native Hawaiian/Pacific Islander").when(F.col("Ethnicity")=="Other", "Others").otherwise(F.col("Ethnicity")))

# COMMAND ----------

# MAGIC %md #### Ethnicity-Income

# COMMAND ----------

inc_cols = ['StateName', 'Division', 'White_HHIlt15000','White_HHI15000_24999','White_HHI25000_34999','White_HHI35000_49999','White_HHI50000_74999','White_HHI75000_99999','White_HHI100000_124999','White_HHI125000_149999','White_HHI150000_199999','White_HHI200000p','B_AA_HHIlt15000','B_AA_HHI15000_24999','B_AA_HHI25000_34999','B_AA_HHI35000_49999','B_AA_HHI50000_74999','B_AA_HHI75000_99999','B_AA_HHI100000_124999','B_AA_HHI125000_149999','B_AA_HHI150000_199999','B_AA_HHI200000p','AI_AN_HHIlt15000','AI_AN_HHI15000_24999','AI_AN_HHI25000_34999','AI_AN_HHI35000_49999','AI_AN_HHI50000_74999','AI_AN_HHI75000_99999','AI_AN_HHI100000_124999','AI_AN_HHI125000_149999','AI_AN_HHI150000_199999','AI_AN_HHI200000p','Asian_HHIlt15000','Asian_HHI15000_24999','Asian_HHI25000_34999','Asian_HHI35000_49999','Asian_HHI50000_74999','Asian_HHI75000_99999','Asian_HHI100000_124999','Asian_HHI125000_149999','Asian_HHI150000_199999','Asian_HHI200000p','NH_PI_HHIlt15000','NH_PI_HHI15000_24999','NH_PI_HHI25000_34999','NH_PI_HHI35000_49999','NH_PI_HHI50000_74999','NH_PI_HHI75000_99999','NH_PI_HHI100000_124999','NH_PI_HHI125000_149999','NH_PI_HHI150000_199999','NH_PI_HHI200000p','Other_HHIncomelt15000','Other_HHIncome15000_24999','Other_HHIncome25000_34999','Other_HHIncome35000_49999','Other_HHIncome50000_74999','Other_HHIncome75000_99999','Other_HHIncome100000_124999','Other_HHIncome125000_149999','Other_HHIncome150000_199999','Other_HHIncome200000p','H_L_HHIlt15000','H_L_HHI15000_24999','H_L_HHI25000_34999','H_L_HHI35000_49999','H_L_HHI50000_74999','H_L_HHI75000_99999','H_L_HHI100000_124999','H_L_HHI125000_149999','H_L_HHI150000_199999','H_L_HHI200000p']
len(inc_cols)

# COMMAND ----------

income_data = census.select(inc_cols)
state_data_inc = income_data.selectExpr('StateName' ,'Division', "stack({}, {})".format(len(inc_cols[2:]), ', '.join(("'{}', {}".format(i, i) for i in inc_cols[2:]))))
state_data_inc = state_data_inc.withColumnRenamed("col0","Variable").withColumnRenamed("col1","Count")

# COMMAND ----------

state_data_inc = state_data_inc.withColumn("Variable",F.regexp_replace(F.col("Variable"),"Income","I")).withColumn("Ethnicity",F.split("Variable","_HHI").getItem(0)).withColumn("Income",F.split("Variable","_HHI").getItem(1))

# COMMAND ----------

division_data_inc = state_data_inc.groupBy("Division","Variable","Ethnicity","Income").agg(F.sum("Count").alias("Count"))
census_div_tot_inc =  division_data_inc.groupBy("Division").agg(F.sum("Count").alias("div_total"))
census_div_data_inc = census_div_tot_inc.join(division_data_inc, on="Division", how="inner").withColumn("Ethnicity", F.when(F.col("Ethnicity")=="AI_AN", "American Indian/Alaskan Native").when(F.col("Ethnicity")=="B_AA", "Black/African American").when(F.col("Ethnicity")=="H_L", "Hispanic/Latino").when(F.col("Ethnicity")=="NH_PI", "Native Hawaiian/Pacific Islander").when(F.col("Ethnicity")=="Other", "Others").otherwise(F.col("Ethnicity")))

# COMMAND ----------

# MAGIC %md #### Age-Income

# COMMAND ----------

age_inc_cols = ['StateName', 'Division', 'Age15_24_HHIlt15000','Age15_24_HHI15000_24999','Age15_24_HHI25000_34999','Age15_24_HHI35000_49999','Age15_24_HHI50000_74999','Age15_24_HHI75000_99999','Age15_24_HHI100000_124999','Age15_24_HHI125000_149999','Age15_24_HHI150000_199999','Age15_24_HHI200000p','Age25_34_HHIlt15000','Age25_34_HHI15000_24999','Age25_34_HHI25000_34999','Age25_34_HHI35000_49999','Age25_34_HHI50000_74999','Age25_34_HHI75000_99999','Age25_34_HHI100000_124999','Age25_34_HHI125000_149999','Age25_34_HHI150000_199999','Age25_34_HHI200000p','Age35_44_HHIlt15000','Age35_44_HHI15000_24999','Age35_44_HHI25000_34999','Age35_44_HHI35000_49999','Age35_44_HHI50000_74999','Age35_44_HHI75000_99999','Age35_44_HHI100000_124999','Age35_44_HHI125000_149999','Age35_44_HHI150000_199999','Age35_44_HHI200000p','Age45_54_HHIlt15000','Age45_54_HHI15000_24999','Age45_54_HHI25000_34999','Age45_54_HHI35000_49999','Age45_54_HHI50000_74999','Age45_54_HHI75000_99999','Age45_54_HHI100000_124999','Age45_54_HHI125000_149999','Age45_54_HHI150000_199999','Age45_54_HHI200000p','Age55_64_HHIlt15000','Age55_64_HHI15000_24999','Age55_64_HHI25000_34999','Age55_64_HHI35000_49999','Age55_64_HHI50000_74999','Age55_64_HHI75000_99999','Age55_64_HHI100000_124999','Age55_64_HHI125000_149999','Age55_64_HHI150000_199999','Age55_64_HHI200000p','Age65_74_HHIlt15000','Age65_74_HHI15000_24999','Age65_74_HHI25000_34999','Age65_74_HHI35000_49999','Age65_74_HHI50000_74999','Age65_74_HHI75000_99999','Age65_74_HHI100000_124999','Age65_74_HHI125000_149999','Age65_74_HHI150000_199999','Age65_74_HHI200000p','Age75_84_HHIlt15000','Age75_84_HHI15000_24999','Age75_84_HHI25000_34999','Age75_84_HHI35000_49999','Age75_84_HHI50000_74999','Age75_84_HHI75000_99999','Age75_84_HHI100000_124999','Age75_84_HHI125000_149999','Age75_84_HHI150000_199999','Age75_84_HHI200000p','Age85p_HHIlt15000','Age85p_HHI15000_24999','Age85p_HHI25000_34999','Age85p_HHI35000_49999','Age85p_HHI50000_74999','Age85p_HHI75000_99999','Age85p_HHI100000_124999','Age85p_HHI125000_149999','Age85p_HHI150000_199999','Age85p_HHI200000p']

# COMMAND ----------
age_income_data = census.select(age_inc_cols)
state_data_age_inc = age_income_data.selectExpr('StateName' ,'Division', "stack({}, {})".format(len(age_inc_cols[2:]), ', '.join(("'{}', {}".format(i, i) for i in age_inc_cols[2:]))))
state_data_age_inc = state_data_age_inc.withColumnRenamed("col0","Variable").withColumnRenamed("col1","Count")

# COMMAND ----------
state_data_age_inc = state_data_age_inc.withColumn("Variable",F.regexp_replace(F.col("Variable"),"Income","I")).withColumn("Age",F.split("Variable","_HHI").getItem(0)).withColumn("Income",F.split("Variable","_HHI").getItem(1))

# COMMAND ----------

state_data_age_inc1 = state_data_age_inc.withColumn("Age",F.regexp_replace(F.col("Age"),"Age","")).withColumn("Age",F.when(F.col("Age")=="15_24","18_24").when(F.col("Age").isin(["65_74","75_84","85p"]),"65p").otherwise(F.col("Age")))
division_data_ageinc = state_data_age_inc1.groupBy("Division","Variable","Age","Income").agg(F.sum("Count").alias("Count"))
census_div_tot_ageinc =  division_data_ageinc.groupBy("Division").agg(F.sum("Count").alias("div_total"))
census_div_data_ageinc = census_div_tot_ageinc.join(division_data_ageinc, on="Division", how="inner")

# COMMAND ----------

display(census_div_data_ageinc)

# COMMAND ----------

import pyspark.sql.functions as F
import datetime as dt

# COMMAND ----------

# MAGIC %md #### Reading TSP Data

# COMMAND ----------

tsp_data = spark.read.table("mcdonalds_prod_coremodel.dim_tsp_demographic_data")
tsp_metadata = spark.read.table("mcdonalds_prod_coremodel.dim_demographic_metadata")

# COMMAND ----------

print("Data Dimensions")
print("Columns: "+str(len(tsp_data.columns)))
print("Rows: "+str(tsp_data.count()))
print("Core IDs: "+str(tsp_data.select("individual_identity_key").distinct().count()))
# COMMAND ----------

# MAGIC %md #### Factors to Balance

# COMMAND ----------

features = ["gender_id", "advantage_individual_age_nbr", "advantage_target_income_3_0_code", "ethnic_group_code"]
id_cols = ["individual_identity_key", "state_abbreviation_code"]
tsp_sub = tsp_data.select(id_cols + features)
# COMMAND ----------

# MAGIC %md ### EDA

# MAGIC %md #### Data Pre-Processing

# COMMAND ----------

## Decoding Coded Features
for c in features:
    if c!="advantage_individual_age_nbr":
        tsp_meta_sub = tsp_metadata.filter(F.col("demographic_metadata_group_name")==c)\
                                   .withColumnRenamed("demographic_metadata_value_code",c).withColumnRenamed("demographic_metadata_value_desc","Value").select(c,"Value")
        tsp_sub = tsp_sub.join(tsp_meta_sub, on=c, how="inner").drop(c).withColumnRenamed("Value",c)

## Regrouping Age
c = "advantage_individual_age_nbr"
tsp_sub = tsp_sub.withColumn(c,F.when(F.col(c)<18,"< 18").when(F.col(c)<=20,"18_20").when(F.col(c)<=24,"21_24").when(F.col(c)<=34,"25_34").when(F.col(c)<=44,"35_44").when(F.col(c)<=54,"45_54").when(F.col(c)<=64,"55_64").when(F.col(c)<=74,"65_74").when(F.col(c)<=84,"75_84").otherwise("85p"))

## Expanding State Abbreviations
c = "state_abbreviation_code"
tsp_meta_sub = tsp_metadata.filter(F.col("demographic_metadata_group_name")==c)\
                           .withColumnRenamed("demographic_metadata_value_code",c)\
                           .withColumnRenamed("demographic_metadata_value_desc","Value").select(c,"Value")
tsp_sub = tsp_sub.join(tsp_meta_sub, on=c, how="inner").drop(c).withColumnRenamed("Value",c)

# COMMAND ----------

# MAGIC %md #### Data Re-Grouping

# COMMAND ----------
tsp_income = tsp_data.select("individual_identity_key", "advantage_target_narrow_band_income_3_0_code").join(tsp_sub.select("individual_identity_key","advantage_target_income_3_0_code"), on="individual_identity_key", how="inner")
c = "advantage_target_narrow_band_income_3_0_code"
tsp_meta_sub = tsp_metadata.filter(F.col("demographic_metadata_group_name")==c)\
                           .withColumnRenamed("demographic_metadata_value_code",c)\
                           .withColumnRenamed("demographic_metadata_value_desc","Value").select(c,"Value")
tsp_income = tsp_income.join(tsp_meta_sub, on=c, how="inner").drop(c).withColumnRenamed("Value",c)

tsp_income = tsp_income.withColumn("advantage_target_income_3_0_code", F.when(F.col("advantage_target_income_3_0_code")=="$15,000 - $19,999", F.col("advantage_target_narrow_band_income_3_0_code")).when(F.col("advantage_target_income_3_0_code")=="$20,000 - $29,999", F.col("advantage_target_narrow_band_income_3_0_code")).when(F.col("advantage_target_income_3_0_code")=="$30,000 - $39,999", F.col("advantage_target_narrow_band_income_3_0_code")).when(F.col("advantage_target_income_3_0_code")=="$40,000 - $49,999", F.col("advantage_target_narrow_band_income_3_0_code")).otherwise(F.col("advantage_target_income_3_0_code")))

tsp_income = tsp_income.withColumn("advantage_target_income_3_0_code", F.when(F.col("advantage_target_income_3_0_code")=="$ 0 - $ 14,999", F.lit("lt15000")).when(F.col("advantage_target_income_3_0_code").isin(["$15,000 - $19,999", "$20,000 - $24,999"]), F.lit("15000_24999")).when(F.col("advantage_target_income_3_0_code").isin(["$25,000 - $29,999", "$30,000 - $34,999"]), F.lit("25000_34999")).when(F.col("advantage_target_income_3_0_code").isin(["$35,000 - $39,999", "$40,000 - $44,999", "$45,000 - $49,999"]), F.lit("35000_49999")).when(F.col("advantage_target_income_3_0_code")=="$50,000 - $74,999", F.lit("50000_74999")).when(F.col("advantage_target_income_3_0_code")=="$75,000 - $99,999", F.lit("75000_99999")).when(F.col("advantage_target_income_3_0_code")=="$100,000 - $124,999", F.lit("100000_124999")).when(F.col("advantage_target_income_3_0_code")=="$125,000 - $149,999", F.lit("125000_149999")).when(F.col("advantage_target_income_3_0_code").isin(["$150,000 - $174,999", "$175,000 - 199,999"]), F.lit("150000_199999")).otherwise(F.lit("200000p")))

# COMMAND ----------

#tsp_income.groupBy("advantage_target_income_3_0_code").count().show(truncate=False)

# COMMAND ----------
tsp_base =  tsp_sub.drop("advantage_target_income_3_0_code").join(tsp_income.select("individual_identity_key","advantage_target_income_3_0_code"), on="individual_identity_key", how="inner")
tsp_input = tsp_base.withColumn("ethnic_group_code",F.when(F.col('ethnic_group_code')=='Native American', 'American Indian/Alaskan Native')\
                    .when(F.col('ethnic_group_code').isin(['Southeast Asia', 'Far Eastern', 'Central and Southwest Asia', 'Middle Eastern']), 'Asian')\
                    .when(F.col('ethnic_group_code')=='African American', 'Black/African American')\
                    .when(F.col('ethnic_group_code')=='Hispanic', 'Hispanic/Latino')\
                    .when(F.col('ethnic_group_code')=='Polynesian', 'Native Hawaiian/Pacific Islander')\
                    .when(F.col('ethnic_group_code')=='Other Groups', 'Others')\
                    .when(F.col('ethnic_group_code').isin(['Western European', 'Eastern European', 'Jewish', 'Mediterranean', 'Scandinavian']), 'White')\
                    .otherwise("Unknown"))
# COMMAND ----------

# MAGIC %md QC

# COMMAND ----------
tsp_input = tsp_input.withColumnRenamed("state_abbreviation_code","StateName").join(division_mapping, on="StateName", how="left")
print("Processed Data Dimensions")
print("Columns: "+str(len(tsp_input.columns)))
print("Rows: "+str(tsp_input.count()))
print("Core IDs: "+str(tsp_input.select("individual_identity_key").distinct().count()))

# COMMAND ----------

# MAGIC %md ## TSP Data Balancing
import pyspark.sql.functions as F
import datetime as dt

def univariate_distribution_tsp(df,col,geo):
    df1 = df.groupBy(geo,F.lit(col).alias("Feature"),F.col(col).alias("Value")).count().orderBy([geo,"Value"]).withColumnRenamed("count","tsp_count")
    geo_tot = df.groupBy(geo).count().withColumnRenamed("count","geo_total")
    df2 = df1.join(geo_tot,on=geo,how="inner").withColumn("tsp_ratio",F.col("tsp_count")/F.col("geo_total")).drop("geo_total")
    return df2
def univariate_distribution_census(df,col,geo):
    df1 = df.groupBy(geo,F.lit(col).alias("Feature"),F.col(col).alias("Value")).agg(F.sum("census_count").alias("census_count")).orderBy([geo,"Value"])
    geo_tot = df1.groupBy(geo).agg(F.sum("census_count").alias("geo_total"))
    df2 = df1.join(geo_tot,on=geo,how="inner").withColumn("census_ratio",F.col("census_count")/F.col("geo_total")).drop("geo_total")
    return df2

tsp_input = tsp_input.withColumnRenamed("advantage_individual_age_nbr", "Age").withColumnRenamed("gender_id", "Gender").withColumnRenamed("ethnic_group_code", "Ethnicity").withColumnRenamed("advantage_target_income_3_0_code", "Income").filter(F.col("Ethnicity")!="Others").withColumn("Age", F.when(F.col("Age").isin(["65_74", "75_84", "85p"]),"65p").when(F.col("Age").isin(["18_20", "21_24"]), "18_24").otherwise(F.col("Age"))).withColumn("Ethnicity1",F.when(F.col("Ethnicity").isin(["American Indian/Alaskan Native", "Native Hawaiian/Pacific Islander"]), "Misc").otherwise(F.col("Ethnicity")))#.withColumn("Income1", F.when(F.col("Income").isin(["lt15000","15000_24999","25000_34999","35000_49999","50000_74999"]),"lt75000").when(F.col("Income").isin(["75000_99999", "100000_124999", "125000_149999"]), "75000_150000").otherwise("150000p"))

census_div_data = census_div_data.drop("div_total").filter(F.col("Ethnicity")!="Others").withColumn("Age", F.when(F.col("Age").isin(["18_20", "21_24"]), "18_24").otherwise(F.col("Age"))).withColumn("Ethnicity1",F.when(F.col("Ethnicity").isin(["American Indian/Alaskan Native", "Native Hawaiian/Pacific Islander"]), "Misc").otherwise(F.col("Ethnicity")))

census_div_data_inc = census_div_data_inc.drop("div_total").filter(F.col("Ethnicity")!="Others").withColumn("Ethnicity1", F.when(F.col("Ethnicity").isin(["American Indian/Alaskan Native", "Native Hawaiian/Pacific Islander"]), "Misc").otherwise(F.col("Ethnicity")))#.withColumn("Income1", F.when(F.col("Income").isin(["lt15000","15000_24999","25000_34999","35000_49999","50000_74999"]),"lt75000").when(F.col("Income").isin(["75000_99999", "100000_124999", "125000_149999"]), "75000_150000").otherwise("150000p"))

tsp_input = tsp_input.withColumn("Ethnicity",F.col("Ethnicity1"))#\
                     #.withColumn("Income",F.col("Income1"))
census_div_data = census_div_data.withColumn("Ethnicity",F.col("Ethnicity1"))
census_div_data_inc = census_div_data_inc.withColumn("Ethnicity",F.col("Ethnicity1"))#\
                                         #.withColumn("Income",F.col("Income1"))

join_cols = ["Division", "Gender", "Age", "Ethnicity"]
tsp_div_crosstab = tsp_input.groupBy(join_cols).count().withColumnRenamed("count","tsp_count")
census_div_crosstab = census_div_data.groupBy(join_cols).agg(F.sum("Count").alias("census_count"))
tsp_census_divdata = census_div_crosstab.select(join_cols+["census_count"]).join(tsp_div_crosstab.select(join_cols+["tsp_count"]), on=join_cols, how="inner")
totals = tsp_census_divdata.select(F.sum("census_count").alias("census_tot"),F.sum("tsp_count").alias("tsp_tot"))
tsp_census_divdata_final = tsp_census_divdata.join(totals).withColumn("census_ratio", F.col("census_count")/F.col("census_tot"))\
                                                          .withColumn("tsp_ratio", F.col("tsp_count")/F.col("tsp_tot"))\
                                                          .withColumn("difference",F.col("tsp_ratio")/F.col("census_ratio") - 1)\
                                                          .withColumn("projected_sample",F.round(F.col("tsp_count")/F.col("census_ratio"),0))

#tsp_inc1 = tsp_input.groupBy("Division","Ethnicity","Income").count().withColumnRenamed("count","tsp_count")
#census_div_inc_crosstab1 = census_div_data_inc.groupBy("Division","Ethnicity","Income").agg(F.sum("Count").alias("census_count"))
#tsp_census_divdata_inc1 = census_div_inc_crosstab1.join(tsp_inc1, on=["Division","Ethnicity","Income"], how="inner")

expected_sample_size = 39000000

sampling_factor = tsp_census_divdata_final.withColumn("Key", F.concat(F.col("Division"),F.lit("-"),F.col("Gender"),F.lit("-"),F.col("Age"),F.lit("-"),F.col("Ethnicity")))\
                                          .withColumn("Factor", F.when(F.col("projected_sample")<=expected_sample_size,1)\
                                                                 .when(F.col("Age")=="18_24",1)\
                                                                 .otherwise(F.col("census_ratio")*expected_sample_size/F.col("tsp_count")))
sampling_dict = {row['Key']:row['Factor'] for row in sampling_factor.collect()}

tsp_l1_resampled = tsp_input.withColumn("Key", F.concat(F.col("Division"),F.lit("-"),F.col("Gender"),F.lit("-"),F.col("Age"),F.lit("-"),F.col("Ethnicity")))\
                            .sampleBy("Key", fractions=sampling_dict, seed=12345)
print(tsp_l1_resampled.count())

# COMMAND ----------

tsp_l1_resampled_iid = tsp_l1_resampled.select('individual_identity_key')

today = date.today()

#tablename = "mcdonalds_prod_audience_xfer.epc_mcd_" + today.strftime('%Y%m%d') + "_tsp_census_normalized_sample"
#tsp_l1_resampled_iid.write.format("delta").mode("overwrite").option("header","true").saveAsTable(tablename)
#print(tablename)

# COMMAND ----------

# DBTITLE 1,TSP data subset for normalized sample - TSP_census_normalized
tsp_census_normaized = tsp_l1_resampled.select('individual_identity_key').join(tsp_data, 'individual_identity_key', 'inner')

today = date.today()
tsp_census_normaized_1 = tsp_census_normaized.select(lit(today).alias('refresh_date'), "*")

print(tsp_census_normaized_1.select('individual_identity_key').distinct().count())

tablename1 = "mcdonalds_prod_public_works.epc_mcd_" + today.strftime('%Y%m%d') + "_tsp_census_normalized_sample"
tsp_census_normaized_1.write.format("delta").mode("overwrite").option("header","true").saveAsTable(tablename1)
print(tablename1)

# COMMAND ----------

######################################

# COMMAND ----------

# Summary Tables 1 - Division-Gender-Age-Ethnicity-Census Count-TSP Count-Sampled TSP
join_cols = ["Division", "Gender", "Age", "Ethnicity"]

tsp_summary = tsp_input.groupBy(join_cols).count().withColumnRenamed("count","TSP_Count")
census_summary = census_div_data.groupBy(join_cols).agg(F.sum("Count").alias("Census_Count"))
resampled_tsp_summary = tsp_l1_resampled.groupBy(join_cols).count().withColumnRenamed("count","Sampled_TSP_Count")

tsp_census_resampled_summary = census_summary.select(join_cols+["Census_Count"]).join(tsp_summary.select(join_cols+["TSP_Count"]), on=join_cols, how="inner").join(resampled_tsp_summary.select(join_cols+["Sampled_TSP_Count"]), on=join_cols, how="inner")

tsp_census_resampled_summary.display()

tb_name1 = "mcdonalds_prod_public_works.tsp_resampled_division_gender_age_ethnicity_summary"+ today.strftime('%Y%m%d')
tsp_census_resampled_summary.write.format("delta").mode("overwrite").option("header","true").saveAsTable(tb_name1)

# COMMAND ----------

# Summary Tables 2 - Division-Ethnicity-Income-Census Count-TSP Count-Sampled TSP
join_cols = ["Division", "Ethnicity", "Income"]

tsp_summary2 = tsp_input.groupBy(join_cols).count().withColumnRenamed("count","TSP_Count")
census_summary2 = census_div_data_inc.groupBy(join_cols).agg(F.sum("Count").alias("Census_Count"))
resampled_tsp_summary2 = tsp_l1_resampled.groupBy(join_cols).count().withColumnRenamed("count","Sampled_TSP_Count")

tsp_census_resampled_summary2 = census_summary2.select(join_cols+["Census_Count"]).join(tsp_summary2.select(join_cols+["TSP_Count"]), on=join_cols, how="inner").join(resampled_tsp_summary2.select(join_cols+["Sampled_TSP_Count"]), on=join_cols, how="inner")

tb_name2 = "mcdonalds_prod_public_works.tsp_resampled_division_ethnicity_income_summary"+ today.strftime('%Y%m%d')
tsp_census_resampled_summary2.write.format("delta").mode("overwrite").option("header","true").saveAsTable(tb_name2)

tsp_census_resampled_summary2.display()

# COMMAND ----------

# Summary Tables 3 - Overall Division-Income-TSP Ratio-Census Ratio-Sampled TSP Ratio

division_data_inc = state_data_inc.groupBy("Division","Variable","Ethnicity","Income").agg(F.sum("Count").alias("Count"))
census_div_tot_inc =  division_data_inc.groupBy("Division").agg(F.sum("Count").alias("div_total"))
census_div_data_inc = census_div_tot_inc.join(division_data_inc, on="Division", how="inner")

join_cols = ["Income"]

tsp_summary3 = tsp_input.groupBy(join_cols).count().withColumnRenamed("count","TSP_Count")
tsp_summary3 = tsp_summary3.crossJoin(tsp_summary3.groupBy().agg(F.sum('TSP_Count').alias('sum_tsp_count'))).select('Income', round(((F.col('TSP_Count')/F.col('sum_tsp_count'))*100),2).alias('TSP_Ratio'))

census_summary3 = census_div_data_inc.groupBy(join_cols).agg(F.sum("Count").alias("Census_Count"))
census_summary3 = census_summary3.crossJoin(census_summary3.groupBy().agg(F.sum('Census_Count').alias('sum_census_count'))).select('Income', round(((F.col('Census_Count')/F.col('sum_census_count'))*100),2).alias('Census_Ratio'))

resampled_tsp_summary3 = tsp_l1_resampled.groupBy(join_cols).count().withColumnRenamed("count","Sampled_TSP_Count")
resampled_tsp_summary3 = resampled_tsp_summary3.crossJoin(resampled_tsp_summary3.groupBy().agg(F.sum('Sampled_TSP_Count').alias('sum_sampled_count'))).select('Income', round(((F.col('Sampled_TSP_Count')/F.col('sum_sampled_count'))*100),2).alias('Sampled_TSP_Ratio'))

tsp_census_resampled_summary3 = census_summary3.select(join_cols+["Census_Ratio"]).join(tsp_summary3.select(join_cols+["TSP_Ratio"]), on=join_cols, how="inner").join(resampled_tsp_summary3.select(join_cols+["Sampled_TSP_Ratio"]), on=join_cols, how="inner")
tsp_census_resampled_summary3 = tsp_census_resampled_summary3.select(F.lit('Overall').alias('Division'), 'Income', 'Census_Ratio', 'TSP_Ratio', 'Sampled_TSP_Ratio')
tsp_census_resampled_summary3.display()

# COMMAND ----------

# Summary Tables 4 - East North Central-Income-TSP Ratio-Census Ratio-Sampled TSP Ratio
join_cols = ["Income"]

tsp_summary4 = tsp_input.filter(col('Division')=='East North Central').groupBy(join_cols).count().withColumnRenamed("count","TSP_Count")
tsp_summary4 = tsp_summary4.crossJoin(tsp_summary4.groupBy().agg(F.sum('TSP_Count').alias('sum_tsp_count'))).select('Income', round(((F.col('TSP_Count')/F.col('sum_tsp_count'))*100),2).alias('TSP_Ratio'))

census_summary4 = census_div_data_inc.filter(col('Division')=='East North Central').groupBy(join_cols).agg(F.sum("Count").alias("Census_Count"))
census_summary4 = census_summary4.crossJoin(census_summary4.groupBy().agg(F.sum('Census_Count').alias('sum_census_count'))).select('Income', round(((F.col('Census_Count')/F.col('sum_census_count'))*100),2).alias('Census_Ratio'))

resampled_tsp_summary4 = tsp_l1_resampled.filter(col('Division')=='East North Central').groupBy(join_cols).count().withColumnRenamed("count","Sampled_TSP_Count")
resampled_tsp_summary4 = resampled_tsp_summary4.crossJoin(resampled_tsp_summary4.groupBy().agg(F.sum('Sampled_TSP_Count').alias('sum_sampled_count'))).select('Income', round(((F.col('Sampled_TSP_Count')/F.col('sum_sampled_count'))*100),2).alias('Sampled_TSP_Ratio'))

tsp_census_resampled_summary4 = census_summary4.select(join_cols+["Census_Ratio"]).join(tsp_summary4.select(join_cols+["TSP_Ratio"]), on=join_cols, how="inner").join(resampled_tsp_summary4.select(join_cols+["Sampled_TSP_Ratio"]), on=join_cols, how="inner")
tsp_census_resampled_summary4 = tsp_census_resampled_summary4.select(F.lit('East North Central').alias('Division'), 'Income', 'Census_Ratio', 'TSP_Ratio', 'Sampled_TSP_Ratio')
tsp_census_resampled_summary4.display()


# COMMAND ----------

# Summary Tables 5 - East South Central-Income-TSP Ratio-Census Ratio-Sampled TSP Ratio

join_cols = ["Income"]

tsp_summary5 = tsp_input.filter(col('Division')=='East South Central').groupBy(join_cols).count().withColumnRenamed("count","TSP_Count")
tsp_summary5 = tsp_summary5.crossJoin(tsp_summary5.groupBy().agg(F.sum('TSP_Count').alias('sum_tsp_count'))).select('Income', round(((F.col('TSP_Count')/F.col('sum_tsp_count'))*100),2).alias('TSP_Ratio'))

census_summary5 = census_div_data_inc.filter(col('Division')=='East South Central').groupBy(join_cols).agg(F.sum("Count").alias("Census_Count"))
census_summary5 = census_summary5.crossJoin(census_summary5.groupBy().agg(F.sum('Census_Count').alias('sum_census_count'))).select('Income', round(((F.col('Census_Count')/F.col('sum_census_count'))*100),2).alias('Census_Ratio'))

resampled_tsp_summary5 = tsp_l1_resampled.filter(col('Division')=='East South Central').groupBy(join_cols).count().withColumnRenamed("count","Sampled_TSP_Count")
resampled_tsp_summary5 = resampled_tsp_summary5.crossJoin(resampled_tsp_summary5.groupBy().agg(F.sum('Sampled_TSP_Count').alias('sum_sampled_count'))).select('Income', round(((F.col('Sampled_TSP_Count')/F.col('sum_sampled_count'))*100),2).alias('Sampled_TSP_Ratio'))

tsp_census_resampled_summary5 = census_summary5.select(join_cols+["Census_Ratio"]).join(tsp_summary5.select(join_cols+["TSP_Ratio"]), on=join_cols, how="inner").join(resampled_tsp_summary5.select(join_cols+["Sampled_TSP_Ratio"]), on=join_cols, how="inner")
tsp_census_resampled_summary5 = tsp_census_resampled_summary5.select(F.lit('East South Central').alias('Division'), 'Income', 'Census_Ratio', 'TSP_Ratio', 'Sampled_TSP_Ratio')
tsp_census_resampled_summary5.display()


# COMMAND ----------

# Summary Tables 6 - Middle Atlantic-Income-TSP Ratio-Census Ratio-Sampled TSP Ratio

join_cols = ["Income"]

tsp_summary6 = tsp_input.filter(col('Division')=='Middle Atlantic').groupBy(join_cols).count().withColumnRenamed("count","TSP_Count")
tsp_summary6 = tsp_summary6.crossJoin(tsp_summary6.groupBy().agg(F.sum('TSP_Count').alias('sum_tsp_count'))).select('Income', round(((F.col('TSP_Count')/F.col('sum_tsp_count'))*100),2).alias('TSP_Ratio'))

census_summary6 = census_div_data_inc.filter(col('Division')=='Middle Atlantic').groupBy(join_cols).agg(F.sum("Count").alias("Census_Count"))
census_summary6 = census_summary6.crossJoin(census_summary6.groupBy().agg(F.sum('Census_Count').alias('sum_census_count'))).select('Income', round(((F.col('Census_Count')/F.col('sum_census_count'))*100),2).alias('Census_Ratio'))

resampled_tsp_summary6 = tsp_l1_resampled.filter(col('Division')=='Middle Atlantic').groupBy(join_cols).count().withColumnRenamed("count","Sampled_TSP_Count")
resampled_tsp_summary6 = resampled_tsp_summary6.crossJoin(resampled_tsp_summary6.groupBy().agg(F.sum('Sampled_TSP_Count').alias('sum_sampled_count'))).select('Income', round(((F.col('Sampled_TSP_Count')/F.col('sum_sampled_count'))*100),2).alias('Sampled_TSP_Ratio'))

tsp_census_resampled_summary6 = census_summary6.select(join_cols+["Census_Ratio"]).join(tsp_summary6.select(join_cols+["TSP_Ratio"]), on=join_cols, how="inner").join(resampled_tsp_summary6.select(join_cols+["Sampled_TSP_Ratio"]), on=join_cols, how="inner")
tsp_census_resampled_summary6 = tsp_census_resampled_summary6.select(F.lit('Middle Atlantic').alias('Division'), 'Income', 'Census_Ratio', 'TSP_Ratio', 'Sampled_TSP_Ratio')
tsp_census_resampled_summary6.display()


# COMMAND ----------

# Summary Tables 7 - Mountain-Income-TSP Ratio-Census Ratio-Sampled TSP Ratio

join_cols = ["Income"]

tsp_summary7 = tsp_input.filter(col('Division')=='Mountain').groupBy(join_cols).count().withColumnRenamed("count","TSP_Count")
tsp_summary7 = tsp_summary7.crossJoin(tsp_summary7.groupBy().agg(F.sum('TSP_Count').alias('sum_tsp_count'))).select('Income', round(((F.col('TSP_Count')/F.col('sum_tsp_count'))*100),2).alias('TSP_Ratio'))

census_summary7 = census_div_data_inc.filter(col('Division')=='Mountain').groupBy(join_cols).agg(F.sum("Count").alias("Census_Count"))
census_summary7 = census_summary7.crossJoin(census_summary7.groupBy().agg(F.sum('Census_Count').alias('sum_census_count'))).select('Income', round(((F.col('Census_Count')/F.col('sum_census_count'))*100),2).alias('Census_Ratio'))

resampled_tsp_summary7 = tsp_l1_resampled.filter(col('Division')=='Mountain').groupBy(join_cols).count().withColumnRenamed("count","Sampled_TSP_Count")
resampled_tsp_summary7 = resampled_tsp_summary7.crossJoin(resampled_tsp_summary7.groupBy().agg(F.sum('Sampled_TSP_Count').alias('sum_sampled_count'))).select('Income', round(((F.col('Sampled_TSP_Count')/F.col('sum_sampled_count'))*100),2).alias('Sampled_TSP_Ratio'))

tsp_census_resampled_summary7 = census_summary7.select(join_cols+["Census_Ratio"]).join(tsp_summary7.select(join_cols+["TSP_Ratio"]), on=join_cols, how="inner").join(resampled_tsp_summary7.select(join_cols+["Sampled_TSP_Ratio"]), on=join_cols, how="inner")
tsp_census_resampled_summary7 = tsp_census_resampled_summary7.select(F.lit('Mountain').alias('Division'), 'Income', 'Census_Ratio', 'TSP_Ratio', 'Sampled_TSP_Ratio')
tsp_census_resampled_summary7.display()

# COMMAND ----------

# Summary Tables 8 - New England-Income-TSP Ratio-Census Ratio-Sampled TSP Ratio

join_cols = ["Income"]

tsp_summary8 = tsp_input.filter(col('Division')=='New England').groupBy(join_cols).count().withColumnRenamed("count","TSP_Count")
tsp_summary8 = tsp_summary8.crossJoin(tsp_summary8.groupBy().agg(F.sum('TSP_Count').alias('sum_tsp_count'))).select('Income', round(((F.col('TSP_Count')/F.col('sum_tsp_count'))*100),2).alias('TSP_Ratio'))

census_summary8 = census_div_data_inc.filter(col('Division')=='New England').groupBy(join_cols).agg(F.sum("Count").alias("Census_Count"))
census_summary8 = census_summary8.crossJoin(census_summary8.groupBy().agg(F.sum('Census_Count').alias('sum_census_count'))).select('Income', round(((F.col('Census_Count')/F.col('sum_census_count'))*100),2).alias('Census_Ratio'))

resampled_tsp_summary8 = tsp_l1_resampled.filter(col('Division')=='New England').groupBy(join_cols).count().withColumnRenamed("count","Sampled_TSP_Count")
resampled_tsp_summary8 = resampled_tsp_summary8.crossJoin(resampled_tsp_summary8.groupBy().agg(F.sum('Sampled_TSP_Count').alias('sum_sampled_count'))).select('Income', round(((F.col('Sampled_TSP_Count')/F.col('sum_sampled_count'))*100),2).alias('Sampled_TSP_Ratio'))

tsp_census_resampled_summary8 = census_summary8.select(join_cols+["Census_Ratio"]).join(tsp_summary8.select(join_cols+["TSP_Ratio"]), on=join_cols, how="inner").join(resampled_tsp_summary8.select(join_cols+["Sampled_TSP_Ratio"]), on=join_cols, how="inner")
tsp_census_resampled_summary8 = tsp_census_resampled_summary8.select(F.lit('New England').alias('Division'), 'Income', 'Census_Ratio', 'TSP_Ratio', 'Sampled_TSP_Ratio')
tsp_census_resampled_summary8.display()

# COMMAND ----------

# Summary Tables 9 - Pacific-Income-TSP Ratio-Census Ratio-Sampled TSP Ratio

join_cols = ["Income"]

tsp_summary9 = tsp_input.filter(col('Division')=='Pacific').groupBy(join_cols).count().withColumnRenamed("count","TSP_Count")
tsp_summary9 = tsp_summary9.crossJoin(tsp_summary9.groupBy().agg(F.sum('TSP_Count').alias('sum_tsp_count'))).select('Income', round(((F.col('TSP_Count')/F.col('sum_tsp_count'))*100),2).alias('TSP_Ratio'))

census_summary9 = census_div_data_inc.filter(col('Division')=='Pacific').groupBy(join_cols).agg(F.sum("Count").alias("Census_Count"))
census_summary9 = census_summary9.crossJoin(census_summary9.groupBy().agg(F.sum('Census_Count').alias('sum_census_count'))).select('Income', round(((F.col('Census_Count')/F.col('sum_census_count'))*100),2).alias('Census_Ratio'))

resampled_tsp_summary9 = tsp_l1_resampled.filter(col('Division')=='Pacific').groupBy(join_cols).count().withColumnRenamed("count","Sampled_TSP_Count")
resampled_tsp_summary9 = resampled_tsp_summary9.crossJoin(resampled_tsp_summary9.groupBy().agg(F.sum('Sampled_TSP_Count').alias('sum_sampled_count'))).select('Income', round(((F.col('Sampled_TSP_Count')/F.col('sum_sampled_count'))*100),2).alias('Sampled_TSP_Ratio'))

tsp_census_resampled_summary9 = census_summary9.select(join_cols+["Census_Ratio"]).join(tsp_summary9.select(join_cols+["TSP_Ratio"]), on=join_cols, how="inner").join(resampled_tsp_summary9.select(join_cols+["Sampled_TSP_Ratio"]), on=join_cols, how="inner")
tsp_census_resampled_summary9 = tsp_census_resampled_summary9.select(F.lit('Pacific').alias('Division'), 'Income', 'Census_Ratio', 'TSP_Ratio', 'Sampled_TSP_Ratio')
tsp_census_resampled_summary9.display()

# COMMAND ----------

# Summary Tables 10 - South Atlantic-Income-TSP Ratio-Census Ratio-Sampled TSP Ratio

join_cols = ["Income"]

tsp_summary10 = tsp_input.filter(col('Division')=='South Atlantic').groupBy(join_cols).count().withColumnRenamed("count","TSP_Count")
tsp_summary10 = tsp_summary10.crossJoin(tsp_summary10.groupBy().agg(F.sum('TSP_Count').alias('sum_tsp_count'))).select('Income', round(((F.col('TSP_Count')/F.col('sum_tsp_count'))*100),2).alias('TSP_Ratio'))

census_summary10 = census_div_data_inc.filter(col('Division')=='South Atlantic').groupBy(join_cols).agg(F.sum("Count").alias("Census_Count"))
census_summary10 = census_summary10.crossJoin(census_summary10.groupBy().agg(F.sum('Census_Count').alias('sum_census_count'))).select('Income', round(((F.col('Census_Count')/F.col('sum_census_count'))*100),2).alias('Census_Ratio'))

resampled_tsp_summary10 = tsp_l1_resampled.filter(col('Division')=='South Atlantic').groupBy(join_cols).count().withColumnRenamed("count","Sampled_TSP_Count")
resampled_tsp_summary10 = resampled_tsp_summary10.crossJoin(resampled_tsp_summary10.groupBy().agg(F.sum('Sampled_TSP_Count').alias('sum_sampled_count'))).select('Income', round(((F.col('Sampled_TSP_Count')/F.col('sum_sampled_count'))*100),2).alias('Sampled_TSP_Ratio'))

tsp_census_resampled_summary10 = census_summary10.select(join_cols+["Census_Ratio"]).join(tsp_summary10.select(join_cols+["TSP_Ratio"]), on=join_cols, how="inner").join(resampled_tsp_summary10.select(join_cols+["Sampled_TSP_Ratio"]), on=join_cols, how="inner")
tsp_census_resampled_summary10 = tsp_census_resampled_summary10.select(F.lit('South Atlantic').alias('Division'), 'Income', 'Census_Ratio', 'TSP_Ratio', 'Sampled_TSP_Ratio')
tsp_census_resampled_summary10.display()

# COMMAND ----------

# Summary Tables 11 - West North Central-Income-TSP Ratio-Census Ratio-Sampled TSP Ratio
join_cols = ["Income"]

tsp_summary11 = tsp_input.filter(col('Division')=='West North Central').groupBy(join_cols).count().withColumnRenamed("count","TSP_Count")
tsp_summary11 = tsp_summary11.crossJoin(tsp_summary11.groupBy().agg(F.sum('TSP_Count').alias('sum_tsp_count'))).select('Income', round(((F.col('TSP_Count')/F.col('sum_tsp_count'))*100),2).alias('TSP_Ratio'))

census_summary11 = census_div_data_inc.filter(col('Division')=='West North Central').groupBy(join_cols).agg(F.sum("Count").alias("Census_Count"))
census_summary11 = census_summary11.crossJoin(census_summary11.groupBy().agg(F.sum('Census_Count').alias('sum_census_count'))).select('Income', round(((F.col('Census_Count')/F.col('sum_census_count'))*100),2).alias('Census_Ratio'))

resampled_tsp_summary11 = tsp_l1_resampled.filter(col('Division')=='West North Central').groupBy(join_cols).count().withColumnRenamed("count","Sampled_TSP_Count")
resampled_tsp_summary11 = resampled_tsp_summary11.crossJoin(resampled_tsp_summary11.groupBy().agg(F.sum('Sampled_TSP_Count').alias('sum_sampled_count'))).select('Income', round(((F.col('Sampled_TSP_Count')/F.col('sum_sampled_count'))*100),2).alias('Sampled_TSP_Ratio'))

tsp_census_resampled_summary11 = census_summary11.select(join_cols+["Census_Ratio"]).join(tsp_summary11.select(join_cols+["TSP_Ratio"]), on=join_cols, how="inner").join(resampled_tsp_summary11.select(join_cols+["Sampled_TSP_Ratio"]), on=join_cols, how="inner")
tsp_census_resampled_summary11 = tsp_census_resampled_summary11.select(F.lit('West North Central').alias('Division'), 'Income', 'Census_Ratio', 'TSP_Ratio', 'Sampled_TSP_Ratio')
tsp_census_resampled_summary11.display()

# COMMAND ----------

# Summary Tables 12 - West South Central-Income-TSP Ratio-Census Ratio-Sampled TSP Ratio
join_cols = ["Income"]

tsp_summary12 = tsp_input.filter(col('Division')=='West South Central').groupBy(join_cols).count().withColumnRenamed("count","TSP_Count")
tsp_summary12 = tsp_summary12.crossJoin(tsp_summary12.groupBy().agg(F.sum('TSP_Count').alias('sum_tsp_count'))).select('Income', round(((F.col('TSP_Count')/F.col('sum_tsp_count'))*100),2).alias('TSP_Ratio'))

census_summary12 = census_div_data_inc.filter(col('Division')=='West South Central').groupBy(join_cols).agg(F.sum("Count").alias("Census_Count"))
census_summary12 = census_summary12.crossJoin(census_summary12.groupBy().agg(F.sum('Census_Count').alias('sum_census_count'))).select('Income', round(((F.col('Census_Count')/F.col('sum_census_count'))*100),2).alias('Census_Ratio'))

resampled_tsp_summary12 = tsp_l1_resampled.filter(col('Division')=='West South Central').groupBy(join_cols).count().withColumnRenamed("count","Sampled_TSP_Count")
resampled_tsp_summary12 = resampled_tsp_summary12.crossJoin(resampled_tsp_summary12.groupBy().agg(F.sum('Sampled_TSP_Count').alias('sum_sampled_count'))).select('Income', round(((F.col('Sampled_TSP_Count')/F.col('sum_sampled_count'))*100),2).alias('Sampled_TSP_Ratio'))

tsp_census_resampled_summary12 = census_summary12.select(join_cols+["Census_Ratio"]).join(tsp_summary12.select(join_cols+["TSP_Ratio"]), on=join_cols, how="inner").join(resampled_tsp_summary12.select(join_cols+["Sampled_TSP_Ratio"]), on=join_cols, how="inner")
tsp_census_resampled_summary12 = tsp_census_resampled_summary12.select(F.lit('West South Central').alias('Division'), 'Income', 'Census_Ratio', 'TSP_Ratio', 'Sampled_TSP_Ratio')
tsp_census_resampled_summary12.display()

# COMMAND ----------

# Division level income summary
tsp_census_resampled_summary13 = tsp_census_resampled_summary3.union(tsp_census_resampled_summary4).union(tsp_census_resampled_summary5).union(tsp_census_resampled_summary6).union(tsp_census_resampled_summary7).union(tsp_census_resampled_summary8).union(tsp_census_resampled_summary9).union(tsp_census_resampled_summary10).union(tsp_census_resampled_summary11).union(tsp_census_resampled_summary12)
tsp_census_resampled_summary13.display()

tb_name = "mcdonalds_prod_public_works.tsp_resamples_division_summary"+ today.strftime('%Y%m%d')
tsp_census_resampled_summary13.write.format("delta").mode("overwrite").option("header","true").saveAsTable(tb_name)

# COMMAND ----------

# DBTITLE 1,Audience push to Discovery
tsp_l1_resampled_iid = tsp_l1_resampled_iid.withColumn('tablename', lit('tsp_census_normalized_sample'))

def clean_column_spark(col):
    return F.lower(F.regexp_replace( F.regexp_replace( col, r"[\s-]", "_" ),  r"\W", ""))

tsp_l1_resampled_iid = tsp_l1_resampled_iid.withColumn('attr_clean', clean_column_spark(F.col('tablename')))

attr2_list = tsp_l1_resampled_iid.select('attr_clean').distinct().rdd.flatMap(lambda x: x).collect()

file_root_name = "epc_mcd"
id_key = 'individual_identity_key'
current_date = today.strftime('%Y%m%d')

for i in attr2_list:
        
    df = tsp_l1_resampled_iid.where(col("attr_clean") == i).select(id_key).distinct()
    df.createOrReplaceTempView("df")
        
    spark.sql(f"""  
        drop table if exists mcdonalds_prod_audience_xfer.{file_root_name}_{current_date}_{i}
    """)
        
    spark.sql(f"""  
        create external table mcdonalds_prod_audience_xfer.{file_root_name}_{current_date}_{i}
        row format delimited fields terminated by ','
        location 's3://mcdonalds-prod-us-east-1-data/analytics/audience_xfer/department/{file_root_name}_{current_date}_{i}'
        as select distinct {id_key} from df;
    """)
        
    print('Table Complete: ',[i],'-',current_date)
    
tsp_l1_resampled_iid.groupby("attr_clean").agg(F.countDistinct("individual_identity_key").alias("individuals_count")).display()

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import *
import json
import requests

#Provide list of table names to be activated. Dont include the database name. Provide only table names

current_date = today.strftime('%Y%m%d')
audience_table_names = ["epc_mcd_" + current_date + "_" + i for i in attr2_list]

# Change this to your name
user = 'Kumari Jayshree'

url = "https://d33amdmy04.execute-api.us-east-1.amazonaws.com/v1/audience/bulk-activate"

db_root = 'mcdonalds_prod_audience_xfer'

id_key = 'individual_identity_key'

# COMMAND ----------

def activate(aud_name,tab_name):
 
    
     #Enter user name that will show up in Activation UI
    #Enter root name for audience tables
    
    audience_name = aud_name
    
    table_name = tab_name

    payload = {
        "transfer_name": "Transfer_"+ audience_name,
        "platforms": [
            {
                "id": 2,
                "name": "People Cloud Discovery",
                "type": 1,
                "base_count_code": "individual",
                "derived_count_code": None,
                "cost_code": "individual",
                "info_text": "This audience will be available 2-3 days after activation",
                "calc_text": "Individual ID/1000 * 0",
                "is_universal_connector": False,
                "include_tsp": False,
                "order": 20,
                "units": 1000,
                "cost_per_unit": 0,
                "include_premium": False,
                "cas_platform_ids": [
                    37
                ]
            }
        ],
        "data_sources": [],
        "tags": [],
        "notes": "",
        "created_by": user,
        "audiences": [
            {
                "audience_name": audience_name,
                "data_file": f"{db_root}|{table_name}",
                "column_id": id_key
            }
        ]
    }

    headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}

    response = requests.post(url, data=json.dumps(payload), headers=headers)
    print(table_name)
    print(response.content)
    print(response.status_code)
    
for aud in audience_table_names:
    activate(f"{aud}",f"{aud}")

# COMMAND ----------


