# Databricks Notebook
# ---------------------------------------------------------
# Notebook 02: Clean, Transform and Prepare Silver Data
# ---------------------------------------------------------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# ---------------------------------------------
# Paths
# ---------------------------------------------
bronze_patient_path = "dbfs:/FileStore/Mini_Project/bronze/patient_bronze"
bronze_insurance_path = "dbfs:/FileStore/Mini_Project/bronze/insurance_bronze"

silver_patient_path = "dbfs:/FileStore/Mini_Project/silver/patient_silver"
silver_insurance_path = "dbfs:/FileStore/Mini_Project/silver/insurance_silver"


# ---------------------------------------------
# 1. Read Bronze Tables
# ---------------------------------------------
df_patient_raw = spark.read.format("delta").load(bronze_patient_path)
df_insurance_raw = spark.read.format("delta").load(bronze_insurance_path)

print("Bronze Patient Schema:")
df_patient_raw.printSchema()


# ---------------------------------------------
# 2. Clean + Transform Patient Data (Silver)
# ---------------------------------------------
df_patient_silver = (
    df_patient_raw
    .withColumn("patient_id", trim(col("patient_id")))
    .withColumn("name", trim(col("name")))
    .withColumn("age", col("age").cast("int"))
    .withColumn("address", trim(col("address")))
    .withColumn("phone_number", regexp_replace(trim(col("phone_number")), "\\s+", ""))
    .withColumn("bill_amount", col("bill_amount").cast("double"))
    .withColumn("insurance_provider", trim(col("insurance_provider")))
    .withColumn("policy_id", trim(col("policy_id")))

    # Split name into first_name and last_name
    .withColumn("first_name", split(col("name"), " ").getItem(0))
    .withColumn("last_name", split(col("name"), " ").getItem(1))

    # Business key (MD5)
    .withColumn("primary_key", md5(concat_ws("|", col("patient_id"), col("name"))))

    # Checksum of all changeable attributes
    .withColumn(
        "checksum_txt",
        md5(concat_ws("|",
            col("address"),
            col("phone_number"),
            col("bill_amount"),
            col("insurance_provider"),
            col("policy_id")
        ))
    )

    .withColumn("current_timestamp", current_timestamp())
    .withColumn("updated_timestamp", current_timestamp())
    .withColumn("load_ctl_key", lit(2001))
)

df_patient_silver.createOrReplaceTempView("patient_landing_cleaned")

print("Silver Patient Schema:")
df_patient_silver.printSchema()


# ---------------------------------------------
# 3. Write Silver Patient Delta Table
# ---------------------------------------------
df_patient_silver.write.mode("overwrite").format("delta").save(silver_patient_path)

spark.sql("""
    CREATE TABLE IF NOT EXISTS patient_silver
    USING DELTA
    LOCATION 'dbfs:/FileStore/Mini_Project/silver/patient_silver'
""")

print("Silver Patient Table Created Successfully.")


# ---------------------------------------------
# 4. Insurance Silver (Simple Clean)
# ---------------------------------------------
df_insurance_silver = (
    df_insurance_raw
    .withColumn("policy_id", trim(col("policy_id")))
    .withColumn("insurance_provider", trim(col("insurance_provider")))
    .withColumn("amount_covered", col("amount_covered").cast("dou
