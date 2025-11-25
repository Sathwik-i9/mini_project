# Databricks Notebook
# ---------------------------------------------------------
# Notebook 01: Ingest Raw Source Files into Bronze Layer
# ---------------------------------------------------------

from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# -----------------------------
# 1. File Paths
# -----------------------------
patient_src_path = "dbfs:/FileStore/Mini_Project/Source_data/Patient_Source.csv"
insurance_src_path = "dbfs:/FileStore/Mini_Project/Source_data/Insurance_Source.csv"

bronze_patient_path = "dbfs:/FileStore/Mini_Project/bronze/patient_bronze"
bronze_insurance_path = "dbfs:/FileStore/Mini_Project/bronze/insurance_bronze"


# -----------------------------
# 2. Read Raw Patient File
# -----------------------------
df_patient_raw = (
    spark.read
         .option("header", True)
         .option("inferSchema", True)
         .csv(patient_src_path)
         .withColumn("ingest_time", current_timestamp())
         .withColumn("source_file", input_file_name())
)

print("Patient_RAW Schema:")
df_patient_raw.printSchema()


# -----------------------------
# 3. Write Patient Data to Bronze (Delta)
# -----------------------------
df_patient_raw.write.mode("overwrite").format("delta").save(bronze_patient_path)

spark.sql("""
    CREATE TABLE IF NOT EXISTS patient_bronze
    USING DELTA
    LOCATION 'dbfs:/FileStore/Mini_Project/bronze/patient_bronze'
""")

print("Patient Bronze Table Created Successfully.")


# -----------------------------
# 4. Read Raw Insurance File
# -----------------------------
df_insurance_raw = (
    spark.read
         .option("header", True)
         .option("inferSchema", True)
         .csv(insurance_src_path)
         .withColumn("ingest_time", current_timestamp())
         .withColumn("source_file", input_file_name())
)

print("Insurance_RAW Schema:")
df_insurance_raw.printSchema()


# -----------------------------
# 5. Write Insurance Data to Bronze (Delta)
# -----------------------------
df_insurance_raw.write.mode("overwrite").format("delta").save(bronze_insurance_path)

spark.sql("""
    CREATE TABLE IF NOT EXISTS insurance_bronze
    USING DELTA
    LOCATION 'dbfs:/FileStore/Mini_Project/bronze/insurance_bronze'
""")

print("Insurance Bronze Table Created Successfully.")
