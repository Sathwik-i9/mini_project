-- Databricks Notebook
-- ---------------------------------------------------------
-- Notebook 03: SCD-2 Merge into Gold Layer (patient_dim)
-- ---------------------------------------------------------


-- ---------------------------------------------
-- 1. Load Silver Table into Temp View
-- ---------------------------------------------
CREATE OR REPLACE TEMP VIEW patient_silver AS
SELECT *
FROM delta.`dbfs:/FileStore/Mini_Project/silver/patient_silver`;

-- ---------------------------------------------
-- 2. Load Existing Dimension Table (patient_dim)
-- ---------------------------------------------
CREATE TABLE IF NOT EXISTS patient_dim (
    patient_dim_key BIGINT,
    patient_id STRING,
    first_name STRING,
    last_name STRING,
    age INT,
    address STRING,
    phone_number STRING,
    bill_amount DOUBLE,
    insurance_provider STRING,
    policy_id STRING,
    checksum_txt STRING,
    load_ctl_key INT,
    effective_start_dt TIMESTAMP,
    effective_end_dt TIMESTAMP,
    is_current STRING
)
USING DELTA
LOCATION 'dbfs:/FileStore/Mini_Project/dim/patient_dim';


-- ---------------------------------------------
-- 3. Create Transaction Indicator (I/U/N)
-- ---------------------------------------------
CREATE OR REPLACE TEMP VIEW patient_landing_txn AS
SELECT
    src.*,
    CASE
        WHEN dim.patient_id IS NULL THEN 'I'
        WHEN dim.checksum_txt <> src.checksum_txt THEN 'U'
        ELSE 'N'
    END AS transaction_ind
FROM patient_silver src
LEFT JOIN patient_dim dim
    ON src.patient_id = dim.patient_id
    AND dim.is_current = 'Y';


-- ---------------------------------------------
-- 4. Deduplicate Landing Data (ROW_NUMBER logic)
-- ---------------------------------------------
CREATE OR REPLACE TEMP VIEW patient_landing_final_dedup AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY updated_timestamp DESC) AS rn
    FROM patient_landing_txn
) t
WHERE rn = 1;


-- ---------------------------------------------
-- 5. Perform SCD-2 MERGE into patient_dim
-- ---------------------------------------------
MERGE INTO patient_dim AS dim
USING patient_landing_final_dedup AS src
ON dim.patient_id = src.patient_id
AND dim.is_current = 'Y'

-- ---------------------------------------------
-- 5A. Update existing record (close old row)
-- ---------------------------------------------
WHEN MATCHED AND src.transaction_ind = 'U'
THEN UPDATE SET
    dim.is_current = 'N',
    dim.effective_end_dt = current_timestamp()

-- ---------------------------------------------
-- 5B. Insert new version after update
-- ---------------------------------------------
WHEN NOT MATCHED AND src.transaction_ind = 'U'
THEN INSERT (
      patient_dim_key,
      patient_id,
      first_name,
      last_name,
      age,
      address,
      phone_number,
      bill_amount,
      insurance_provider,
      policy_id,
      checksum_txt,
      load_ctl_key,
      effective_start_dt,
      effective_end_dt,
      is_current
)
VALUES (
      (SELECT COALESCE(MAX(patient_dim_key), 0) + 1 FROM patient_dim),
      src.patient_id,
      src.first_name,
      src.last_name,
      src.age,
      src.address,
      src.phone_number,
      src.bill_amount,
      src.insurance_provider,
      src.policy_id,
      src.checksum_txt,
      src.load_ctl_key,
      current_timestamp(),
      '9999-12-31',
      'Y'
)

-- ---------------------------------------------
-- 5C. Insert new record (Insert case)
-- ---------------------------------------------
WHEN NOT MATCHED AND src.transaction_ind = 'I'
THEN INSERT (
      patient_dim_key,
      patient_id,
      first_name,
      last_name,
      age,
      address,
      phone_number,
      bill_amount,
      insurance_provider,
      policy_id,
      checksum_txt,
      load_ctl_key,
      effective_start_dt,
      effective_end_dt,
      is_current
)
VALUES (
      (SELECT COALESCE(MAX(patient_dim_key), 0) + 1 FROM patient_dim),
      src.patient_id,
      src.first_name,
      src.last_name,
      src.age,
      src.address,
      src.phone_number,
      src.bill_amount,
      src.insurance_provider,
      src.policy_id,
      src.checksum_txt,
      src.load_ctl_key,
      current_timestamp(),
      '9999-12-31',
      'Y'
);
