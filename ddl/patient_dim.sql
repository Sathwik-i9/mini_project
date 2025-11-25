CREATE TABLE patient_dim (
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
USING DELTA;
