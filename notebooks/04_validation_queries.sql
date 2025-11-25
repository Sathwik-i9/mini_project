-- Databricks Notebook
-- ---------------------------------------------------------
-- Notebook 04: Validation & Quality Checks for SCD-2 Pipeline
-- ---------------------------------------------------------

------------------------------------------------------------
-- 1. Basic Record Counts
------------------------------------------------------------
SELECT 'Patient Silver Count' AS metric, COUNT(*) AS value
FROM patient_silver;

SELECT 'Patient Dim Count (Gold)' AS metric, COUNT(*) AS value
FROM patient_dim;


------------------------------------------------------------
-- 2. Identify All Current Active Records
------------------------------------------------------------
SELECT *
FROM patient_dim
WHERE is_current = 'Y'
ORDER BY patient_id;


------------------------------------------------------------
-- 3. Full SCD History for Each Patient
------------------------------------------------------------
SELECT *
FROM patient_dim
ORDER BY patient_id, effective_start_dt;


------------------------------------------------------------
-- 4. Show Only Updated Versioned Records
------------------------------------------------------------
SELECT *
FROM patient_dim
WHERE is_current = 'N'
ORDER BY patient_id, effective_end_dt;


------------------------------------------------------------
-- 5. Validate SCD-2 Behavior:
--    Check if for every patient:
--    - Exactly one record has is_current = 'Y'
------------------------------------------------------------
SELECT patient_id, COUNT(*) AS total_versions,
SUM(CASE WHEN is_current = 'Y' THEN 1 ELSE 0 END) AS active_version_count
FROM patient_dim
GROUP BY patient_id
HAVING active_version_count != 1;


------------------------------------------------------------
-- 6. Check If Any Records Have Incorrect End Date
------------------------------------------------------------
SELECT *
FROM patient_dim
WHERE is_current = 'Y'
  AND effective_end_dt <> '9999-12-31'
ORDER BY patient_id;


------------------------------------------------------------
-- 7. Compare Silver → Gold Checksum Differences
------------------------------------------------------------
-- Ensure values that changed in Silver truly created new version in Gold
SELECT s.patient_id, 
       s.checksum_txt AS silver_checksum,
       d.checksum_txt AS gold_checksum,
       d.is_current,
       d.effective_start_dt
FROM patient_silver s
LEFT JOIN patient_dim d
ON s.patient_id = d.patient_id
ORDER BY patient_id;


------------------------------------------------------------
-- 8. Validate Surrogate Key Increment Logic
------------------------------------------------------------
SELECT patient_dim_key, patient_id, effective_start_dt
FROM patient_dim
ORDER BY patient_dim_key;


------------------------------------------------------------
--
