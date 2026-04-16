\set ON_ERROR_STOP on

-- Silver load script for the curated IPCA warehouse layer.
-- Bronze table 1737 feeds the national index fact and contributes shared dimensions.
-- Bronze table 7060 feeds the regional monthly variation fact and contributes shared dimensions.
-- The script performs a full refresh of the Silver dimensions and facts in one transaction.
-- `RESTART IDENTITY` is intentional so `silver.dim_location` keeps deterministic surrogate keys.

-- Rebuild the Silver layer atomically so dimensions and facts stay in sync.
BEGIN;

-- Clear the current Silver contents before repopulating them from Bronze.
-- Resetting identities here preserves a stable `location_id` sequence on every reload.
TRUNCATE TABLE
    silver.fact_ipca_monthly_variation_regional,
    silver.fact_ipca_index_number_brazil,
    silver.dim_location,
    silver.dim_month
RESTART IDENTITY;

-- Build the month dimension from every distinct month present in either Bronze source.
INSERT INTO silver.dim_month(
    month_code,
    month_date)
SELECT
    src.month_code,
    -- `month_code` arrives as `YYYYMM`, so `TO_DATE` maps it to the first day of that month.
    TO_DATE(src.month_code::text, 'YYYYMM') AS month_date
FROM (
    -- `UNION` keeps one row per month even when both Bronze tables contain the same period.
    SELECT month_code
    FROM bronze.ipca_1737_raw
    UNION
    SELECT month_code
    FROM bronze.ipca_7060_raw
) AS src
ORDER BY src.month_code;

-- Build the location dimension from the distinct location attributes in both Bronze sources.
INSERT INTO silver.dim_location (
    location_code,
    location_name, 
    territorial_level_code,
    territorial_level_name)
SELECT
    src.location_code,
    src.location_name,
    src.territorial_level_code,
    src.territorial_level_name
FROM (
    -- `UNION` removes duplicate location definitions shared across the two extracts.
    SELECT
    location_code,
    location_name, 
    territorial_level_code,
    territorial_level_name
    FROM bronze.ipca_1737_raw
    UNION
    SELECT
    location_code,
    location_name,
    territorial_level_code,
    territorial_level_name
    FROM bronze.ipca_7060_raw
) AS src
ORDER BY
    src.territorial_level_code,
    src.location_code;

-- Load the Brazil index fact from Bronze table 1737 after resolving the location surrogate key.
INSERT INTO silver.fact_ipca_index_number_brazil (
    month_code,
    location_id, 
    index_number)
SELECT
    month_code,
    location_id,
    value AS index_number
    -- Facts store the surrogate key from `dim_location`, not the raw Bronze location code.
FROM bronze.ipca_1737_raw bir
    LEFT JOIN silver.dim_location sdl
    ON bir.territorial_level_code = sdl.territorial_level_code
    AND bir.location_code = sdl.location_code; 

-- Load the regional monthly variation fact from Bronze table 7060 using the same location mapping.
INSERT INTO silver.fact_ipca_monthly_variation_regional (
    month_code,
    location_id, 
    monthly_variation)
SELECT
    month_code,
    location_id,
    value AS monthly_variation
-- Resolve the shared `dim_location` surrogate key before inserting the regional fact rows.
FROM bronze.ipca_7060_raw bir
    LEFT JOIN silver.dim_location sdl
    ON bir.territorial_level_code = sdl.territorial_level_code
    AND bir.location_code = sdl.location_code;

COMMIT;
