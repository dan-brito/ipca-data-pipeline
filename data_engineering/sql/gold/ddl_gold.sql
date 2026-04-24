CREATE OR REPLACE VIEW gold.vw_ipca_brazil_monthly_variation AS
WITH brazil_index_base AS (
    SELECT
        f.month_code,
        m.month_date,
        f.location_id,
        l.location_name,
        l.territorial_level_code,
        l.territorial_level_name,
        f.index_number,
        LAG(f.index_number) OVER (ORDER BY f.month_code) AS previous_index_number
    FROM silver.fact_ipca_index_number_brazil AS f
    INNER JOIN silver.dim_month AS m
        ON f.month_code = m.month_code
    INNER JOIN silver.dim_location AS l
        ON f.location_id = l.location_id
)
SELECT
    month_code,
    month_date,
    location_id,
    location_name,
    territorial_level_code,
    territorial_level_name,
    index_number,
    previous_index_number,
    ROUND(((index_number / previous_index_number) - 1) * 100, 2) AS monthly_variation
FROM brazil_index_base;


CREATE OR REPLACE VIEW gold.vw_ipca_regional_monthly_variation AS
SELECT
    f.month_code,
    m.month_date,
    f.location_id,
    l.location_name,
    l.territorial_level_code,
    l.territorial_level_name,
    f.monthly_variation
FROM silver.fact_ipca_monthly_variation_regional AS f
INNER JOIN silver.dim_month AS m
    ON f.month_code = m.month_code
INNER JOIN silver.dim_location AS l
    ON f.location_id = l.location_id;

CREATE OR REPLACE VIEW gold.vw_ipca_monthly_variation_base AS
SELECT
    month_code,
    month_date,
    location_id,
    location_name,
    territorial_level_code,
    territorial_level_name,
    monthly_variation,
    'brazil_derived_from_index' AS source_type
FROM gold.vw_ipca_brazil_monthly_variation

UNION ALL
SELECT
    month_code,
    month_date,
    location_id,
    location_name,
    territorial_level_code,
    territorial_level_name,
    monthly_variation,
    'regional_source_monthly_variation' AS source_type
FROM gold.vw_ipca_regional_monthly_variation;

