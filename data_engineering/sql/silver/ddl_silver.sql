-- Silver layer dimension and fact tables for IPCA data.
-- `dim_month` stores the month reference data (temporal dimension).
-- `dim_location` stores the location reference data (geographic dimension).
-- `fact_ipca_index_number_brazil` stores the national IPCA index numbers.
-- `fact_ipca_monthly_variation_regional` stores the monthly regional variations.

-- Dimension table for temporal context
CREATE TABLE IF NOT EXISTS silver.dim_month (
    month_code INTEGER,
    month_date DATE NOT NULL,
    CONSTRAINT dim_month_pk PRIMARY KEY (month_code),
    CONSTRAINT dim_month_month_date_uk UNIQUE (month_date) -- Ensures each unique month appears only once
);

-- Dimension table for geographic context
CREATE TABLE IF NOT EXISTS silver.dim_location (
    location_id INTEGER GENERATED ALWAYS AS IDENTITY,
    location_code INTEGER NOT NULL,
    location_name TEXT NOT NULL,
    territorial_level_code INTEGER NOT NULL,
    territorial_level_name TEXT NOT NULL,
    CONSTRAINT dim_location_pk PRIMARY KEY (location_id),
    CONSTRAINT dim_location_bk_uk UNIQUE (territorial_level_code,location_code) -- Ensures no duplicate locations by territorial level and code
);

-- Fact table for national IPCA index numbers
CREATE TABLE IF NOT EXISTS silver.fact_ipca_index_number_brazil (
    month_code INTEGER NOT NULL,
    location_id INTEGER NOT NULL,
    index_number NUMERIC(15,6) NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fact_ipca_index_number_brazil_dim_month_fk
        FOREIGN KEY (month_code) REFERENCES silver.dim_month (month_code),
    CONSTRAINT fact_ipca_index_number_brazil_dim_location_fk
        FOREIGN KEY (location_id) REFERENCES silver.dim_location (location_id),
    CONSTRAINT fact_ipca_index_number_brazil_grain_uk
        UNIQUE (month_code, location_id) -- Ensures single index number per month and location combination
);

-- Fact table for regional monthly variations
CREATE TABLE IF NOT EXISTS silver.fact_ipca_monthly_variation_regional (
    month_code INTEGER NOT NULL,
    location_id INTEGER NOT NULL,
    regional_variation NUMERIC(15,6) NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fact_ipca_monthly_variation_regional_dim_month_fk
        FOREIGN KEY (month_code) REFERENCES silver.dim_month (month_code),
    CONSTRAINT fact_ipca_monthly_variation_regional_dim_location_fk
        FOREIGN KEY (location_id) REFERENCES silver.dim_location (location_id),
    CONSTRAINT fact_ipca_monthly_variation_regional_grain_uk
        UNIQUE (month_code, location_id) -- Ensures single regional variation per month and location combination
);
