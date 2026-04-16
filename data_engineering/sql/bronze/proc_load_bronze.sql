\set ON_ERROR_STOP on

-- Bronze load script for the local IPCA SIDRA exports.
-- Table 1737 feeds the national series.
-- Table 7060 feeds the regional series by locality and classification.
-- The script clears and reloads both raw tables from the JSON files under data/.
-- The JSON files are kept as direct SIDRA exports, so the abbreviated API keys
-- are translated here when loading into the relational Bronze tables.
--
-- SIDRA key mapping used below:
-- `NC` / `NN`   -> territorial level code / name
-- `MC` / `MN`   -> unit of measure code / name
-- `V`           -> published value
-- `D1C` / `D1N` -> variable code / name
-- `D2C` / `D2N` -> month code / name
-- `D3C` / `D3N` -> Brazil code / name in table 1737, locality code / name in table 7060
-- `D4C` / `D4N` -> classification code / name in table 7060
--
-- SIDRA also returns the first JSON row as metadata describing the columns.
-- We discard that row during load by keeping only records with numeric `NC`.

BEGIN;

TRUNCATE TABLE bronze.ipca_1737_raw, bronze.ipca_7060_raw;

-- Stage the source payloads in temporary JSON tables.
CREATE TEMP TABLE tmp_1737 (data jsonb);
CREATE TEMP TABLE tmp_7060 (data jsonb);

\copy tmp_1737(data) FROM 'data/full_load_ipca.json';
\copy tmp_7060(data) FROM 'data/full_load_by_region.json';

-- Load the national series from SIDRA table 1737.
INSERT INTO bronze.ipca_1737_raw (
    -- Shared SIDRA attributes
    territorial_level_code,
    territorial_level_name,
    unit_of_measure_code,
    unit_of_measure_name,
    value,
    variable_code,
    variable_name,
    month_code,
    month_name,
    location_code,
    location_name
)
SELECT
    NULLIF(elem->>'NC', '')::integer,
    NULLIF(elem->>'NN', ''),
    NULLIF(elem->>'MC', '')::integer,
    NULLIF(elem->>'MN', ''),
    NULLIF(elem->>'V', '')::numeric(15,6),
    NULLIF(elem->>'D1C', '')::integer,
    NULLIF(elem->>'D1N', ''),
    NULLIF(elem->>'D2C', '')::integer,
    NULLIF(elem->>'D2N', ''),
    NULLIF(elem->>'D3C', '')::integer,
    NULLIF(elem->>'D3N', '')
FROM tmp_1737 AS src
CROSS JOIN LATERAL jsonb_array_elements(src.data) AS elem
-- Filter out the SIDRA metadata row; real records always have numeric `NC`.
WHERE (elem->>'NC') ~ '^[0-9]+$';

-- Load the regional series from SIDRA table 7060.
INSERT INTO bronze.ipca_7060_raw (
    -- Shared SIDRA attributes
    territorial_level_code,
    territorial_level_name,
    unit_of_measure_code,
    unit_of_measure_name,
    value,
    variable_code,
    variable_name,
    month_code,
    month_name,
    location_code,
    location_name,

    -- Regional-specific fields
    classification_code,
    classification_name
)
SELECT
    NULLIF(elem->>'NC', '')::integer,
    NULLIF(elem->>'NN', ''),
    NULLIF(elem->>'MC', '')::integer,
    NULLIF(elem->>'MN', ''),
    NULLIF(elem->>'V', '')::numeric(15,6),
    NULLIF(elem->>'D1C', '')::integer,
    NULLIF(elem->>'D1N', ''),
    NULLIF(elem->>'D2C', '')::integer,
    NULLIF(elem->>'D2N', ''),
    NULLIF(elem->>'D3C', '')::integer,
    NULLIF(elem->>'D3N', ''),
    NULLIF(elem->>'D4C', '')::integer,
    NULLIF(elem->>'D4N', '')
FROM tmp_7060 AS src
CROSS JOIN LATERAL jsonb_array_elements(src.data) AS elem
-- Filter out the SIDRA metadata row; real records always have numeric `NC`.
WHERE (elem->>'NC') ~ '^[0-9]+$';

COMMIT;
