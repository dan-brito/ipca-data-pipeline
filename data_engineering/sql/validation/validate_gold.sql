\set ON_ERROR_STOP on

/*
===============================================================================
Gold Quality Checks
===============================================================================
Purpose:
    Validate the initial Gold layer base views.

Assumptions:
    - gold.vw_ipca_brazil_monthly_variation excludes rows where monthly_variation
      cannot yet be derived (e.g. first month in the series).
    - gold.vw_ipca_regional_monthly_variation is a 1:1 enrichment of the Silver
      regional fact.
    - gold.vw_ipca_monthly_variation_base is the UNION ALL of Brazil(minus LAG Nulls) + Regional.
===============================================================================
*/

BEGIN;

DROP TABLE IF EXISTS gold_validation_report;

CREATE TEMP TABLE gold_validation_report (
    check_name   TEXT,
    check_status TEXT,
    check_detail TEXT,
    issue_count  BIGINT
) ON COMMIT DROP;

/*
-------------------------------------------------------------------------------
1) Unexpected Nullability
-------------------------------------------------------------------------------
*/

INSERT INTO gold_validation_report (
    check_name,
    check_status,
    check_detail,
    issue_count
)
SELECT
    check_name,
    CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END,
    FORMAT('%s rows', issue_count),
    issue_count
FROM (
    SELECT 'null_month_code_gold_brazil_view' AS check_name, COUNT(*) AS issue_count
    FROM gold.vw_ipca_brazil_monthly_variation
    WHERE month_code IS NULL

    UNION ALL

    SELECT 'null_location_id_gold_brazil_view', COUNT(*)
    FROM gold.vw_ipca_brazil_monthly_variation
    WHERE location_id IS NULL

    UNION ALL

    SELECT 'null_month_code_gold_regional_view', COUNT(*)
    FROM gold.vw_ipca_regional_monthly_variation
    WHERE month_code IS NULL

    UNION ALL

    SELECT 'null_location_id_gold_regional_view', COUNT(*)
    FROM gold.vw_ipca_regional_monthly_variation
    WHERE location_id IS NULL

    UNION ALL

    SELECT 'null_monthly_variation_gold_regional_view', COUNT(*)
    FROM gold.vw_ipca_regional_monthly_variation
    WHERE monthly_variation IS NULL

    UNION ALL

    SELECT 'null_month_code_gold_base_view', COUNT(*)
    FROM gold.vw_ipca_monthly_variation_base
    WHERE month_code IS NULL

    UNION ALL

    SELECT 'null_location_id_gold_base_view', COUNT(*)
    FROM gold.vw_ipca_monthly_variation_base
    WHERE location_id IS NULL

    UNION ALL

    SELECT 'null_monthly_variation_gold_base_view', COUNT(*)
    FROM gold.vw_ipca_monthly_variation_base
    WHERE monthly_variation IS NULL
) AS nullability_issues;

/*
-------------------------------------------------------------------------------
2) Grain Duplication
-------------------------------------------------------------------------------
*/

INSERT INTO gold_validation_report (
    check_name,
    check_status,
    check_detail,
    issue_count
)
SELECT
    check_name,
    CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END,
    FORMAT('%s duplicate groups', issue_count),
    issue_count
FROM (
    SELECT 'duplicate_gold_brazil_view_grain' AS check_name, COUNT(*) AS issue_count
    FROM (
        SELECT
            month_code,
            location_id
        FROM gold.vw_ipca_brazil_monthly_variation
        GROUP BY
            month_code,
            location_id
        HAVING COUNT(*) > 1
    ) AS duplicate_groups

    UNION ALL

    SELECT 'duplicate_gold_regional_view_grain', COUNT(*)
    FROM (
        SELECT
            month_code,
            location_id
        FROM gold.vw_ipca_regional_monthly_variation
        GROUP BY
            month_code,
            location_id
        HAVING COUNT(*) > 1
    ) AS duplicate_groups

    UNION ALL

    SELECT 'duplicate_gold_base_view_grain', COUNT(*)
    FROM (
        SELECT
            month_code,
            location_id
        FROM gold.vw_ipca_monthly_variation_base
        GROUP BY
            month_code,
            location_id
        HAVING COUNT(*) > 1
    ) AS duplicate_groups
) AS duplication_issues;

/*
-------------------------------------------------------------------------------
3) Dynamic Volume / Count Sanity Checks
-------------------------------------------------------------------------------
*/

INSERT INTO gold_validation_report (
    check_name,
    check_status,
    check_detail,
    issue_count
)
SELECT
    check_name,
    CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END,
    FORMAT(
        'expected=%s, actual=%s, difference=%s',
        expected_count,
        actual_count,
        issue_count
    ),
    issue_count
FROM (
    /* Regional Gold view should preserve Silver fact row count 1:1 */
    SELECT
        'row_count_mismatch_gold_regional_view' AS check_name,
        (SELECT COUNT(*) FROM silver.fact_ipca_monthly_variation_regional) AS expected_count,
        (SELECT COUNT(*) FROM gold.vw_ipca_regional_monthly_variation) AS actual_count,
        ABS(
            (SELECT COUNT(*) FROM silver.fact_ipca_monthly_variation_regional) -
            (SELECT COUNT(*) FROM gold.vw_ipca_regional_monthly_variation)
        ) AS issue_count

    UNION ALL

    /* Brazil Gold view should preserve the Silver Brazil fact row count 1:1. */
    SELECT
        'row_count_mismatch_gold_brazil_view',
        (
            SELECT COUNT(*)
            FROM silver.fact_ipca_index_number_brazil
        ) AS expected_count,
        (SELECT COUNT(*) FROM gold.vw_ipca_brazil_monthly_variation) AS actual_count,
        ABS(
            (
                SELECT COUNT(*)
                FROM silver.fact_ipca_index_number_brazil
            ) -
            (SELECT COUNT(*) FROM gold.vw_ipca_brazil_monthly_variation)
        ) AS issue_count

    UNION ALL

    /* Base Gold view SHOULD NOT BE EQUAL Brazil Gold + Regional Gold,
       because null values caused by LAG function in Brazil Gold view are filtered out.
       This is more of a sanity check to ensure the UNION ALL logic is working as expected. */
    SELECT
        'row_count_mismatch_gold_base_view',
        (
            (SELECT COUNT(*) FROM gold.vw_ipca_brazil_monthly_variation) - 1 +
            (SELECT COUNT(*) FROM gold.vw_ipca_regional_monthly_variation)
        ) AS expected_count,
        (SELECT COUNT(*) FROM gold.vw_ipca_monthly_variation_base) AS actual_count,
        ABS(
            (
                (SELECT COUNT(*) FROM gold.vw_ipca_brazil_monthly_variation) - 1 +
                (SELECT COUNT(*) FROM gold.vw_ipca_regional_monthly_variation)
            ) -
            (SELECT COUNT(*) FROM gold.vw_ipca_monthly_variation_base)
        ) AS issue_count
) AS volume_issues;

/*
-------------------------------------------------------------------------------
4) Final Report
-------------------------------------------------------------------------------
*/

DO $$
DECLARE
    rec RECORD;
    failure_count BIGINT;
BEGIN
    FOR rec IN
        SELECT *
        FROM gold_validation_report
    LOOP
        RAISE NOTICE '% -> % -> %',
            rec.check_name,
            rec.check_status,
            rec.check_detail;
    END LOOP;

    SELECT COUNT(*)
    INTO failure_count
    FROM gold_validation_report
    WHERE check_status = 'FAIL';

    IF failure_count > 0 THEN
        RAISE EXCEPTION 'Gold validation failed (% failing checks)', failure_count;
    END IF;
END $$;

SELECT 'gold_validation_passed' AS status;

COMMIT;