\set ON_ERROR_STOP on

BEGIN;

TRUNCATE TABLE
    silver.fato_ipca_regional,
    silver.fato_ipca_brasil,
    silver.dim_localidade,
    silver.dim_unidade_medida,
    silver.dim_variavel,
    silver.dim_tempo;

INSERT INTO silver.dim_tempo (
    mes_codigo,
    mes_data,
    ano,
    mes_numero,
    mes_nome
)
SELECT DISTINCT
    src.mes_codigo,
    TO_DATE(src.mes_codigo::text, 'YYYYMM') AS mes_data,
    (src.mes_codigo / 100) AS ano,
    (src.mes_codigo % 100) AS mes_numero,
    src.mes_nome
FROM (
    SELECT mes_codigo, mes_nome
    FROM bronze.ipca_1737_raw
    WHERE mes_codigo IS NOT NULL
      AND mes_nome IS NOT NULL
    UNION
    SELECT mes_codigo, mes_nome
    FROM bronze.ipca_7060_raw
    WHERE mes_codigo IS NOT NULL
      AND mes_nome IS NOT NULL
) AS src
ORDER BY src.mes_codigo;

INSERT INTO silver.dim_variavel (
    variavel_codigo,
    variavel_nome
)
SELECT DISTINCT
    src.variavel_codigo,
    src.variavel_nome
FROM (
    SELECT variavel_codigo, variavel_nome
    FROM bronze.ipca_1737_raw
    WHERE variavel_codigo IS NOT NULL
      AND variavel_nome IS NOT NULL
    UNION
    SELECT variavel_codigo, variavel_nome
    FROM bronze.ipca_7060_raw
    WHERE variavel_codigo IS NOT NULL
      AND variavel_nome IS NOT NULL
) AS src
ORDER BY src.variavel_codigo;

INSERT INTO silver.dim_unidade_medida (
    unidade_medida_codigo,
    unidade_medida_nome
)
SELECT DISTINCT
    src.unidade_medida_codigo,
    src.unidade_medida_nome
FROM (
    SELECT unidade_medida_codigo, unidade_medida_nome
    FROM bronze.ipca_1737_raw
    WHERE unidade_medida_codigo IS NOT NULL
      AND unidade_medida_nome IS NOT NULL
    UNION
    SELECT unidade_medida_codigo, unidade_medida_nome
    FROM bronze.ipca_7060_raw
    WHERE unidade_medida_codigo IS NOT NULL
      AND unidade_medida_nome IS NOT NULL
) AS src
ORDER BY src.unidade_medida_codigo;

INSERT INTO silver.dim_localidade (
    localidade_codigo,
    localidade_nome
)
SELECT DISTINCT
    localidade_codigo,
    localidade_nome
FROM bronze.ipca_7060_raw
WHERE localidade_codigo IS NOT NULL
  AND localidade_nome IS NOT NULL
ORDER BY localidade_codigo;

INSERT INTO silver.fato_ipca_brasil (
    mes_codigo,
    variavel_codigo,
    unidade_medida_codigo,
    nivel_territorial_codigo,
    nivel_territorial_nome,
    brasil_codigo,
    brasil_nome,
    valor,
    source_table
)
SELECT
    mes_codigo,
    variavel_codigo,
    unidade_medida_codigo,
    nivel_territorial_codigo,
    nivel_territorial_nome,
    brasil_codigo,
    brasil_nome,
    valor,
    source_table
FROM bronze.ipca_1737_raw
WHERE mes_codigo IS NOT NULL
  AND variavel_codigo IS NOT NULL
  AND unidade_medida_codigo IS NOT NULL
  AND nivel_territorial_codigo IS NOT NULL
  AND nivel_territorial_nome IS NOT NULL
  AND brasil_codigo IS NOT NULL
  AND brasil_nome IS NOT NULL
  AND valor IS NOT NULL;

INSERT INTO silver.fato_ipca_regional (
    mes_codigo,
    variavel_codigo,
    unidade_medida_codigo,
    localidade_codigo,
    classificacao_codigo,
    nivel_territorial_codigo,
    nivel_territorial_nome,
    classificacao_nome,
    valor,
    source_table
)
SELECT
    mes_codigo,
    variavel_codigo,
    unidade_medida_codigo,
    localidade_codigo,
    classificacao_codigo,
    nivel_territorial_codigo,
    nivel_territorial_nome,
    classificacao_nome,
    valor,
    source_table
FROM bronze.ipca_7060_raw
WHERE mes_codigo IS NOT NULL
  AND variavel_codigo IS NOT NULL
  AND unidade_medida_codigo IS NOT NULL
  AND localidade_codigo IS NOT NULL
  AND classificacao_codigo IS NOT NULL
  AND nivel_territorial_codigo IS NOT NULL
  AND nivel_territorial_nome IS NOT NULL
  AND classificacao_nome IS NOT NULL
  AND valor IS NOT NULL;

COMMIT;
