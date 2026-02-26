\set ON_ERROR_STOP on

BEGIN;

TRUNCATE TABLE bronze.ipca_1737_raw;
TRUNCATE TABLE bronze.ipca_7060_raw;

CREATE TEMP TABLE tmp_1737 (data jsonb);
CREATE TEMP TABLE tmp_7060 (data jsonb);

\copy tmp_1737(data) FROM 'data/full_load_ipca.json';
\copy tmp_7060(data) FROM 'data/full_load_by_region.json';

INSERT INTO bronze.ipca_1737_raw (
    nivel_territorial_codigo,
    nivel_territorial_nome,
    unidade_medida_codigo,
    unidade_medida_nome,
    valor,
    variavel_codigo,
    variavel_nome,
    mes_codigo,
    mes_nome,
    brasil_codigo,
    brasil_nome
)
SELECT
    NULLIF(elem->>'NC','')::integer,
    NULLIF(elem->>'NN',''),
    NULLIF(elem->>'MC','')::integer,
    NULLIF(elem->>'MN',''),
    NULLIF(elem->>'V','')::numeric(15,6),
    NULLIF(elem->>'D1C','')::integer,
    NULLIF(elem->>'D1N',''),
    NULLIF(elem->>'D2C','')::integer,
    NULLIF(elem->>'D2N',''),
    NULLIF(elem->>'D3C','')::integer,
    NULLIF(elem->>'D3N','')
FROM tmp_1737,
LATERAL jsonb_array_elements(tmp_1737.data) AS elem
WHERE (elem->>'NC') ~ '^[0-9]+$';

INSERT INTO bronze.ipca_7060_raw (
    nivel_territorial_codigo,
    nivel_territorial_nome,
    unidade_medida_codigo,
    unidade_medida_nome,
    valor,
    variavel_codigo,
    variavel_nome,
    mes_codigo,
    mes_nome,
    localidade_codigo,
    localidade_nome,
    classificacao_codigo,
    classificacao_nome
)
SELECT
    NULLIF(elem->>'NC','')::integer,
    NULLIF(elem->>'NN',''),
    NULLIF(elem->>'MC','')::integer,
    NULLIF(elem->>'MN',''),
    NULLIF(elem->>'V','')::numeric(15,6),
    NULLIF(elem->>'D1C','')::integer,
    NULLIF(elem->>'D1N',''),
    NULLIF(elem->>'D2C','')::integer,
    NULLIF(elem->>'D2N',''),
    NULLIF(elem->>'D3C','')::integer,
    NULLIF(elem->>'D3N',''),
    NULLIF(elem->>'D4C','')::integer,
    NULLIF(elem->>'D4N','')
FROM tmp_7060,
LATERAL jsonb_array_elements(tmp_7060.data) AS elem
WHERE (elem->>'NC') ~ '^[0-9]+$';

COMMIT;