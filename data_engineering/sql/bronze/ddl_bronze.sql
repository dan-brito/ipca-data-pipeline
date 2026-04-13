-- Bronze raw tables for IPCA SIDRA extractions.
-- `ipca_1737_raw` stores the national series.
-- `ipca_7060_raw` stores the regional series by locality and classification.
-- Both tables preserve the original payload shape and add lineage metadata.

CREATE TABLE IF NOT EXISTS bronze.ipca_1737_raw (
    -- Shared SIDRA attributes
    nivel_territorial_codigo INTEGER,
    nivel_territorial_nome TEXT,
    unidade_medida_codigo INTEGER,
    unidade_medida_nome TEXT,
    valor NUMERIC(15,6),
    variavel_codigo INTEGER,
    variavel_nome TEXT,
    mes_codigo INTEGER,
    mes_nome TEXT,
    localidade_codigo INTEGER,
    localidade_nome TEXT,

    -- Lineage metadata
    source_table INTEGER DEFAULT 1737,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze.ipca_7060_raw (
    -- Shared SIDRA attributes
    nivel_territorial_codigo INTEGER,
    nivel_territorial_nome TEXT,
    unidade_medida_codigo INTEGER,
    unidade_medida_nome TEXT,
    valor NUMERIC(15,6),
    variavel_codigo INTEGER,
    variavel_nome TEXT,
    mes_codigo INTEGER,
    mes_nome TEXT,
    localidade_codigo INTEGER,
    localidade_nome TEXT,

    -- Regional-specific fields
    classificacao_codigo INTEGER,
    classificacao_nome TEXT,

    -- Lineage metadata
    source_table INTEGER DEFAULT 7060,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
