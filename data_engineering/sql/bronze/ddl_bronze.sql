CREATE TABLE IF NOT EXISTS bronze.ipca_1737_raw (
    nivel_territorial_codigo INTEGER,
    nivel_territorial_nome TEXT,
    unidade_medida_codigo INTEGER,
    unidade_medida_nome TEXT,
    valor NUMERIC(15,6),
    variavel_codigo INTEGER,
    variavel_nome TEXT,
    mes_codigo INTEGER,
    mes_nome TEXT,
    brasil_codigo INTEGER,
    brasil_nome TEXT,
    source_table INTEGER DEFAULT 1737,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze.ipca_7060_raw (
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
    classificacao_codigo INTEGER,
    classificacao_nome TEXT,
    source_table INTEGER DEFAULT 7060,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
