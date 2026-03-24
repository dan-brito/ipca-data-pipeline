CREATE TABLE IF NOT EXISTS silver.dim_tempo (
    mes_codigo INTEGER PRIMARY KEY,
    mes_data DATE NOT NULL UNIQUE,
    ano INTEGER NOT NULL,
    mes_numero INTEGER NOT NULL,
    mes_nome TEXT NOT NULL,
    CONSTRAINT dim_tempo_mes_codigo_chk
        CHECK (mes_codigo BETWEEN 100001 AND 999912),
    CONSTRAINT dim_tempo_mes_numero_chk
        CHECK (mes_numero BETWEEN 1 AND 12)
);

CREATE TABLE IF NOT EXISTS silver.dim_variavel (
    variavel_codigo INTEGER PRIMARY KEY,
    variavel_nome TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.dim_unidade_medida (
    unidade_medida_codigo INTEGER PRIMARY KEY,
    unidade_medida_nome TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.dim_localidade (
    localidade_codigo INTEGER PRIMARY KEY,
    localidade_nome TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.fato_ipca_brasil (
    mes_codigo INTEGER NOT NULL,
    variavel_codigo INTEGER NOT NULL,
    unidade_medida_codigo INTEGER NOT NULL,
    nivel_territorial_codigo INTEGER NOT NULL,
    nivel_territorial_nome TEXT NOT NULL,
    brasil_codigo INTEGER NOT NULL,
    brasil_nome TEXT NOT NULL,
    valor NUMERIC(15,6) NOT NULL,
    source_table INTEGER NOT NULL DEFAULT 1737,
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fato_ipca_brasil_pk
        PRIMARY KEY (mes_codigo, variavel_codigo, unidade_medida_codigo, brasil_codigo),
    CONSTRAINT fato_ipca_brasil_source_table_chk
        CHECK (source_table = 1737),
    CONSTRAINT fato_ipca_brasil_dim_tempo_fk
        FOREIGN KEY (mes_codigo) REFERENCES silver.dim_tempo (mes_codigo),
    CONSTRAINT fato_ipca_brasil_dim_variavel_fk
        FOREIGN KEY (variavel_codigo) REFERENCES silver.dim_variavel (variavel_codigo),
    CONSTRAINT fato_ipca_brasil_dim_unidade_fk
        FOREIGN KEY (unidade_medida_codigo) REFERENCES silver.dim_unidade_medida (unidade_medida_codigo)
);

CREATE TABLE IF NOT EXISTS silver.fato_ipca_regional (
    mes_codigo INTEGER NOT NULL,
    variavel_codigo INTEGER NOT NULL,
    unidade_medida_codigo INTEGER NOT NULL,
    localidade_codigo INTEGER NOT NULL,
    classificacao_codigo INTEGER NOT NULL,
    nivel_territorial_codigo INTEGER NOT NULL,
    nivel_territorial_nome TEXT NOT NULL,
    classificacao_nome TEXT NOT NULL,
    valor NUMERIC(15,6) NOT NULL,
    source_table INTEGER NOT NULL DEFAULT 7060,
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fato_ipca_regional_pk
        PRIMARY KEY (
            mes_codigo,
            variavel_codigo,
            unidade_medida_codigo,
            localidade_codigo,
            classificacao_codigo
        ),
    CONSTRAINT fato_ipca_regional_source_table_chk
        CHECK (source_table = 7060),
    CONSTRAINT fato_ipca_regional_dim_tempo_fk
        FOREIGN KEY (mes_codigo) REFERENCES silver.dim_tempo (mes_codigo),
    CONSTRAINT fato_ipca_regional_dim_variavel_fk
        FOREIGN KEY (variavel_codigo) REFERENCES silver.dim_variavel (variavel_codigo),
    CONSTRAINT fato_ipca_regional_dim_unidade_fk
        FOREIGN KEY (unidade_medida_codigo) REFERENCES silver.dim_unidade_medida (unidade_medida_codigo),
    CONSTRAINT fato_ipca_regional_dim_localidade_fk
        FOREIGN KEY (localidade_codigo) REFERENCES silver.dim_localidade (localidade_codigo)
);
