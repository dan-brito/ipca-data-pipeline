# IPCA Data Pipeline

**Status:** Bronze e Silver concluídas e validadas | Gold em andamento

Pipeline analítica SQL-first em PostgreSQL construída sobre dados oficiais do IPCA do IBGE/SIDRA.

Este README é intencionalmente intermediário. O repositório já possui uma base funcional e validada, mas a camada de consumo analítico ainda está em construção. O objetivo deste documento é comunicar com honestidade o estado atual do projeto, a direção técnica já implementada e o próximo marco planejado para o repositório.

## Objetivo do projeto

Este projeto foi pensado para demonstrar um trabalho disciplinado de Engenharia de Dados sobre dados públicos oficiais.

Nesta fase, o objetivo principal é mostrar:

- ingestão reproduzível de dados públicos
- organização clara em arquitetura medallion
- transformações SQL-first em PostgreSQL
- separação entre camadas bruta, estruturada e analítica
- preparação para uma futura camada de consumo analítico e artefatos de BI

## Estado atual

O repositório atualmente inclui:

- schemas `bronze`, `silver` e `gold`
- scripts de DDL para Bronze e Silver
- carga da Bronze a partir de arquivos JSON locais exportados do SIDRA
- carga da Silver com dimensões e fatos relacionais
- script de validação da Bronze
- script de validação da Silver

Em termos práticos, a pipeline já cobre:

- ingestão fiel das fontes oficiais na Bronze
- modelagem relacional e transformação na Silver
- validação explícita das duas camadas já implementadas

**Importante:** o schema `gold` já existe como parte da estrutura do projeto, mas a camada Gold em si **ainda não foi implementada**.

## Arquitetura em camadas

### Bronze

Tabelas Bronze atuais:

- `bronze.ipca_1737_raw`
- `bronze.ipca_7060_raw`

Objetivo:

- preservar os dados de origem com transformação mínima
- manter a ingestão determinística e reproduzível
- servir como base bruta para as camadas seguintes

### Silver

Tabelas Silver atuais:

- `silver.dim_month`
- `silver.dim_location`
- `silver.fact_ipca_index_number_brazil`
- `silver.fact_ipca_monthly_variation_regional`

Objetivo:

- organizar a origem em dimensões e fatos relacionais
- impor grão de negócio e consistência estrutural
- preparar o modelo para consumo analítico na Gold

### Gold

Camada analítica planejada para consumo.

Responsabilidades esperadas:

- consolidar métricas analíticas e datasets orientados a consumo
- aprofundar a modelagem analítica para comparação temporal e regional
- servir de base para futuros artefatos em Power BI e outras formas de apresentação

A Gold é o próximo marco de implementação do projeto.

## Fontes de dados

O projeto utiliza dados públicos oficiais do IPCA do IBGE/SIDRA em dois recortes principais.

### Tabela SIDRA 1737

Série nacional do número-índice do IPCA.

- arquivo atual: `data/full_load_ipca.json`
- uso principal: base para a série de índice do Brasil

### Tabela SIDRA 7060

Variação mensal do IPCA por recorte regional.

- arquivo atual: `data/full_load_by_region.json`
- uso principal: base para a série regional de variação

Nesta fase, o fluxo documentado parte de arquivos JSON já disponíveis localmente dentro do diretório `data/`.

## Stack e decisões técnicas

Stack atual:

- PostgreSQL
- `psql` para execução dos scripts
- SQL como principal linguagem de transformação
- arquivos JSON locais como entrada de carga

Decisões intencionais desta fase:

- priorizar SQL em vez de lógica pesada em Python
- manter a execução reproduzível a partir da raiz do projeto
- evitar orquestração antes da estabilização da base da pipeline
- evitar expansão desnecessária de escopo

## Fluxo de execução

Em alto nível, o projeto atualmente deve ser executado nesta ordem:

1. inicializar schemas e objetos estruturais
2. executar DDL/carga da Bronze
3. executar validação da Bronze
4. executar DDL/carga da Silver
5. executar validação da Silver

A organização atual segue a separação por camada e responsabilidade:

- DDL
- load
- validation

## Status da validação

As camadas implementadas não são apenas carregadas, mas também validadas explicitamente.

A cobertura atual de validação inclui, no mínimo:

- checks críticos de nulidade
- checks de duplicidade de grão
- checks de integridade referencial na Silver
- sanity checks de contagem de linhas para os fatos implementados

## Próximos passos

Prioridades de curto prazo:

1. iniciar oficialmente a camada Gold
2. definir e aprofundar a modelagem analítica da Gold
3. preparar artefatos orientados a consumo no Power BI
4. construir saídas analíticas com foco temporal e regional
5. reescrever a narrativa principal do repositório quando a camada analítica estiver madura

## Visão de entrega final

A versão final deste projeto deve comunicar de forma integrada dois blocos de competência:

- construção de uma pipeline analítica bem estruturada até a camada de consumo
- preparação de uma camada final orientada a BI, com saídas analíticas coerentes

Quando a Gold estiver madura, a documentação principal poderá ser reescrita em um formato mais enxuto e mais polido, com cara final de portfólio.

