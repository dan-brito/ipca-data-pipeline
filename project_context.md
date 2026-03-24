# IPCA Data Engineering Project Context

Status: Bronze layer validated, reproducible, and considered stable.

## 1. Project Summary

This is an end-to-end Data Engineering portfolio project built on official IPCA data from IBGE/SIDRA.

Its purpose is not to maximize feature count. Its purpose is to demonstrate sound engineering judgment through:

- disciplined ingestion
- relational modeling
- layered architecture
- deterministic execution
- controlled scope
- reproducible transformations

This project should communicate engineering maturity, not experimentation.

---

## 2. Primary Goal

Build a portfolio-grade data pipeline that shows the ability to:

- ingest public data reliably
- persist it correctly in PostgreSQL
- organize transformations through a medallion architecture
- preserve reproducibility and determinism
- make technical decisions with controlled scope

This is a Data Engineering project first.
It is not a dashboard project, not a data science project, and not a framework showcase.

---

## 3. Non-Negotiable Scope

### Included in scope

- Full-load ingestion from local JSON files exported from SIDRA
- Bronze → Silver → Gold architecture
- PostgreSQL as the core analytical store
- SQL-first transformations
- Execution through `psql`
- Relative-path execution from project root
- Deterministic, reproducible pipeline behavior

### Explicitly out of scope for now

- Dashboards
- Airflow, Prefect, Dagster, or similar orchestration tools
- Dockerization before core pipeline stability
- Incremental loading before full-load flow is solid
- Generic multi-source abstractions
- Python-heavy transformation logic
- Over-engineering in general

Rule: do not expand scope until the current layer is stable.

---

## 4. Technology Decisions

### Current stack

- PostgreSQL (running in WSL)
- `psql` for script execution
- VS Code + SQLTools for database connection and inspection
- SQL as the primary transformation language

### Deliberate exclusions

- No ORM
- No pandas in Bronze
- No external transformation framework
- No orchestration layer yet

### Conditional future use of Python

Python may be introduced later for ingestion convenience or controlled abstraction.
It should not replace SQL where SQL is the clearer and more robust option for warehouse-style transformations.

---

## 5. Data Sources

## 5.1 SIDRA Table 1737 — IPCA Número-índice

**Meaning**
- National IPCA index series

**Granularity**
- Brasil × mês × variável

**Source file**
- `data/full_load_ipca.json`

**Expected Bronze row count**
- 372

---

## 5.2 SIDRA Table 7060 — IPCA Regional Variation

**Meaning**
- Monthly IPCA variation by metropolitan region / regional cut

**Granularity**
- Região Metropolitana × mês × variável × classificação

**Source file**
- `data/full_load_by_region.json`

**Expected Bronze row count**
- 1152

---

## 6. Architectural Model

The project follows a medallion architecture with clear layer responsibilities.

## 6.1 Bronze Layer

### Purpose
Store the source data as faithfully as possible with only the minimum required parsing and typing.

### Bronze principles

- ingest source data with minimal transformation
- preserve source-level granularity
- avoid semantic remodeling
- avoid derived metrics
- allow full reloads deterministically

### Bronze characteristics

- one table per SIDRA source
- JSON exploded into rows
- header row filtered out
- explicit casting applied where necessary
- no aggregation
- no analytical logic
- no business-level derivations

### Bronze load pattern

- `TRUNCATE + full reload`
- deterministic script execution
- client-side file loading through `\copy`

Bronze is stable when row counts, typing, and reproducibility are validated.

---

## 6.2 Silver Layer

### Purpose
Transform Bronze data into cleaned, constrained, semantically usable relational data.

### Silver responsibilities

- normalize time representation
- enforce business granularity
- reduce redundant textual duplication
- apply stronger typing and constraints
- prepare data for analytical consumption

### Planned Silver operations

- convert `mes_codigo` from `YYYYMM` integer into a proper date representation
- define natural keys where appropriate
- remove unnecessary duplication inherited from raw extracts
- optionally restrict variables if the analytical scope requires it

Silver is where semantic structure begins.

---

## 6.3 Gold Layer

### Purpose
Expose analytical views, metrics, and comparison-ready datasets.

### Gold responsibilities

- analytical views
- derived measures
- regional comparisons
- temporal comparisons

### Example outputs

- year-over-year inflation
- regional divergence measures
- comparison between index-based and variation-based views

Gold is for consumption, not ingestion.

---

## 7. Current Directory Structure

```text
ipca/
├── data/
│   ├── full_load_ipca.json
│   └── full_load_by_region.json
├── data_engineering/
│   └── sql/
│       ├── init_schemas.sql
│       └── bronze/
│           └── proc_load_bronze.sql
├── PROJECT_CONTEXT.md
```

### Execution rule

Scripts must be executed from project root.

```bash
cd ipca/
psql -d ipca -f data_engineering/sql/bronze/proc_load_bronze.sql
```

This is an operational invariant, not a suggestion.

---

## 8. Bronze Load Script: Functional Contract

The Bronze load procedure currently does the following:

1. starts a transaction
2. truncates Bronze target tables
3. creates temporary staging structures
4. loads source files via `\copy`
5. explodes JSON arrays into row form with `jsonb_array_elements`
6. filters the header row
7. applies explicit casting
8. inserts final records into Bronze tables
9. commits the transaction

### Script assumptions

- each JSON file is a single-line array
- execution starts from project root
- relative paths are valid
- source files match the currently expected extract format

If one of these assumptions changes, the load script may need revision.

---

## 9. Data Modeling Decisions Already Made

These decisions are intentional and should not be casually reversed.

- `mes_codigo` remains `INTEGER` in Bronze
- date conversion is deferred to Silver
- `NULLIF` is used before casting to avoid hard failures from empty values
- header rows are filtered explicitly rather than assumed away
- Bronze prioritizes faithful ingestion over semantic elegance
- duplication inherited from the source is tolerated in Bronze

Principle: Bronze optimizes for ingestion fidelity; Silver optimizes for relational usability.

---

## 10. Reproducibility and Idempotence

### Reproducibility goal
Running the same script against the same source files should produce the same resulting dataset.

### Current strategy
This is achieved through:

- full reload logic
- explicit typing
- deterministic script flow
- controlled file paths
- fixed source files

### Important nuance
The current Bronze process is operationally reproducible and behaves idempotently in practice because it resets target tables before reloading.

That does **not** yet mean the whole project has generalized incremental idempotence.
Incremental merge logic is intentionally postponed.

---

## 11. Versioning Strategy

Commits should reflect logical units of work.

### Expected commit grouping

1. schema initialization
2. Bronze DDL
3. Bronze load procedure
4. Silver DDL
5. Silver transformations
6. Gold views

### Versioning principles

- avoid mixing unrelated changes in one commit
- preserve readable project evolution
- do not introduce premature abstractions in the same commit as core logic
- keep infrastructure changes separate from data-model changes

---

## 12. Rules for AI Assistants Working on This Project

Any AI assistant supporting this repository must follow these rules:

### Must do

- respect current layer boundaries
- preserve controlled scope
- prefer SQL for Silver and Gold transformations
- preserve deterministic execution
- treat current design choices as intentional unless a strong technical reason exists to change them
- optimize for clarity, maintainability, and portfolio credibility

### Must not do

- suggest dashboards before core layers are complete
- introduce orchestration frameworks prematurely
- replace SQL transformations with pandas by default
- propose generic abstraction too early
- bloat the project with infra that does not improve the core DE signal
- optimize for “impressiveness” at the expense of engineering discipline

Default assumption: this project is evaluated on fundamentals, not on ecosystem complexity.

---

## 13. Current State

### Bronze status

- implemented
- operational
- validated
- reproducible
- stable under current assumptions

### Validated row counts

- Table 1737 load: 372 rows
- Table 7060 load: 1152 rows

### Current milestone

Design and implement the Silver layer.

---

## 14. Next-Step Priority

The next work should focus on Silver, specifically:

- defining cleaned relational structures
- converting time fields appropriately
- enforcing business granularity
- reducing redundant textual fields where justified
- preparing the model for Gold-layer analysis

Do not move to Gold until Silver is structurally sound.

---

## 15. Deferred Future Evolution

Only after Bronze and Silver are stable should the project consider:

- Dockerization
- incremental loading
- merge/upsert strategies
- data quality checks
- automated validation scripts
- controlled ingestion abstraction in Python

These are phase-2 improvements, not current priorities.

---

## 16. One-Sentence Project Identity

A SQL-first, PostgreSQL-based, end-to-end Data Engineering portfolio project using official IPCA data, designed to demonstrate disciplined ingestion, layered modeling, reproducibility, and strong scope control.
