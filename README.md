# ENTSOE Project

Personal Project on using ENTSO-E data

## Repository Structure

This project uses a multi-environment setup to keep dependencies clean.
```
.
├── src/                # 🧠 MAIN LIBRARY: Core logic, Schemas, and Connectors.
├── tests/              # ✅ CORE TESTS: Unit tests for src/.
├── streamlit_app/      # 📊 FRONTEND: Lightweight UI tools.
├── dagster/            # ⚙️ ORCHESTRATOR: Heavyweight ETL definitions.
│   ├── pyproject.toml  #    (Has its own distinct environment)
│   └── uv.lock         #
├── catalog.yaml        # 📖 METADATA: Global data catalog.
├── config.yaml         # 🔧 CONFIG: Global settings.
├── pyproject.toml      # 📦 ROOT ENV: For Streamlit & Core development.
└── uv.lock             #
```


## Setup

### ENTSOE API Access

You will need API key to run the ingestion step of the pipeline - follow this [instruction](https://transparencyplatform.zendesk.com/hc/en-us/articles/12845911031188-How-to-get-security-token)

### Environment Variables

1. Copy the example environment
```bash
cp .env.example .env
```

2. Update `.env` with your credentials

### Install Dependencies 

#### Main src
1. Clone the repository:

```bash
git clone https://github.com/ejaebeen/entsoe-project.git
cd entsoe-project
```

2. Install dependencies using uv

```bash
# make sure that you have installed uv
uv sync
```

#### Dagster (Pipeline Orchestrator)

Please refer to [dagster/README.md](dagster/README.md)

## Usage

### Running Tests

WIP

## Configuration Guide

### Data Catalog

This is the Single Source of Truth for the data assets. It defines:

- Asset names and descriptions.
- Parameters for extraction (e.g., country codes, PSR types).
- Domain tags (Load vs. Generation).

[`CatalogItem`](src/entsoe_project/typed_defs/catalog.py) is the pydantic model of each data catalog item.   

Example entry:
```yaml
- name: raw_generation_belgium
  description: Ingest raw generation data for Belgium
  layer: ingestion
  group_name: entsoe
  tags:
    domain: generation
  kwargs:
    country_code: BE
```

### Schema

Polars version of Pandera is being used to define the schema of the data tables. Schemas are located in `src/entsoe_project/schema/` and is organised into a dict variable where the key is the name of the data table and value is `pa.DataFrameSchema`.

Example entry:
```python
SCHEMA = {
...
    "raw_load_belgium": pa.DataFrameSchema({
        "index": pa.Column(
            pl.Datetime("ns", "Europe/Brussels"),
            description="time of the measurement",
            nullable=False
        ),
        "Actual Load": pa.Column(
            float,
            description="Actual Load (MW)",
            checks=[pa.Check.ge(0)]
        )
    }),
...
}
```

### Config

