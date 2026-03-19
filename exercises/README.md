# Exercises: Setup and Prerequisites

This directory contains runnable Python exercises that accompany the
[Data Lineage: From Novice to Expert](../index.md) guide.

---

## Python Version

All exercises require **Python 3.10 or later** (3.12 recommended). This project uses [pixi](https://pixi.sh) for environment and dependency management.

```bash
# Verify pixi is installed
pixi --version

# Python is managed by pixi — no separate install needed
pixi run python --version  # Should show 3.12+
```

---

## Environment Setup with Pixi

Pixi manages both the Python interpreter and all dependencies. From the repository root:

```bash
# Install all dependencies (creates .pixi environment automatically)
pixi install

# Activate the exercises environment
pixi shell -e exercises

# Or run commands directly without activating
pixi run -e exercises python exercises/ch04_first_graph.py
```

> **Note**: If the `exercises` environment is not yet defined in `pixi.toml`,
> you can add the exercise dependencies to the `dev` environment and use
> `pixi run -e dev` instead. See the pixi configuration section below.

---

## Install Dependencies

All exercise dependencies are declared in `pixi.toml`. Always prefer **conda
packages** (conda-forge) over PyPI. They include native binaries, handle
non-Python dependencies (e.g., Java for Spark, C libraries for Arrow), and
integrate cleanly with pixi's solver. Fall back to PyPI only when a package is
not available on conda-forge.

Add the following to your pixi environment feature:

```toml
# pixi.toml — conda-first dependencies under a feature
[feature.exercises.dependencies]
# --- Available on conda-forge (preferred) ---
networkx = "*"
matplotlib = "*"
pandas = "*"
pyarrow = "*"
scikit-learn = "*"
kedro = "*"
fastapi = "*"
uvicorn = "*"
httpx = "*"
mlflow = "*"
pyspark = "*"
neo4j-python-driver = "*"    # conda name for the neo4j Python driver
confluent-kafka = "*"
openai = "*"

[feature.exercises.pypi-dependencies]
# --- PyPI-only (not on conda-forge or conda version lags) ---
openlineage-python = "*"
sqllineage = "*"
apache-airflow = "*"
apache-airflow-providers-openlineage = "*"
marquez-python = "*"
openlineage-spark = "*"
dbt-core = "*"
dbt-postgres = "*"
great-expectations = "*"
langchain = "*"
chromadb = "*"
```

Then run:

```bash
pixi install
```

> **Why conda-first?** Conda packages are pre-compiled for your platform and
> include native dependencies. For example, `pyspark` from conda-forge bundles a
compatible JDK, and `pyarrow` ships with the Arrow C++ runtime, avoiding
> common "missing library" errors that occur with pip-only installs.

> **Tip**: You don't need every library to get started. Each exercise file lists
> its specific requirements in a comment at the top. Add only what you need
> for the chapter you're working on.

### Per-Chapter Requirements

| Exercise File | Chapter | Key Libraries |
|--------------|---------|---------------|
| `ch04_first_graph.py` | 4 | `networkx`, `matplotlib` |
| `ch05_openlineage_events.py` | 5 | `openlineage-python` |
| `ch06_sql_parsing.py` | 6 | `sqllineage` |
| `ch07_kedro_lineage.py` | 7 | `kedro`, `networkx` |
| `ch08_airflow_marquez.py` | 8 | `apache-airflow`, `apache-airflow-providers-openlineage` |
| `ch09_spark_lineage.py` | 9 | `pyspark`, `openlineage-spark` |
| `ch10_dbt_lineage.py` | 10 | `dbt-core`, `dbt-postgres` |
| `ch11_column_lineage.py` | 11 | `sqllineage`, `openlineage-python` |
| `ch12_neo4j_lineage.py` | 12 | `neo4j`, `networkx` |
| `ch13_lineage_api.py` | 13 | `fastapi`, `uvicorn`, `httpx`, `networkx` |
| `ch14_quality_lineage.py` | 14 | `great-expectations`, `networkx` |
| `ch15_observability.py` | 15 | `networkx`, `pandas` |
| `ch16_streaming_lineage.py` | 16 | `confluent-kafka`, `openlineage-python` |
| `ch17_governance.py` | 17 | `networkx` |
| `ch18_data_mesh.py` | 18 | `networkx` |
| `ch19_ml_lineage.py` | 19 | `mlflow`, `scikit-learn`, `openlineage-python` |
| `ch20_genai_lineage.py` | 20 | `langchain`, `chromadb`, `openai` |
| `ch21_scale.py` | 21 | `networkx` |
| `ch22_capstone/` | 22 | `networkx`, `fastapi` |

---

## Docker Services

Some exercises require external services running in Docker:

### Marquez (Chapter 8)

```bash
git clone https://github.com/MarquezProject/marquez.git
cd marquez
docker compose up -d
# Marquez API: http://localhost:5000
# Marquez UI:  http://localhost:3000
```

### Neo4j (Chapter 12)

```bash
docker run -d \
  --name neo4j-lineage \
  -p 7474:7474 \
  -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/lineage123 \
  neo4j:5
# Browser: http://localhost:7474
# Bolt:    bolt://localhost:7687
```

### Kafka + Zookeeper (Chapter 16)

```bash
docker compose -f - <<'EOF' up -d
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
  kafka:
    image: confluentinc/cp-kafka:7.5.0
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
EOF
```

---

## Running Exercises

Each exercise is a standalone Python script. Run them through pixi from the repository root:

```bash
# Run a specific exercise using pixi
pixi run -e exercises python docs/exercises/ch04_first_graph.py

# Or if you've activated the pixi shell
pixi shell -e exercises
python docs/exercises/ch04_first_graph.py

# Exercises print output and may generate files (e.g., PNG graphs)
```

### Exercise Structure

Every exercise file follows this pattern:

```python
"""
Chapter X Exercise: Title
========================
Description of what you'll build/learn.

Prerequisites:
    pixi install  # or: pixi add library1 (conda) / pixi add --pypi library2 (PyPI fallback)

Usage:
    pixi run -e exercises python chXX_exercise_name.py
"""

# --- Section 1: Setup ---
# Code and explanations...

# --- Section 2: Your Turn ---
# TODO: Implement the following function
def your_function():
    # HINT: Look at the example above
    pass

# --- Section 3: Verify ---
if __name__ == "__main__":
    # Run and verify your implementation
    your_function()
    print("Exercise complete!")
```

Look for `# TODO:` comments; those are where you write your code.
Solution hints are provided as `# HINT:` comments.

---

## Troubleshooting

### Common Issues

**`ModuleNotFoundError: No module named 'xxx'`**

Install the missing package: first try `pixi add xxx` (conda-forge). If the package isn't available on conda, fall back to `pixi add --pypi xxx`. Check the per-chapter requirements table above to see whether a package is conda or PyPI.

**Neo4j connection refused**

Ensure the Docker container is running: `docker ps | grep neo4j`. Wait 10-15 seconds after starting for Neo4j to initialize.

**Airflow import errors**

Airflow has many dependencies. If you only need the DAG definition for learning, you can read the exercise without running it, or use a standalone Airflow Docker setup.

**PySpark Java errors**

PySpark requires Java 11+. You can install it via pixi (conda) or your system package manager:

```bash
# Via pixi (recommended, adds to your environment)
pixi add openjdk=11

# Or via system package manager:
# macOS
brew install openjdk@11

# Ubuntu
sudo apt install openjdk-11-jdk
```

---

*Return to the [main guide](../index.md)*
