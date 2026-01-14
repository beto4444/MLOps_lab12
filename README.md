# Lab 12 - ML pipelines

This lab concerns creating ML pipelines, useful for organizing dataset creation
and model training workflows. We will use Apache Airflow for this purpose, as
arguably the most popular workflow orchestrator.

**Learning plan**
1. Apache Airflow basics
   - local setup
   - building DAGs
   - scheduling and backfilling
2. Airflow practical features
   - Docker Compose deployment, Celery executor
   - object storage XCom backend
   - connections, hooks, variables

**Necessary software**
- [Docker and Docker Compose](https://docs.docker.com/engine/install/), 
  also [see those post-installation notes](https://docs.docker.com/engine/install/linux-postinstall/)
- [uv](https://docs.astral.sh/uv/getting-started/installation/)
- [LocalStack CLI](https://docs.localstack.cloud/aws/getting-started/installation/)

Note that you should also activate `uv` project and install dependencies with `uv sync`.

**Lab**

See [lab instruction](LAB_INSTRUCTION.md). Laboratory is worth 5 points.

**Homework**

See [homework instruction](HOMEWORK.md). Homework is worth 10 points.

**Data**

We will be using [New York City yellow taxis](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
data about the trips of taxis in NYC for homework.
