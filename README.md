# Data Engineering POCs

This repository contains various Proof of Concepts (POCs) for exploring different Data Engineering technologies and tools.

## Purpose

The goal of this repository is to experiment with and compare different data engineering technologies, formats, and tools through hands-on implementations and benchmarks.

**Note:** Results and performance characteristics can vary significantly depending on:
- **Use cases**: Different workloads (sequential scans, random access, aggregations, etc.)
- **Execution environment**: Hardware specifications (CPU, RAM, storage type), operating system, and system load
- **Data characteristics**: File sizes, data distribution, compression ratios, and schema complexity
- **Configuration**: Query engine settings, memory limits, and optimization parameters

These experiments provide insights into specific scenarios and should be evaluated in the context of your own use case and environment.

## Projects

### [benchmark_vortex_parquet](./benchmark_vortex_parquet/)

Benchmark comparing Parquet and Vortex data formats using Polars and DuckDB query engines on NYC taxi data.

- Compares performance of different data formats and query engines
- Includes both sequential scan and random access tests
- Generates detailed performance reports and visualizations

### [smart_city](./smart_city/)

FIWARE-based smart city demo: air quality monitoring with Orion Context Broker, QuantumLeap, CrateDB, and Grafana.

- Uses **AirQualityObserved** data model and Orion for context
- QuantumLeap stores time-series in CrateDB on entity updates
- Grafana dashboards for CO, temperature, and other metrics
- Docker Compose stack and Python scripts for setup and sensor simulation

### [trino_delta_lake](./trino_delta_lake/)

Proof of Concept for using Trino with Delta Lake for data querying and analytics.

### [trino](./trino/)

Proof of Concept for using Trino as a federated query engine over Postgres, MinIO (Apache Iceberg + raw Parquet), and ClickHouse on the NYC yellow taxi dataset.

- Shows how Trino catalogs map to different backends and how to join Iceberg tables with Postgres lookup tables
- Includes a ClickHouse star-schema ETL and Trino analytics queries as well. 

---

*Each project contains its own README with specific setup and usage instructions.*
