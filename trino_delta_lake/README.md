# Delta Lake with Trino POC

This POC demonstrates the integration of Delta Lake with Trino, using MinIO as the storage layer and Hive Metastore for table metadata management.

## Components

- MinIO: S3-compatible object storage
- Hive Metastore: Centralized metadata repository
- Trino: Distributed SQL query engine (Release 419)
- Delta Lake: Storage layer with ACID transactions
- Python data generator: Creates sample data using [delta-rs](https://github.com/delta-io/delta-rs)

## Prerequisites

- Docker
- Docker Compose

## Setup

1. Clone this repository
2. Run the following command to start all services:

```bash
docker-compose up -d
```

## Services and Ports

- MinIO:
  - S3 API: http://localhost:9000
  - Console: http://localhost:9001 (credentials: minio/minio123)
- Trino:
  - Web UI: http://localhost:8080
  - JDBC: jdbc:trino://localhost:8080
- Hive Metastore:
  - Thrift: thrift://localhost:9083

## Data Generation

The data generator will automatically create:
- 1000 client records
- 200,000 user records (partitioned by client_id)

## Querying Data with Trino

1. Connect to Trino using the CLI or your preferred client

docker exec -it trino_delta_lake-trino-1 trino --server localhost:8080 --user admin


2. [Register](conf/trino/register.sql) delta tables

3. Example queries:

```sql
-- List all tables
SHOW TABLES FROM delta.default;

-- Query users table
SELECT * FROM delta.default.users LIMIT 10;

-- Join users with clients
SELECT u.name as user_name, 
       c.name as company_name, 
       u.salary 
FROM delta.default.users u 
JOIN delta.default.clients c ON u.client_id = c.id 
LIMIT 10;

-- Query by partition
SELECT * FROM delta.default.users 
WHERE client_id = 1;
``` 