# Data Pipeline Kata Challenge

## Must Create a Modern Data Pipeline with:
1. Ingestion for 3 different data sources (Relational DB, File system and Traditional WS-*)
2. Modern Processing with Spark, Flink or Kafka Streams
3. Data Lineage
4. Observability
5. Pipelines must have at least 2 pipelines:
  - Top Sales per City
  - Top Salesman in the whole country
6. The final Aggregated results mut be in a dedicated DB and API
7. Restrictions:
  - Python
  - Red-Shift
  - Hadoop

## Design
![](/design.png)

## Ingestion Data Sources

### Relational DB
- PostgreSQL
  - Relational DB Source

- CSV Files
  - File System Source

- REST API
  - Traditional Web Service Source


## Streaming/Processing
- Apache Kafka
  - Event Streaming

- Kafka Streams
  - Java
  - Processing

- Why Kafka Streams?
  - Works well with Java
  - Lightweight compared to Spark/Flink
  - Easy to run locally with Docker

## Data Lineage
- OpenLineage + Marquez

This automatically tracks:
- Where data came from
- Which pipeline processed it
- Where it went

## Observability
- Prometheus
  - Collect and Store metrics
- Grafana
  - Visualize in dashboards

Metrics from:
- Kafka
- Processing Service
- API

## Storage for Aggregated Data
- PostgreSQL(separated DB)

Table:

```
top_sales_per_city
top_salesman_country
```

## API
- NodeJS + Express

Endpoints:
```
GET /top-sales/city
GET /top-sales/salesman
```

## Infrastructure

Uses Docker Compose so everything runs locally.

Services:
```
postgres-source
postgres-analytics
kafka
zookeeper
kafka-streams-processor
mock-sales-api
prometheus
grafana
marquez
node-api
```