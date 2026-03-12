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
  - to build and run `mvn compile` and `mvn exec:java -Dexec.mainClass="com.pipeline.DbIngestionApp"` inside of db-ingestion
  - to verify on kafka `docker exec -it kafka kafka-console-consumer \
--bootstrap-server localhost:9092 \
--topic sales-db \
--from-beginning`

- CSV Files
  - File System Source
  - to build and run `mvn compile` and `mvn exec:java -Dexec.mainClass="com.pipeline.FileIngestionApp"` inside of file-ingestion
  - to verify on kafka `docker exec -it kafka kafka-console-consumer \
--bootstrap-server localhost:9092 \
--topic sales-file \
--from-beginning`

- REST API
  - Traditional Web Service Source
  - to build and run `mvn compile` and `mvn exec:java -Dexec.mainClass="com.pipeline.ApiIngestionApp"` inside of api-ingestion
  - to send to kafka with request `curl -X POST http://localhost:8080/sales \
-H "Content-Type: application/json" \
-d '{"id":"501","city":"Chicago","salesman":"Bob","amount":900}'` in other terminal
  - to verify on kafka `docker exec -it kafka kafka-console-consumer \
--bootstrap-server localhost:9092 \
--topic sales-api \
--from-beginning`

With this all ingestion layer is done:
```
Postgres DB ──────► sales-db topic

JSON File ────────► sales-file topic

REST API ────────► sales-api topic
```


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