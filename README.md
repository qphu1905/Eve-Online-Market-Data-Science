# Eve-Online-Market-Data-Science

This project builds a data pipeline using the **ETL** process to collect, transform, and store market data from EVE Online, a space MMORPG with a fully **player-driven economy**.
Market data is extracted from the publicly avaiable EVE Online ESI (EVE Swagger Interface) API endpoint, transformed, and loaded into a star schema database.

The goal is to provide a **reliable, scalable,** and **efficient** backend for analyzing market trends, item prices, and trading activity across major trade hubs.

---

## Tech stack
  - **Python**: Data extraction and transformation.
  - **Airflow**: Pipeline and job orchestration.
  - **Docker**: Containerization and deployment of the pipeline.
  - **MariaDB**: Database backend.

---

## Project Features

### Extraction 
  - Extract market data using the EVE Online ESI (EVE Swagger Interface) API endpoint.
  - Ultilize asynchronous requests to massively speed up extraction of data.

### Cleaning and Transformation**:
  - Normalize JSON payloads, remove duplicates, and handle missing values.
  - Enrich market data with analytical and technical indicators.

### Load
  - Data is model based on the star schema.
  - Transformed market data is loaded into a central fact table.
  - Facts such as items data, regions data, and more metadata stored in dimension tables.

### Deployment
  - Aiflow deployed using docker-compose.
  - ETL tasks containerized using Docker.
  - Containerized tasks orchestrated using Aiflow's DockerOperator API.
  - Cleanup tasks on success or failure of the pipeline included in pipeline DAG.

### To do list
  - Further transform data into analytics ready data (Gold Tier).
  - Create dashboard to display analytics metrics.