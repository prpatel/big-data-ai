# Iceberg/Spark/AI Demo

This project demonstrates an AI-powered analytics platform using Apache Iceberg, Apache Spark, and Spring AI. It allows users to query real estate data using natural language, leveraging an LLM to generate Spark SQL queries against an Iceberg data lake.

## Prerequisites

*   Java 21
*   Docker & Docker Compose
*   Maven

> [!WARNING] NOT FOR PRODUCTION USE
> 
> This project is for experimentation and prototyping. It bundles a spark master node inside of a Spring Boot project.
> A production Iceberg/Spark project would have a separate Spark system with master and worker nodes, and of course use
> authentication for the various services running in docker.

## Getting Started

### 1. Start Infrastructure

Start the required services (LakeKeeper, MinIO, etc.) using Docker Compose:

```bash
docker compose up -d
```

### 2. Configure LLM (Ollama)

This project uses Ollama for the LLM. Ensure you have Ollama running and a model pulled (e.g., `llama3`).

Update `src/main/resources/application.properties` with your Ollama configuration:

```properties
spring.ai.ollama.base-url=http://localhost:11434
spring.ai.ollama.chat.model=llama3
```

### 3. Run the Application

Run the Spring Boot application:

```bash
./mvnw spring-boot:run
```

The application will be available at `http://localhost:8080`.

## Setup & Data Loading

Before you can query data, you need to set up the environment and load data. You can do this via the Admin interface.

1.  Navigate to the **Admin Page**: `http://localhost:8080/admin`

2.  **Bootstrap Iceberg Catalog (LakeKeeper)**:
    *   The application attempts to bootstrap the project and create the warehouse bucket on startup.
    *   You can verify the setup in the logs or by checking the MinIO console (`http://localhost:9001`, user: `minio`, pass: `minio1234`).

3.  **Download Data**:
    *   On the Admin page, use the "Download Data" section.
    *   Enter a year (e.g., `2023`) to download specific UK Price Paid data, or leave it blank to download data from 2015-2025.
    *   Downloaded files will appear in the "Files already downloaded" list.

4.  **Load Data**:
    *   On the Admin page, use the "Load Data" section.
    *   Enter a year to load a specific file into the Iceberg table, or leave it blank to load all downloaded files.
    *   This process reads the CSVs using Spark and writes them to the `lakekeeper.housing.staging_prices` Iceberg table.

## Usage

### Home Page (`/`)

*   **Generate Query**: Enter a natural language question (e.g., "Show me the top 10 most expensive properties sold in London in 2023"). Click "Generate Query" to have the LLM convert this into a Spark SQL query.
*   **Run Query**: Review the generated SQL and click "Run Query" to execute it against the Iceberg table and see the results.

### Admin Page (`/admin`)

*   **Clear Data**: Removes all data from the Iceberg tables.
*   **Download Data**: Downloads raw CSV data files.
*   **Load Data**: Ingests CSV data into Iceberg.

## Architecture

*   **Spring Boot**: Web application framework.
*   **Spring AI**: Integration with LLMs (Ollama).
*   **Apache Spark**: Distributed data processing engine used for reading/writing data and executing queries.
*   **Apache Iceberg**: Open table format for huge analytic datasets.
*   **LakeKeeper**: Iceberg REST Catalog server.
*   **MinIO**: S3-compatible object storage.

## Controllers

*   **`HomeController`**: Handles the main UI, query generation, and query execution.
*   **`AdminController`**: Manages data ingestion and system maintenance tasks.
*   **`IcebergController`**: (Internal) Additional Iceberg-specific operations.

## Acknowledgements

This project borrows the basic Docker setup, data sources (UK Price Paid data), and inspiration from the following project:
*   [PyData London 2025 Hands-on Apache Iceberg](https://github.com/andersbogsnes/pydata-london-2025-hands-on-apache-iceberg)
