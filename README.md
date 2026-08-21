---
title: Big Data AI Basic
emoji: 🦀
colorFrom: red
colorTo: gray
sdk: docker
pinned: false
license: apache-2.0
short_description: big-data-ai-basic
---


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

### 1. Build and run

The image is self-contained: it builds the Spring Boot jar from source and also carries
Postgres, MinIO and LakeKeeper, which `docker-entrypoint.sh` starts in dependency order.
One build, one container, no compose needed.

```bash
docker build -t big-data-ai-app .

docker run -d --name big-data-ai \
  -p 7860:7860 -p 8181:8181 -p 9000:9000 -p 9001:9001 \
  -v "$PWD/data":/data \
  --env-file .env \
  big-data-ai-app
```

| Port | Service |
|------|---------|
| 7860 | The app (`/` and `/admin`) — the only port Hugging Face Spaces publishes |
| 8181 | LakeKeeper (Iceberg REST catalog) |
| 9000 | MinIO API |
| 9001 | MinIO console (`minio` / `minio1234`) |

Publish only what you need — the services listen inside the container regardless.

Useful runtime switches:

| Variable | Default | Effect |
|----------|---------|--------|
| `DATA_ROOT` | `/data` | Where all persistent state lives |
| `PG_DUMP_INTERVAL` | `60` | Seconds between catalog dumps to `DATA_ROOT`. A dump is also taken on shutdown |
| `EMBEDDED_SERVICES` | `1` | Set to `0` to run only the app and rely on external services |

`compose.yaml` still exists and runs the same services as separate containers if you
prefer that topology. It is also where **Trino** lives (port 9999, configured by
`images/trino/lakekeeper.properties`) — the image deliberately does not bundle it, since
nothing in the app talks to it and Spaces would not be able to reach it anyway. Bring it
back with `docker compose up -d trino` when you want ad-hoc SQL over the warehouse. Note
`compose.kafka.yaml` includes `compose.yaml`, so do not delete the file.

## Storage on Hugging Face Spaces

A Space's `/data` is object-backed, not a POSIX block volume, and it is the only durable
storage Spaces offers - the container's own disk is wiped on every restart. Object storage
does not preserve file modes, does not store empty directories, and cannot promise a
durable `fsync` or an atomic rename.

Postgres depends on all of those, so **the cluster does not live on `/data`.** It is
created fresh on the container filesystem at `/app/pgdata` on every boot, where those
guarantees are real. Durability comes from `pg_dump` instead:

- Every `PG_DUMP_INTERVAL` seconds (default 60) the catalog is dumped to a scratch file
  and copied to `${DATA_ROOT}/lakekeeper-catalog.sql` **only if it changed** - a single
  sequential whole-file write, which is what object storage is good at.
- A final dump is taken on `SIGTERM`, so a normal restart loses nothing rather than up to
  an interval's worth of changes.
- At boot the dump is restored into the fresh cluster, then `lakekeeper migrate` runs, so
  a dump taken against an older LakeKeeper is brought up to the current schema.

The catalog is metadata only - on the order of 100KB - so this is cheap. The Iceberg data
files themselves live in MinIO and are never dumped.

`LAKEKEEPER__PG_ENCRYPTION_KEY` must stay stable across restores: LakeKeeper encrypts
stored credentials with it, and a restored dump carries ciphertext written under whatever
key was in force when it was taken.

### Migrating from the older in-place layout

Earlier versions kept the cluster on `/data` directly. On first boot the entrypoint
detects such a cluster, repairs it just enough to start (re-applying `0700` and recreating
the empty directories the object store dropped), dumps it, and renames it to
`postgres.migrated`. This runs once. The rename is kept rather than deleted so the old
cluster remains available if the dump turns out to be unusable - delete
`${DATA_ROOT}/postgres.migrated` yourself once you are satisfied.

## Deployment Topologies

The same services run in two different shapes depending on where the app is deployed.

**Locally**, `compose.yaml` runs each service as its own container, and they reach each
other by compose service name (`lakekeeper:8181`, `minio:9000`).

**On Hugging Face Spaces**, `compose.yaml` is never read. A Space with `sdk: docker`
builds the repo's `Dockerfile` and runs **exactly one container** — there is no compose
orchestration and no multi-container network. So the runtime image also carries Postgres,
MinIO and LakeKeeper, and `docker-entrypoint.sh` starts them in dependency order before
handing off to the app. They talk over loopback (`127.0.0.1:8181`, `127.0.0.1:9000`).

Trino is not in the image. Spaces publishes only `app_port`, so a bundled Trino would run
unreachable while costing ~1.2GB of image and ~1.5GB of RAM; it stays a compose-only
service instead.
This is the pattern the [HF Docker Spaces docs](https://huggingface.co/docs/hub/en/spaces-sdks-docker)
describe: extra services are installed *inside* the Space, and only `app_port` (7860) is
published publicly — Postgres, MinIO and LakeKeeper stay container-internal.

The endpoints are configuration, not hardcoded (`app.catalog.uri`, `app.catalog.base-url`,
`app.s3.endpoint` in `application.properties`). The defaults are the compose service names,
so local development needs no extra setup; the entrypoint exports the `APP_*` environment
overrides when it runs the embedded stack. Which mode the image uses is decided by
`EMBEDDED_SERVICES`, which defaults to `1` and is set to `0` by the compose `app` service.

Because the Space runs everything in one container, note that the app URL is the Space's
own subdomain, not the Hub page. `https://huggingface.co/spaces/<owner>/<name>` is a Hub
wrapper that embeds the app in an iframe and does not proxy sub-paths, so `/admin` there
404s. Use `https://<owner>-<name>.hf.space/admin` instead (for a private Space, in a
browser logged into Hugging Face).

## Data Persistence

Every piece of persistent state lives under one **data root**:

| Component | Persistent data | Local path | HF Spaces path |
|-----------|-----------------|------------|----------------|
| App (downloaded CSVs) | `data/house_prices/` | `./data/house_prices/` | `/data/app/house_prices/` |
| MinIO (Iceberg objects) | MinIO volume | `./data/minio/` | `/data/minio/` |
| LakeKeeper catalog | `pg_dump` of the metadata | `./data/lakekeeper-catalog.sql` | `/data/lakekeeper-catalog.sql` |

The Postgres cluster itself is **not** in this table: it is recreated on the container
filesystem at every boot and rebuilt from the dump above. See
[Storage on Hugging Face Spaces](#storage-on-hugging-face-spaces) for why.

- **Locally** it defaults to `./data` in the project directory, so `docker compose down`
  (or removing containers) does **not** wipe your large downloads or the MinIO state.
- **On Hugging Face Spaces** it defaults to `/data`, the persistent storage volume, which
  requires persistent storage to be attached to the Space. That volume is runtime-only and
  is not available during the Docker build. If it is missing or unwritable the entrypoint
  logs a warning and falls back to `/app/localdata`, so the Space still boots — but all
  state is then lost on every restart.
- You can override the location at any time:
  ```bash
  DATA_ROOT=/some/path docker compose up -d
  ```

The main app listens on **7860**, which is also Hugging Face Spaces' default
`app_port`, so the Space routes external traffic to it with no extra README config.
No `app_port` override is needed.

### 2. Configure LLM (Ollama)

This project uses Ollama for the LLM. Ensure you have Ollama running and a model pulled (e.g., `qwen3-coder`).

```bash
ollama pull qwen3-coder:latest
```


Update `src/main/resources/application.properties` with your Ollama configuration:

```properties
spring.ai.ollama.base-url=http://localhost:11434
spring.ai.ollama.chat.model=qwen3-coder:latest
```

### 3. Run the Application

This app is configured to run Spring Boot on port 7860

Run the Spring Boot application:

```bash
mvn spring-boot:run
```

The application will be available at `http://localhost:7860/`.

## Setup & Data Loading

Before you can query data, you need to set up the environment and load data. You can do this via the Admin interface.

1.  Navigate to the **Admin Page**: `http://localhost:7860/admin`
    (on Hugging Face Spaces: `https://<owner>-<name>.hf.space/admin`)

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
*   **Trino**: Ad-hoc SQL engine over the same Iceberg catalog, available via
    `compose.yaml` only (port 9999). Nothing in the app talks to it.

## Controllers

*   **`HomeController`**: Handles the main UI, query generation, and query execution.
*   **`AdminController`**: Manages data ingestion and system maintenance tasks.
*   **`IcebergController`**: (Internal) Additional Iceberg-specific operations.

## Acknowledgements

This project borrows the basic Docker setup, data sources (UK Price Paid data), and inspiration from the following project:
*   [PyData London 2025 Hands-on Apache Iceberg](https://github.com/andersbogsnes/pydata-london-2025-hands-on-apache-iceberg)
