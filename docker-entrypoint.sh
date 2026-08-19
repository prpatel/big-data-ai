#!/usr/bin/env bash
#
# Brings the whole stack up inside a single container.
#
# Hugging Face Spaces builds only the Dockerfile and runs exactly one container -
# compose.yaml is never read there - so Postgres, MinIO and LakeKeeper have to run
# alongside the app rather than as sibling containers. This is the pattern the HF
# Docker Spaces docs describe ("you can install Elasticsearch inside your Space and
# call it internally on its default port 9200").
#
# Under docker compose the sibling containers already provide those services, so
# compose.yaml sets EMBEDDED_SERVICES=0 and this script does nothing but exec the app.
#
# Everything that must survive a restart lives under DATA_ROOT, which is /data on
# Spaces (the persistent storage volume, available at runtime only - never at build).

set -euo pipefail

EMBEDDED_SERVICES="${EMBEDDED_SERVICES:-1}"
DATA_ROOT="${DATA_ROOT:-/data}"

JAVA_ARGS=(
    --add-exports java.base/sun.nio.ch=ALL-UNNAMED
    --add-opens java.base/java.nio=ALL-UNNAMED
    --add-exports java.base/sun.util.calendar=ALL-UNNAMED
    -jar /app/app.jar
)

if [[ "${EMBEDDED_SERVICES}" != "1" ]]; then
    echo "[init] EMBEDDED_SERVICES=${EMBEDDED_SERVICES}; expecting sibling containers for postgres/minio/lakekeeper"
    exec java "${JAVA_ARGS[@]}"
fi

PGBIN=/usr/lib/postgresql/18/bin
export PGDATA="${DATA_ROOT}/postgres"
export PGHOST=127.0.0.1
export PGPORT=5432
MINIO_DATA="${DATA_ROOT}/minio"
APP_DATA="${DATA_ROOT}/app"

# Prefix a child process's output so the interleaved HF log stream stays readable.
run_tagged() {
    local tag="$1"; shift
    "$@" 2>&1 | sed -u "s/^/[${tag}] /" &
}

wait_for() {
    local label="$1" retries="$2"; shift 2
    for ((i = 1; i <= retries; i++)); do
        if "$@" >/dev/null 2>&1; then
            echo "[init] ${label} is ready"
            return 0
        fi
        sleep 1
    done
    echo "[init] ERROR: ${label} did not become ready after ${retries}s" >&2
    return 1
}

# /data only exists when persistent storage is attached to the Space. Without it,
# fall back to the container's own filesystem so the demo still runs - state is then
# lost on every restart, which is worth saying out loud rather than failing to boot.
if ! mkdir -p "${DATA_ROOT}" 2>/dev/null || [[ ! -w "${DATA_ROOT}" ]]; then
    echo "[init] WARNING: ${DATA_ROOT} is not writable - no persistent storage attached?"
    echo "[init] WARNING: falling back to /app/localdata; all state is lost on restart"
    DATA_ROOT=/app/localdata
    PGDATA="${DATA_ROOT}/postgres"
    MINIO_DATA="${DATA_ROOT}/minio"
    APP_DATA="${DATA_ROOT}/app"
    export PGDATA
fi

echo "[init] data root: ${DATA_ROOT}"
mkdir -p "${PGDATA}" "${MINIO_DATA}" "${APP_DATA}"

# IcebergService reads and writes ./data/house_prices relative to the working dir, so
# point /app/data at the persistent volume rather than the container's ephemeral layer.
if [[ ! -e /app/data || -L /app/data ]]; then
    ln -sfn "${APP_DATA}" /app/data
fi

# ---- Postgres (LakeKeeper's metadata store) -------------------------------------
if [[ ! -s "${PGDATA}/PG_VERSION" ]]; then
    echo "[init] initialising a new Postgres cluster in ${PGDATA}"
    # trust auth is safe here: Postgres only ever listens on this container's loopback
    # and its port is not among the ports HF exposes (only app_port is public).
    "${PGBIN}/initdb" -U postgres --auth=trust -E UTF8 >/dev/null
fi

# Postgres refuses to start unless PGDATA is exactly 0700 or 0750, and not every
# persistent volume preserves the mode initdb set - Hugging Face Spaces' /data comes
# back group/world-readable after a restart, which strands the cluster. Re-apply the
# mode on every boot rather than trusting what is on disk.
chmod 0700 "${PGDATA}" 2>/dev/null || true
pgdata_mode="$(stat -c %a "${PGDATA}")"
if [[ "${pgdata_mode}" != "700" && "${pgdata_mode}" != "750" ]]; then
    echo "[init] ERROR: ${PGDATA} is mode ${pgdata_mode} and could not be changed to 0700." >&2
    echo "[init] ERROR: Postgres will not start on this volume. If it cannot hold POSIX" >&2
    echo "[init] ERROR: modes, point DATA_ROOT at storage that can." >&2
    exit 1
fi

run_tagged postgres "${PGBIN}/postgres" \
    -D "${PGDATA}" \
    -p "${PGPORT}" \
    -c listen_addresses=127.0.0.1 \
    -k /tmp

wait_for postgres 60 "${PGBIN}/pg_isready" -h "${PGHOST}" -p "${PGPORT}" -U postgres

# ---- MinIO (S3 storage backing the Iceberg warehouse) ---------------------------
export MINIO_ROOT_USER="${MINIO_ROOT_USER:-minio}"
export MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minio1234}"

run_tagged minio minio server "${MINIO_DATA}" --address :9000 --console-address :9001

wait_for minio 60 curl -fsS http://127.0.0.1:9000/minio/health/live

# ---- LakeKeeper (Iceberg REST catalog) ------------------------------------------
LK_DB_URL="postgresql://postgres@${PGHOST}:${PGPORT}/postgres"
export LAKEKEEPER__PG_DATABASE_URL_READ="${LK_DB_URL}"
export LAKEKEEPER__PG_DATABASE_URL_WRITE="${LK_DB_URL}"
export LAKEKEEPER__PG_ENCRYPTION_KEY="${LAKEKEEPER__PG_ENCRYPTION_KEY:-P0d6Ye9v4rXDUpHUSj003yfF4E07SSBj}"
# LakeKeeper's metrics exporter defaults to port 9000, which is MinIO's port. Under
# compose they are separate containers and never collide; sharing one network namespace
# they do, and LakeKeeper responds by tearing down its background services. Move it.
export LAKEKEEPER__METRICS_PORT="${LAKEKEEPER__METRICS_PORT:-9191}"
export RUST_LOG="${RUST_LOG:-info}"

echo "[init] running LakeKeeper migrations"
lakekeeper migrate 2>&1 | sed -u 's/^/[migrate] /'

run_tagged lakekeeper lakekeeper serve

wait_for lakekeeper 60 lakekeeper healthcheck

catalog_host="http://127.0.0.1:8181"
s3_host="http://127.0.0.1:9000"

# ---- The app --------------------------------------------------------------------
# Point the app at the colocated services. Anything already set in the environment
# (an HF Space variable, say) wins, so these stay overridable without editing the image.
export APP_CATALOG_URI="${APP_CATALOG_URI:-${catalog_host}/catalog}"
export APP_CATALOG_BASE_URL="${APP_CATALOG_BASE_URL:-${catalog_host}}"
export APP_S3_ENDPOINT="${APP_S3_ENDPOINT:-${s3_host}}"

echo "[init] stack is up; starting the app on port ${SERVER_PORT:-7860}"
exec java "${JAVA_ARGS[@]}"
