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
PG_DUMP_INTERVAL="${PG_DUMP_INTERVAL:-60}"

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
export PGHOST=127.0.0.1
export PGPORT=5432

# The cluster deliberately does NOT live on DATA_ROOT. A Space's /data is object-backed
# and provides none of the guarantees Postgres depends on: it does not preserve file
# modes, it drops empty directories, and it cannot promise a durable fsync or an atomic
# rename - so a hard kill mid-write can corrupt the cluster with no error to show for it.
# The cluster therefore lives on the container's own filesystem, and durability comes
# from a periodic pg_dump into DATA_ROOT: one sequential whole-file write, which is
# precisely what an object store is good at.
export PGDATA=/app/pgdata

MINIO_DATA="${DATA_ROOT}/minio"
APP_DATA="${DATA_ROOT}/app"
CATALOG_DUMP="${DATA_ROOT}/lakekeeper-catalog.sql"
LEGACY_PGDATA="${DATA_ROOT}/postgres"

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
    MINIO_DATA="${DATA_ROOT}/minio"
    APP_DATA="${DATA_ROOT}/app"
    CATALOG_DUMP="${DATA_ROOT}/lakekeeper-catalog.sql"
    LEGACY_PGDATA="${DATA_ROOT}/postgres"
fi

echo "[init] data root: ${DATA_ROOT}"
mkdir -p "${MINIO_DATA}" "${APP_DATA}"

# IcebergService reads and writes ./data/house_prices relative to the working dir, so
# point /app/data at the persistent volume rather than the container's ephemeral layer.
if [[ ! -e /app/data || -L /app/data ]]; then
    ln -sfn "${APP_DATA}" /app/data
fi

# ---- Postgres (LakeKeeper's metadata store) -------------------------------------
start_postgres() {
    run_tagged postgres "${PGBIN}/postgres" \
        -D "$1" \
        -p "${PGPORT}" \
        -c listen_addresses=127.0.0.1 \
        -k /tmp
    wait_for postgres 60 "${PGBIN}/pg_isready" -h "${PGHOST}" -p "${PGPORT}" -U postgres
}

dump_catalog_to() {
    "${PGBIN}/pg_dump" -h "${PGHOST}" -p "${PGPORT}" -U postgres -d postgres --clean --if-exists > "$1"
}

# Earlier versions of this script kept the cluster on DATA_ROOT. Migrate one off that
# layout exactly once: repair it enough to start (the mode and the empty directories the
# object store dropped), dump it, then set it aside so this never runs again.
if [[ ! -f "${CATALOG_DUMP}" && -s "${LEGACY_PGDATA}/PG_VERSION" ]]; then
    echo "[init] found an in-place cluster from an earlier version; migrating it to a dump"
    chmod 0700 "${LEGACY_PGDATA}" 2>/dev/null || true
    for pgdir in pg_commit_ts pg_dynshmem pg_logical/mappings pg_logical/snapshots \
                 pg_notify pg_replslot pg_serial pg_snapshots pg_stat pg_stat_tmp \
                 pg_tblspc pg_twophase pg_wal/archive_status pg_wal/summaries; do
        mkdir -p "${LEGACY_PGDATA}/${pgdir}"
    done

    if start_postgres "${LEGACY_PGDATA}"; then
        if dump_catalog_to "${CATALOG_DUMP}.tmp"; then
            mv "${CATALOG_DUMP}.tmp" "${CATALOG_DUMP}"
            echo "[init] migrated the existing catalog to ${CATALOG_DUMP}"
        else
            echo "[init] WARNING: could not dump the old cluster; its catalog is lost" >&2
            rm -f "${CATALOG_DUMP}.tmp"
        fi
        "${PGBIN}/pg_ctl" -D "${LEGACY_PGDATA}" -m fast stop >/dev/null 2>&1 || true
    else
        echo "[init] WARNING: the old cluster would not start; continuing with an empty catalog" >&2
        pkill -f "postgres -D ${LEGACY_PGDATA}" 2>/dev/null || true
    fi
    mv "${LEGACY_PGDATA}" "${LEGACY_PGDATA}.migrated" 2>/dev/null || true
fi

# The cluster is on the container filesystem, so it is gone on every start. Build a fresh
# one and pour the dump back in - initdb takes a second or two on this data volume.
echo "[init] initialising the local Postgres cluster"
rm -rf "${PGDATA}"
mkdir -p "${PGDATA}"
chmod 0700 "${PGDATA}"
# trust auth is safe here: Postgres only ever listens on this container's loopback and
# its port is not among the ports HF exposes (only app_port is public).
"${PGBIN}/initdb" -U postgres --auth=trust -E UTF8 >/dev/null

start_postgres "${PGDATA}"

if [[ -f "${CATALOG_DUMP}" ]]; then
    echo "[init] restoring the catalog from ${CATALOG_DUMP}"
    if "${PGBIN}/psql" -v ON_ERROR_STOP=1 -h "${PGHOST}" -p "${PGPORT}" \
           -U postgres -d postgres -f "${CATALOG_DUMP}" >/dev/null 2>&1; then
        echo "[init] catalog restored"
    else
        echo "[init] WARNING: the catalog dump would not restore; starting empty." >&2
        echo "[init] WARNING: tables already written to storage will need reloading." >&2
    fi
else
    echo "[init] no catalog dump yet; starting with an empty catalog"
fi

# ---- MinIO (S3 storage backing the Iceberg warehouse) ---------------------------
export MINIO_ROOT_USER="${MINIO_ROOT_USER:-minio}"
export MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minio1234}"

run_tagged minio minio server "${MINIO_DATA}" --address :9000 --console-address :9001

wait_for minio 60 curl -fsS http://127.0.0.1:9000/minio/health/live

# ---- LakeKeeper (Iceberg REST catalog) ------------------------------------------
LK_DB_URL="postgresql://postgres@${PGHOST}:${PGPORT}/postgres"
export LAKEKEEPER__PG_DATABASE_URL_READ="${LK_DB_URL}"
export LAKEKEEPER__PG_DATABASE_URL_WRITE="${LK_DB_URL}"
# Must stay stable across restores: LakeKeeper encrypts stored credentials with it, and
# a restored dump carries ciphertext written under whatever key was in force before.
export LAKEKEEPER__PG_ENCRYPTION_KEY="${LAKEKEEPER__PG_ENCRYPTION_KEY:-P0d6Ye9v4rXDUpHUSj003yfF4E07SSBj}"
# LakeKeeper's metrics exporter defaults to port 9000, which is MinIO's port. Under
# compose they are separate containers and never collide; sharing one network namespace
# they do, and LakeKeeper responds by tearing down its background services. Move it.
export LAKEKEEPER__METRICS_PORT="${LAKEKEEPER__METRICS_PORT:-9191}"
export RUST_LOG="${RUST_LOG:-info}"

# Idempotent, and it runs after the restore so a dump taken against an older LakeKeeper
# is brought up to the current schema rather than being used as-is.
echo "[init] running LakeKeeper migrations"
lakekeeper migrate 2>&1 | sed -u 's/^/[migrate] /'

run_tagged lakekeeper lakekeeper serve

wait_for lakekeeper 60 lakekeeper healthcheck

catalog_host="http://127.0.0.1:8181"
s3_host="http://127.0.0.1:9000"

# ---- Catalog persistence --------------------------------------------------------
# Dump to a scratch file on the container filesystem and only copy it onto DATA_ROOT
# when the contents actually changed, so an idle system is not rewriting the object
# store every minute. The catalog is metadata only - kilobytes, not gigabytes.
# 0 = written, 1 = failed, 2 = unchanged since the last write.
persist_catalog() {
    local scratch=/tmp/catalog-dump.sql sum
    dump_catalog_to "${scratch}" 2>/dev/null || return 1
    # Postgres 18's pg_dump stamps a freshly randomised token into its \restrict and
    # \unrestrict lines on every run, so a byte-for-byte comparison always differs.
    # Hash the dump with those two lines excluded; the file written out keeps them.
    sum="$(grep -vE '^\\(un)?restrict ' "${scratch}" | sha256sum | cut -d' ' -f1)"
    [[ "${sum}" == "${last_dump_sum:-}" ]] && return 2
    cp "${scratch}" "${CATALOG_DUMP}.tmp" || return 1
    mv "${CATALOG_DUMP}.tmp" "${CATALOG_DUMP}" || return 1
    last_dump_sum="${sum}"
    return 0
}

catalog_dumper() {
    local last_dump_sum="" rc
    while true; do
        sleep "${PG_DUMP_INTERVAL}"
        rc=0; persist_catalog || rc=$?
        case "${rc}" in
            0) echo "[dump] catalog changed; persisted to ${CATALOG_DUMP}" ;;
            1) echo "[dump] WARNING: catalog dump failed" >&2 ;;
        esac
    done
}

catalog_dumper &
dumper_pid=$!

# ---- The app --------------------------------------------------------------------
# Point the app at the colocated services. Anything already set in the environment
# (an HF Space variable, say) wins, so these stay overridable without editing the image.
export APP_CATALOG_URI="${APP_CATALOG_URI:-${catalog_host}/catalog}"
export APP_CATALOG_BASE_URL="${APP_CATALOG_BASE_URL:-${catalog_host}}"
export APP_S3_ENDPOINT="${APP_S3_ENDPOINT:-${s3_host}}"

echo "[init] stack is up; starting the app on port ${SERVER_PORT:-7860}"

# The app runs as a child rather than via exec so this script stays PID 1 and can take a
# final dump on shutdown. A periodic dump alone would lose up to PG_DUMP_INTERVAL seconds
# of catalog changes on every restart.
java "${JAVA_ARGS[@]}" &
app_pid=$!

on_term() {
    trap - TERM INT
    echo "[init] shutting down; writing a final catalog dump"
    kill "${dumper_pid}" 2>/dev/null || true
    rc=0; persist_catalog || rc=$?
    [[ "${rc}" == "1" ]] && echo "[init] WARNING: the final catalog dump failed" >&2
    kill -TERM "${app_pid}" 2>/dev/null || true
    wait "${app_pid}" 2>/dev/null || true
    exit 0
}
trap on_term TERM INT

wait "${app_pid}" || true
echo "[init] the app exited; writing a final catalog dump"
kill "${dumper_pid}" 2>/dev/null || true
persist_catalog || true
