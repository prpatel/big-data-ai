# Multi-stage build so the image is fully self-contained: the Spring Boot jar is
# built from source inside the image, so no pre-built target/*.jar is required.
# This works both locally and on Hugging Face Spaces (where target/ is not present
# because it is gitignored).
#
# The runtime stage also carries Postgres, MinIO and LakeKeeper. Hugging Face Spaces
# builds only this Dockerfile and runs a single container - compose.yaml is never read
# there - so the services compose would otherwise supply have to live in this image.
# See docker-entrypoint.sh, which starts them in dependency order; under compose the
# app service sets EMBEDDED_SERVICES=0 and only the jar runs.

FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build

# Resolve dependencies first for better layer caching.
COPY pom.xml .
RUN mvn -q -B dependency:go-offline || true

COPY src ./src
RUN mvn -q -B -DskipTests package

# Ubuntu-based (not alpine): the LakeKeeper binary is glibc-linked, and Postgres 18
# is packaged in this release's main archive - the same major version compose uses.
FROM eclipse-temurin:21-jre

# postgresql-client supplies pg_isready, used by the entrypoint's readiness wait.
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        postgresql-18 \
        postgresql-client-18 \
        curl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Static Go binaries, so copying them out of the official image is enough. Pinned to a
# release rather than :latest so an upstream push cannot silently invalidate this layer
# and everything built below it. compose.yaml uses the same tag.
COPY --from=minio/minio:RELEASE.2025-09-07T16-13-09Z /usr/bin/minio /usr/local/bin/minio
COPY --from=minio/minio:RELEASE.2025-09-07T16-13-09Z /usr/bin/mc    /usr/local/bin/mc

# Pinned to the same version compose uses.
COPY --from=quay.io/lakekeeper/catalog:v0.13.3 /home/nonroot/lakekeeper /usr/local/bin/lakekeeper

# Spaces runs the container as uid 1000, and Postgres refuses to run as root, so
# everything below this point runs as an ordinary user.
# Recent Ubuntu releases ship a stock "ubuntu" account already holding uid 1000,
# so it has to be removed before that uid can be reused.
RUN if id -u 1000 >/dev/null 2>&1; then userdel -r "$(id -un 1000)" 2>/dev/null || true; fi \
    && useradd -m -u 1000 user
WORKDIR /app

COPY --from=build --chown=user /build/target/big-data-ai-0.0.1-SNAPSHOT.jar app.jar
COPY --chown=user docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh && chown user /app

USER user

# 7860 is the app, and the only port Spaces publishes. The rest are container-internal
# there, but declared so `docker run -p` can surface them for local use:
#   8181 LakeKeeper   9000 MinIO API   9001 MinIO console   5432 Postgres
EXPOSE 7860 8181 9000 9001 5432

ENTRYPOINT ["docker-entrypoint.sh"]
