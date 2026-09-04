#!/usr/bin/env bash
#
# Local dev loop: build the jar on the host, then restart the app container that has
# it bind-mounted (see compose.dev.yaml). Roughly 9 seconds, and it never touches
# Postgres, MinIO or LakeKeeper - only the app container is restarted.
#
#   ./local-build-deploy.sh            build + restart + wait until it serves
#   ./local-build-deploy.sh -l         ... then follow the app log
#   ./local-build-deploy.sh -n         skip the build, just restart
#   ./local-build-deploy.sh -d         stop the whole stack and exit
#
set -euo pipefail

COMPOSE=(docker compose -f compose.yaml -f compose.dev.yaml)
JAR=target/big-data-ai-0.0.1-SNAPSHOT.jar
URL=http://localhost:7860/admin

follow=0; build=1
for arg in "$@"; do
    case "$arg" in
        -l|--logs)  follow=1 ;;
        -n|--no-build) build=0 ;;
        -d|--down)  "${COMPOSE[@]}" down; exit 0 ;;
        -h|--help)  sed -n '3,11p' "$0"; exit 0 ;;
        *) echo "unknown option: $arg (try -h)" >&2; exit 2 ;;
    esac
done

started=$SECONDS

if (( build )); then
    echo "==> building the jar"
    # Offline keeps it fast; fall back to online in case a dependency was just added.
    if ! mvn -o -q -B -DskipTests package 2>/dev/null; then
        echo "    offline build failed - retrying online (new dependency?)"
        mvn -q -B -DskipTests package
    fi
fi

[[ -f "$JAR" ]] || { echo "no jar at $JAR - run without -n first" >&2; exit 1; }

# A bind mount with a missing source makes Docker create a directory at /app/app.jar,
# so the jar has to exist before the first `up`. It does, by here.
if [[ -n "$("${COMPOSE[@]}" ps -q app 2>/dev/null)" ]]; then
    echo "==> restarting the app container"
    "${COMPOSE[@]}" restart app >/dev/null
else
    echo "==> starting the stack"
    "${COMPOSE[@]}" up -d app >/dev/null
fi

echo -n "==> waiting for $URL "
for _ in $(seq 1 60); do
    if curl -sf -o /dev/null "$URL" 2>/dev/null; then
        echo "- up in $((SECONDS - started))s"
        (( follow )) && exec "${COMPOSE[@]}" logs -f app
        exit 0
    fi
    echo -n "."
    sleep 1
done

echo " - TIMED OUT"
echo "last 30 lines of the app log:" >&2
"${COMPOSE[@]}" logs --tail 30 app >&2
exit 1
