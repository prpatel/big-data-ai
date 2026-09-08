#!/usr/bin/env bash
#
# Local dev loop: build the jar on the host, then restart the app container that has
# it bind-mounted (see compose.dev.yaml). Roughly 9 seconds, and it never touches
# Postgres, MinIO or LakeKeeper - only the app container is restarted.
#
#   ./local-build-deploy.sh            build + restart + wait until it serves
#   ./local-build-deploy.sh -l         ... then follow the app log
#   ./local-build-deploy.sh -n         skip the build, just restart
#   ./local-build-deploy.sh -r         recreate the container, picking up .env/.env.local
#   ./local-build-deploy.sh -d         stop the whole stack and exit
#
set -euo pipefail

# .env.local layers machine-specific values (a local model endpoint, its key) on top of
# .env. Both have to be named explicitly once there is a second one: --env-file replaces
# compose's default .env rather than adding to it, so listing only .env.local would drop
# HF_TOKEN and DATA_ROOT.
COMPOSE=(docker compose)
if [[ -f .env && -f .env.local ]]; then
    COMPOSE+=(--env-file .env --env-file .env.local)
elif [[ -f .env.local ]]; then
    COMPOSE+=(--env-file .env.local)
fi
COMPOSE+=(-f compose.yaml -f compose.dev.yaml)
JAR=target/big-data-ai-0.0.1-SNAPSHOT.jar
URL=http://localhost:7860/admin

follow=0; build=1; recreate=0
for arg in "$@"; do
    case "$arg" in
        -l|--logs)  follow=1 ;;
        -n|--no-build) build=0 ;;
        -r|--recreate) recreate=1 ;;
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
if [[ -n "$("${COMPOSE[@]}" ps -q app 2>/dev/null)" ]] && (( ! recreate )); then
    # Note: restart does NOT re-read environment. Changing anything in .env or .env.local
    # needs -r, or the container keeps the values it was created with and the change looks
    # like it applied when it did not.
    echo "==> restarting the app container"
    "${COMPOSE[@]}" restart app >/dev/null
elif (( recreate )); then
    echo "==> recreating the app container (picks up .env / .env.local changes)"
    "${COMPOSE[@]}" up -d --force-recreate app >/dev/null
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
