# Workshop: Lakehouse + AI Analyst, entirely on Hugging Face

**Format** 3.5–4 hours, hands-on · **Audience** technical (backend / data / platform engineers)
· **Starting point** this repo, already deployable as a Docker Space.

> **Bonus material:** [`WORKSHOP-BONUS-LABS.md`](WORKSHOP-BONUS-LABS.md) has twelve further labs —
> build-a-feature depth, sorted by audience background — for the optional slot, a second session, or
> take-home work. [`WORKSHOP-FEATURE-LABS.md`](WORKSHOP-FEATURE-LABS.md) has twelve more, written as
> product tickets from a fictional client rather than as technology topics.

Attendees leave with a running Space of their own, an Iceberg warehouse whose data files sit in
their own Hugging Face bucket, an ingest job that runs on HF compute instead of their laptop, and
— the part they will actually reuse at work — a text-to-SQL eval harness and a model bake-off
they ran themselves.

---

## The design principle

Every lab has to pass two tests:

1. **Useful anywhere.** The skill transfers even if the attendee never opens huggingface.co again —
   catalog/storage separation, SQL guardrails, eval-driven model selection, batch/serving split.
2. **Better on HF.** The HF service isn't a detour; it's the shortest path to the result.

Labs that only pass test 2 are marketing. Labs that only pass test 1 could be run anywhere and
waste the venue. Every lab below is scored against both.

---

## Prerequisites — send these out a week ahead

Non-negotiable, and worth a reminder email the night before. Forty minutes of workshop time
disappears if people arrive cold.

**1. Create one token with Full Access.** At
[hf.co/settings/tokens](https://huggingface.co/settings/tokens): **New token** → type
**Fine-grained** → name it `workshop` → click the **Full Access** preset → **Create**. Copy it; it is
shown once.

Everything the day needs is covered: creating a Space and a bucket, pushing over git, calling
Inference Providers, starting Jobs, publishing a dataset, and the S3 credentials H2 derives from
it. There is a minimum set (see the appendix at the end of this file) but do not use it in a
workshop — a token missing one box authenticates perfectly and then fails with
`AccessDenied … Unknown`, naming no permission. That is unrecoverable in a room.

Everything lives in the attendee's **own namespace**, so only the *User permissions* section
matters. No organisation is involved.

**2. Install the CLI and log in:**

```bash
curl -LsSf https://hf.co/cli/install.sh | bash -s
hf auth login --token hf_xxx --add-to-git-credential   # --token required: browser login ignores the flag
hf auth whoami                            # confirms which account the token belongs to
```

**3. Run the pre-flight — two calls, and the second is the one that matters.**

```bash
# inference works, and the credit is on the account
curl -s https://router.huggingface.co/v1/chat/completions -H "Authorization: Bearer $(hf auth token)" -H "Content-Type: application/json" -d '{"model":"openai/gpt-oss-120b:cheapest","messages":[{"role":"user","content":"say ok"}]}'

# the token can WRITE - a read check passes with a read-only token and proves nothing
hf buckets create <you>/preflight --private
hf buckets delete <you>/preflight --yes
```

A completion and a clean create/delete means the day will work. Anything else means there is a week
to fix it rather than ten minutes.

- A **write** token, and separately a token with **inference** permission (or one fine-grained
  token carrying both — `Make calls to Inference Providers` + write).
- `git`, an editor, and any HTTP client they like.
- **No Docker. No Java. No Maven.** The workshop runs entirely on push-to-deploy, deliberately —
  see below.

**Optional, and only if it is done at home on home broadband:** the local loop needs
Docker Desktop, JDK 21 and Maven, and pulls roughly **700 MB per person** — about 220 MB of
container images (`postgres:18-alpine` 112 MB, `minio` 55 MB, `lakekeeper` ~50 MB compressed) plus
around 500 MB of Maven dependencies into `~/.m2` (Spark alone is 350 MB, Iceberg another 95 MB).
Across 25 people that is roughly **17 GB over the venue wifi**, which is not a thing to attempt in
a conference centre. Anyone who wants it should run `docker compose pull` and
`mvn dependency:go-offline` before they travel.

Optional but useful: an SSH key added at hf.co/settings/keys (needed for the Dev Mode finale).

---

## Presenter prep — do this a week out, not the night before

| # | Task | Why it matters |
|---|------|----------------|
| 1 | **Get credits onto each attendee's account**, and confirm at least one has landed before the day | Jobs need a positive credit balance, and so does inference past the free tier. Credits on their own account mean no org, no `--namespace`, and no `X-HF-Bill-To` — it all bills to them. Budget ~$2 each; $5 is generous. **The Space itself now costs too**: `--flavor cpu-upgrade` is a paid tier, billed for as long as the Space is awake, so tell them to pause it at the end of the day. |
| 2 | **Decide whether the Space runs on MinIO or on a bucket** | Both work. Bucket-backed means the data is real Parquet on the Hub from minute one, and H2 becomes a demo rather than an exercise; MinIO-backed keeps H2 as a hands-on lab. Either way, hand out the recipe — it is not discoverable. |
| 3 | Pre-download `pp-2015.csv` into a **public** HF bucket or dataset repo | The Land Registry origin is one shared 170 MB download for the whole room. Serving it from HF is faster *and* demonstrates the point. |
| 4 | Decide the **fallback venue** for the AI labs | If the venue's egress is bad, inference still works (it's a small API call) but Space builds may not. Have a pre-built Space per attendee as plan B. |
| 5 | Confirm **PRO** on your own account | Dev Mode (the finale) is PRO/Team/Enterprise only. Attendees on free accounts watch that one rather than do it. |

### How expensive is a rebuild, actually

Measured on this machine today, so the numbers are real rather than guessed:

| Build | Time |
|---|------|
| **First build**, `docker build --no-cache`, wall clock | **70 seconds** — 64 s of which is one step, `mvn dependency:go-offline`, pulling the Spark tree. `apt-get install postgresql-18` was 10 s; every other step under 5 s |
| **Subsequent push**, one Java file changed — on Hugging Face | **under 60 seconds**, from this Space's own build logs |
| The same warm rebuild locally | **~5 seconds** — `dependency:go-offline` CACHED, `mvn package` 3.1 s, every apt/MinIO/LakeKeeper layer CACHED |

Two things make this cheap, and both are already true of the Dockerfile as written:

- **The layer order is right.** `COPY pom.xml` and `dependency:go-offline` come before `COPY src`,
  so a source change never re-resolves the Spark dependency tree. That single 63-second step is
  paid once and then cached — which is why the warm rebuild is five seconds.
- **Hugging Face reuses Docker layers between commits to a Space**, so attendees get the warm path
  from their second push onwards. The gap between HF's minute and the local five seconds is image
  export and container start, not compilation.

**There is nothing here worth optimising.** An earlier draft of this document recommended publishing
a prebuilt base image to a registry so attendees' first build would be fast. That advice was built on
an estimate of 8–15 minutes for a cold build, which I never measured and which is wrong by an order
of magnitude. Seventy seconds, once, while you are still doing introductions, does not need a
prebuilt base image. The recommendation is withdrawn.

### What it costs you

Rough, for 25 attendees over 4 hours:

| Item | Basis | Estimate |
|------|-------|----------|
| Spaces — `cpu-upgrade`, now the documented default | 25 × 4 h × ~$0.03/h | ~$3 |
| Jobs — H4 | 25 × 15 min × $1.90/h (`cpu-performance`) | ~$12 |
| Jobs — if you use `cpu-upgrade` instead | 25 × 15 min × $0.03/h | ~$0.20 |
| Inference — F1, F2 | 25 × ~150 calls, small open models | ~$5–15 |
| Buckets | free allowance | $0 |

**≈ $20–40 total.** That number is itself a slide: the whole workshop costs less than lunch for
two people, because everything bills by the minute and idles at zero.

---

## Timeline

The spine is Labs 0–3. Everything after is chosen live based on how the room is doing.

| Time | Block |
|------|-------|
| 0:00 – 0:15 | Intro: what a lakehouse is, what the app does, **start your Space build now** (it builds while you talk) |
| 0:15 – 0:45 | **H1** — Ship the thing |
| 0:45 – 1:25 | **F1** — Make the analyst trustworthy |
| 1:25 – 1:35 | Break |
| 1:35 – 2:05 | **F2** — Model bake-off |
| 2:05 – 2:40 | **H2** — Move the warehouse onto Hugging Face |
| 2:40 – 2:50 | Break |
| 2:50 – 3:30 | **Pick one:** H4 (Jobs) · H3 (Publish + engine shootout) · R5 (MCP) |
| 3:30 – 3:50 | Demos: everyone's Space added to a shared Collection |
| 3:50 – 4:00 | Wrap, where to go next |

**Running only 3 hours?** Do 0, 1, 2 and demo H2 from the stage. F1 is the one that must
not be cut — it's the reason technical people came.

---

## How attendees iterate — push to the Space

**The default and documented loop is push-to-deploy.** Edit locally, commit, push, watch the build.
Nothing is installed and nothing is downloaded: after the clone, each push is a few kilobytes of
diff, and every heavy thing happens on Hugging Face's builders rather than on the venue's wifi.

| Step | Time |
|------|------|
| `git push hf main` | seconds — the diff is tiny |
| Build on HF, layers cached | under 60 s |
| Space restart (Postgres, LakeKeeper, MinIO, then the app) | ~20–40 s |
| **Round trip** | **about 1½ to 2 minutes** |

That is slower than a local restart and it is completely workable, provided the labs are designed
around it:

- **Batch the edit, then push.** Write the whole change and push once. Read the build log while it
  runs — `hf spaces logs --build --follow` — rather than sitting on the Space page.
- **Make the thing you're iterating on a runtime parameter.** This is worth more than any deploy
  trick. F2 is the example: rather than four deploys to try four models, spend the first push
  making the model a per-request option, then run the whole bake-off with no further deploys at all.
- **Space variables do not avoid a build.** Measured on 2026-09-02:
  `hf spaces variables add` puts the Space through `RUNNING → RUNNING_BUILDING →
  RUNNING_APP_STARTING`, and the build log gains a new entry. It takes the *cached* path so it is
  quick, and the old container keeps serving throughout (`RUNNING_BUILDING`, so no downtime) — but
  it costs about the same as a push. Use variables because they can't introduce a compile error,
  not because they're free.
- **Pair up.** Two people per Space halves the round trips and gives one person the build log while
  the other writes code. It is a better workshop for other reasons too.
- **Dev Mode is the real fix if the room has PRO** — SSH into the running Space, edit, hit Refresh,
  no rebuild at all. See the alternative finale.

---

## Local development — optional, and for you rather than the room

Verified on this machine on 2026-09-02. Attendees should not attempt this at the venue for the
bandwidth reasons above; it is how *you* build the material, and how anyone who set it up at home
can work faster during the day.

### The fastest loop — compose, with your jar mounted into the app container

**~9 seconds edit to live**, and it restarts *only* the app: Postgres, MinIO and LakeKeeper are
separate containers here, so they are never touched. Measured 2026-09-02.

Add a dev-only override — compose merges `compose.override.yaml` automatically, and it is the right
place for this because it keeps `compose.yaml` clean (add the filename to `.gitignore`):

```yaml
# compose.override.yaml — dev only
services:
  app:
    volumes:
      - ./target/big-data-ai-0.0.1-SNAPSHOT.jar:/app/app.jar:ro
```

```bash
docker compose up -d app                                  # services + app, jar mounted

mvn -o -q -B -DskipTests package && docker compose restart app     # ~9 s
```

| Step | Time |
|------|------|
| `mvn -o -DskipTests package` (warm, offline) | 6.3 s |
| `docker compose restart app` → serving | **3.1 s** |

Verified: the app container's `/app/app.jar` checksum matches the host's, and after the restart
`docker compose ps` showed `db`, `lakekeeper` and `minio` with unbroken uptime.

This is what the compose topology is *for*, and it is the answer to "can't I just restart the app
rather than everything?" — yes, but only where the app is its own container. In the single-container
image the entrypoint deliberately runs `java … &` and `wait`s on it so it can stay PID 1 and take a
final catalog dump on shutdown, so killing the JVM there stops the container.

### The faithful loop — single container, jar mounted

**~13 seconds edit to live**, in the full single-container topology — embedded Postgres, MinIO and
LakeKeeper, the real entrypoint, exactly what Spaces runs. Slower than the compose loop above
because `docker restart` takes the whole stack down and back up (6.3 s of it), but it is the one
that tells you the thing will actually work on Spaces. Use it before you push.

The entrypoint runs `java … -jar /app/app.jar`, so mounting your freshly built jar over that path is
all it takes:

```bash
docker build -t big-data-ai-app .        # once; ~5 s after the first time

docker run -d --name big-data-ai \
  -p 7860:7860 -p 8181:8181 -p 9000:9000 -p 9001:9001 \
  -v "$PWD/data":/data \
  -v "$PWD/target/big-data-ai-0.0.1-SNAPSHOT.jar":/app/app.jar:ro \
  --env-file .env big-data-ai-app
```

```bash
mvn -o -q -B -DskipTests package && docker restart big-data-ai    # ~13 s
```

Verified: `sha256sum /app/app.jar` inside the container matched the host's jar, and a changed
Thymeleaf template was served on the next request.

Both jar-mount loops sidestep the endpoint bug below, because the app sits inside a container that
shares a network with the services — so a single `app.s3.endpoint` value is correct for both the app
and LakeKeeper, and no `/etc/hosts` entry or `APP_*` override is needed.

### The alternative — services in containers, app on the host

Faster still (a Spring Boot restart rather than a container restart), but it needs a workaround and
it does not exercise the entrypoint.

**Add this line to `/etc/hosts` once** (needs sudo, and it is what makes it work — see the bug below):

```
127.0.0.1 minio lakekeeper
```

Then:

```bash
docker compose up -d lakekeeper      # pulls in db, migrate and minio via depends_on
                                     # — it does NOT start the app or trino
mvn spring-boot:run                  # no APP_* overrides needed
```

App on http://localhost:7860, MinIO console on 9001, and under compose the CSVs land directly in
`./data/house_prices`.

> This is better than the "run the container without publishing 7860" recipe currently in the
> README, which leaves a second, idle copy of the app running inside the container costing about a
> gigabyte of RAM. Starting `lakekeeper` alone gives you the services and nothing else.

#### The bug this avoids — worth understanding before a lab hits it

The README's from-source recipe says to override `APP_S3_ENDPOINT=http://localhost:9000`. That is
correct for the app's own S3 client, and **wrong for LakeKeeper**, because the same string is also
embedded in the warehouse definition the app POSTs to LakeKeeper — and LakeKeeper dials it too.
`application.properties` says as much: *"Both processes sit in the same network namespace in either
deployment, so a single value is correct for both."* With the app on the host and the services in
containers, that stops being true.

What happens next is genuinely hard to debug, and I reproduced it today:

- Inside the LakeKeeper container, `localhost:9000` is **LakeKeeper's own Prometheus metrics
  endpoint** (its metrics port defaults to 9000 — the Dockerfile moves it to 9191 for the
  single-container case, but `compose.yaml` does not).
- Warehouse validation does an S3 `GetObject` against that, gets Prometheus text back, and tries to
  gunzip it.
- LakeKeeper returns **`400 FileDecompressionError: invalid gzip header`**.
- `IcebergService.setup()` catches any 400 and prints **"Warehouse already exists - skipping..."**

So Setup Environment reports success, the log says the warehouse is already there, and
`GET /management/v1/warehouse` returns `{"warehouses":[]}`. Every later step fails for reasons that
look nothing like the cause.

The `/etc/hosts` line fixes it without touching code: `minio` now resolves for the host app (to the
published port) *and* for LakeKeeper (via compose DNS), so one string is correct for both again.

**The proper fix, if you want a lab out of it:** split the property in two —
`app.s3.endpoint` for the app's own client and something like `app.s3.catalog-endpoint` for the
value handed to LakeKeeper. Twenty minutes of work, and it makes the deployment topologies
independent. It would also be an honest addition to F1's list of defects, since the root problem
is the same one: a 400 swallowed into a success message.

### The full rebuild — when you change the Dockerfile or the entrypoint

The jar mount above bypasses the build, so it cannot test a change to `Dockerfile` or
`docker-entrypoint.sh`. For those, rebuild properly and run without the jar mount:

```bash
docker build -t big-data-ai-app .
docker run -d --name big-data-ai \
  -p 7860:7860 -p 8181:8181 -p 9000:9000 -p 9001:9001 \
  -v "$PWD/data":/data --env-file .env big-data-ai-app
```

### What was verified

| Check | Result |
|-------|--------|
| `docker build` from a clean context | **passes**, exit 0, 1.14 GB image |
| Base image drift — `eclipse-temurin:21-jre` | now Ubuntu 26.04 "resolute"; `postgresql-18` (18.6) still in main, so the Dockerfile's assumption holds |
| Pinned tags still published | MinIO `RELEASE.2025-09-07T16-13-09Z` ✓ · LakeKeeper `v0.13.3` ✓ — both with arm64 |
| Apple Silicon | builds and runs **native aarch64**, no emulation |
| Container startup | app, LakeKeeper and MinIO all answer 200; Spring Boot up in ~2 s after the services |
| Setup Environment | ✅ bucket · ✅ bootstrap · ✅ warehouse |
| Load → query | 500-row synthetic year loaded, `GROUP BY town` returned correct counts |
| Iceberg write path | Parquet under `warehouse/housing/staging/data/`, metadata JSON + Avro manifest alongside |
| `.snapshots` metadata table | readable — so bonus lab P1 works as written |
| Compose services-only | `docker compose up -d lakekeeper` starts db + migrate + minio + lakekeeper, and nothing else |
| App from source against those services | starts in ~20 s; bucket ✅ and bootstrap ✅, but **warehouse creation fails** — see the bug above |

Two incidental findings worth knowing:

- **The MCP server is already auto-configuring.** Startup logs show
  `McpServerAutoConfiguration` enabling resource, prompt and completion capabilities — the starter is
  live with zero tools registered. R5 / P6 is adding tools to a running server, not standing one up.
- **A harmless startup warning**: `NoProviderFoundException` from
  `OptionalValidatorFactoryBean` — no Jakarta Validation provider on the classpath. Nothing uses bean
  validation today. If a lab adds `@Valid` on a request parameter it will silently do nothing until
  someone adds `spring-boot-starter-validation`; worth knowing before a table loses twenty minutes to it.

## Two storage paths, and how each one is configured

This is the part that is genuinely confusing, so it is worth being explicit: a bucket-backed Space
writes to Hugging Face storage **two different ways**, and only one of them is a volume.

```mermaid
flowchart LR
    laptop["Your laptop<br/><code>git push hf main</code>"]

    subgraph space["Your Space — one container"]
        app["The app<br/>Spring Boot + Spark local[*]"]
        lk["LakeKeeper<br/>Iceberg REST catalog"]
        pg["Postgres<br/>holds the storage profile"]
    end

    bkt2["<b>Warehouse</b><br/>Iceberg parquet + metadata"]
    bkt1["<b>/data volume</b><br/>CSVs · exports · catalog dump"]

    laptop -->|builds and deploys| space
    app -->|"① Setup: POST the storage profile"| lk
    lk -->|"② persisted against warehouse 'lakehouse'"| pg
    pg -->|"③ pg_dump every 60s"| bkt1
    app -->|"④ parquet, via s3.hf.co"| bkt2
    app -->|"⑤ ordinary file writes"| bkt1
```

**① Setup Environment is where the location is stated — once.** The app takes the `APP_S3_*`
settings and POSTs them to LakeKeeper as a *storage profile*: endpoint, bucket, key-prefix, region,
STS flag, and the access key. Nothing else in the system hardcodes where data lives.

**② LakeKeeper remembers it**, stored in Postgres against the warehouse named `lakehouse`. That is
the answer to *"how does LakeKeeper know where to look?"* — you told it at Setup, and it kept it.

**③ Postgres is dumped to the volume** every 60 seconds and on shutdown, which is how the catalog
survives a restart. The cluster itself is rebuilt from that dump on every boot.

**④ Spark never reads `APP_S3_*` to find the data.** It asks the catalog. `SparkConfig` only knows
the LakeKeeper URL and the warehouse name `lakehouse`; when a table is created or loaded, LakeKeeper
replies with an `s3://…` location, and Spark writes Parquet straight to `s3.hf.co`. That is the
answer to *"how does the app know?"* — the catalog is the middleman, which is exactly what an
Iceberg REST catalog is for.

> The one exception is credentials. Normally LakeKeeper vends those too, but the HF gateway has no
> STS, so `APP_S3_CLIENT_SIDE_SIGNING=true` hands Spark the same access key to sign its own
> requests. That is why the key appears in two places: LakeKeeper needs it to validate and write
> metadata, Spark needs it to write data files.

**⑤ CSVs and exports are plain file writes** to the mounted volume. No S3, no catalog — just
`data/house_prices/…` relative to the working directory.

### Why two buckets

The volume and the warehouse are different mechanisms — a managed filesystem mount versus an S3
endpoint — so give them separate buckets. One bucket can serve both if the warehouse sits under its
own prefix, but that is untested here, and mixing Iceberg's UUID directories with `app/` and
`lakekeeper-catalog.sql` at the same root is harder to read when something goes wrong.

---

## Attendee setup — what it costs and buys

The commands live in **H1** below; this is the decision behind them, kept separate so the two do not
drift apart.

The entrypoint sees a non-local `APP_S3_ENDPOINT` and skips MinIO by itself; there is no switch to
set.

### What this costs, and what it buys

**Buys:** the warehouse is genuine Parquet on the Hub from the first load — openable with
`pd.read_parquet("hf://buckets/…")`, by DuckDB, by a Job, or in the Hub's file preview. One fewer
process (~120 MB). And it retires a quiet risk: the entrypoint keeps Postgres off `DATA_ROOT`
because object storage offers no atomic rename or durable `fsync`, and MinIO's `xl.meta` writes lean
on the same guarantees with no dump to fall back on.

**Costs:** step 5 is **UI-only, once per attendee** — no CLI, no API. Budget five minutes and expect
a few people to swap the access key and the secret. And **H2 stops being a lab**, because what it
teaches is now the starting state.

> If you would rather keep H2 hands-on, run the attendees' Spaces on MinIO — drop steps 3 (second
> bucket), 5's two `APP_S3_*` secrets and all of step 6 — and make only **your own** Space
> bucket-backed for the demo.

---

## H1 — Ship the thing (30 min)

**Goal** Everyone has a URL that answers a question in English, backed by their own data, with that
data stored on the Hub.

Attendees deploy by **pushing to the Space's git remote**, which is also the dev loop they will
use for the rest of the day: edit locally, commit, push, watch it build.

> **This path is proven end to end on a bucket-backed Space.** Setup registers the warehouse,
> `Load Data` writes 1,011,752 rows as real Parquet into a Hugging Face bucket in about 30 seconds,
> and an English question comes back answered. Everything below is what that took.

### Before anything: the token

One token does all of it. **Grant it Full Access** — it is one click, and every fine-grained
combination we tried cost more time than it saved.

If you insist on fine-grained, it needs all three or something later fails in a way that does not
name the cause:

- **Make calls to Inference Providers** — without it, `Generate Query` returns a 500 hiding
  `403 "does not have sufficient permissions to call Inference Providers on behalf of user <you>"`
- **Write access to repos** — creating the Space and pushing to it
- **Write access to buckets** — read alone authenticates perfectly and then refuses every upload

Each step below is one command that does one thing. Run them one at a time and read the output —
several of these fail in ways that only show up three steps later if you paste the lot.

```bash
# ---- 1. the code -------------------------------------------------------------
git clone https://github.com/prpatel/big-data-ai big-data-ai
cd big-data-ai
```

```bash
# ---- 2. the Space ------------------------------------------------------------
# cpu-upgrade, not the free cpu-basic: that is what the timings were measured on
hf repos create <you>/big-data-ai --repo-type space --sdk docker --private --flavor cpu-upgrade
```

```bash
# ---- 3. two buckets ----------------------------------------------------------
# lakehouse holds ordinary files: the CSVs, the catalog dump, the export
hf buckets create <you>/lakehouse --private

# warehouse holds the Iceberg table, reached over the S3 API and NEVER mounted
hf buckets create <you>/warehouse --private

# only lakehouse is mounted, as the Space's /data
hf spaces volumes set <you>/big-data-ai -v hf://buckets/<you>/lakehouse:/data

# confirm the mount took
hf spaces volumes ls <you>/big-data-ai
```

**4. S3 credentials — the one step with no CLI.** In the browser:
[hf.co/settings/tokens](https://huggingface.co/settings/tokens) → your token's **⋯** menu →
**Generate S3 credentials**. You get an access key starting `HFAK…` and a secret **shown once**.
Copy both somewhere before leaving the page.

```bash
# ---- 5. settings, from a file rather than seven flags ------------------------
# Write it first, read it back, then apply it. A typo is visible before it is live.
cat > space.env <<'EOF'
APP_S3_ENDPOINT=https://s3.hf.co
APP_S3_BUCKET=<you>
APP_S3_KEY_PREFIX=warehouse
APP_S3_STS_ENABLED=false
APP_S3_CREATE_BUCKET=false
APP_S3_CLIENT_SIDE_SIGNING=true
APP_WAREHOUSE_EXPLICIT_LOCATION=false
EOF

cat space.env                    # check <you> is your username in BOTH places
hf spaces variables add <you>/big-data-ai --env-file space.env
hf spaces variables ls <you>/big-data-ai      # must list all seven
```

> **`APP_S3_BUCKET` is your username, not `warehouse`.** HF buckets are addressed
> `namespace/bucket`, S3 refuses a `/` in a bucket name, and LakeKeeper refuses an endpoint with a
> path — so the namespace becomes the S3 bucket and the bucket name moves to `APP_S3_KEY_PREFIX`.
> It reads wrongly and is correct.
>
> **If `APP_S3_ENDPOINT` is missing, the Space silently comes up on an embedded MinIO.** The
> entrypoint decides from that one variable, and unset falls into the same branch as localhost.
> Nothing errors: the build succeeds, the app starts, the table is simply in the wrong place. That is
> why step 5 ends with `variables ls`.

```bash
# ---- 6. the three secrets ---------------------------------------------------
# One at a time, so a paste error names the secret it broke.
hf spaces secrets add <you>/big-data-ai -s HF_TOKEN=hf_xxx
hf spaces secrets add <you>/big-data-ai -s APP_S3_ACCESS_KEY=HFAK...
hf spaces secrets add <you>/big-data-ai -s APP_S3_SECRET_KEY=...

hf spaces secrets ls <you>/big-data-ai        # names only, never values
```

Do **not** set `APP_AI_BILL_TO` — see Billing, below.

```bash
# ---- 7. deploy ---------------------------------------------------------------
git remote add hf https://huggingface.co/spaces/<you>/big-data-ai
git push --force hf main
```

`--force` on the first push is expected: `hf repos create` gave the Space its own README commit, so
your local `main` is unrelated history. The Space is seconds old — overwrite it.

```bash
# ---- 8. watch it, then prove it is bucket-backed -----------------------------
# the BUILD log: a broken Dockerfile or a bad token shows up here
hf spaces logs <you>/big-data-ai --build --follow
hf spaces wait <you>/big-data-ai

# the RUNTIME log: the same command without --build. Keep this open all day —
# the admin buttons always say "... operation initiated" whether or not the work
# succeeded, and this is the only place the real outcome appears.
hf spaces logs <you>/big-data-ai --follow

# the one line that proves the warehouse is on your bucket and not on MinIO
hf spaces logs <you>/big-data-ai | grep EMBEDDED_MINIO
#   [init] EMBEDDED_MINIO=0; the warehouse lives in external storage (https://s3.hf.co)
```

Then at `https://<you>-big-data-ai.hf.space/admin`: **Setup Environment** → **Download Data**
(`2015`) → **Load Data** (`2015`). Watch the runtime log for each. Then ask the home page a question.

### Billing — the one thing that differs between you and them

`APP_AI_BILL_TO` sends `X-HF-Bill-To`, which bills inference to an **organisation** instead of to
the account the token belongs to. Both settings fail identically when wrong — a 500 hiding a 403
that reads like a broken token.

| Who | `APP_AI_BILL_TO` |
|-----|------------------|
| An attendee on credits granted to their own account | **unset** — the router bills the token's owner, which is them |
| A Space in an org, where the token's inference permission sits on the org | `<org-name>` |

If you are testing the attendee path from your own personal account, **leave it unset**. If you
deploy into an org and skip it, every query fails with *"on behalf of user &lt;you&gt;"* — because
without the header the router bills you personally, and your permission is on the org.

### The two buckets, because this is the confusing part

| | `<you>/lakehouse` | `<you>/warehouse` |
|---|---|---|
| Reached how | **mounted** as a volume at `/data` | **S3 API** over `s3.hf.co` |
| Holds | downloaded CSVs, `lakekeeper-catalog.sql` | Iceberg `data/` and `metadata/` |
| Why | Postgres's dump and the CSVs are ordinary files that must outlive the container | Spark and LakeKeeper speak S3 to it; a mount would be a second path nothing reads |

**Do not mount the warehouse bucket.** The app never resolves a filesystem path to it. It asks
LakeKeeper where the table is, and LakeKeeper answers from the storage profile that **Setup
Environment** registers.

**`APP_S3_BUCKET` is your namespace, not your bucket.** HF buckets are addressed
`namespace/bucket`, S3 will not take a `/` inside a bucket name, and LakeKeeper rejects an endpoint
that carries a path. So the namespace becomes the S3 bucket and the real bucket name slides down
into `APP_S3_KEY_PREFIX`. Get these two the wrong way round and the error will not tell you.

### Three things to say out loud while they run this

- **`--force` on the first push is expected, not a mistake.** `hf repos create` initialises the Space
  with its own README commit, so your local `main` has unrelated history. The Space is seconds old
  and empty; force-push it.
- **The README's YAML frontmatter is the Space's config.** `sdk: docker` is what makes it build the
  Dockerfile, and the app listens on 7860, which is HF's default `app_port`. If someone's Space
  builds and then 404s, that frontmatter is the first place to look.
- **`data/` is gitignored** and `.gitattributes` carries the standard HF LFS rules, so the push is a
  couple of hundred kilobytes. Nobody should ever commit a `pp-YYYY.csv`.

**What they learn** Docker Spaces are just a Dockerfile and a port; secrets vs. variables; a bucket
volume is how a Space gets state that survives a restart; that a catalog stores *where* a table is,
which is why the same query works against MinIO locally and the Hub in production.

**Talking point while builds run** Walk through `SETUP_FLOW.md` — the three admin buttons and what
each touches. This is dead time otherwise; use it.

### Traps to pre-empt — every one of these actually happened

- **`--add-to-git-credential` is silently ignored by the browser login.** It only applies when the
  token is passed with `--token`. Run `hf auth login` on its own and the flag does nothing, so the
  `git push` in step 6 has no credentials and prompts — which looks like a permissions problem and
  is not. Pass the token directly, which they have on the clipboard anyway.
- **The admin buttons always answer *"… operation initiated"*** whether or not the work succeeded.
  `AdminController` adds that message unconditionally, before the service call returns. Teach
  `hf spaces logs <space> --follow` in the first ten minutes; they will need it all day.
- **Setup is idempotent and self-healing — say so.** If a variable was wrong, fix it and click Setup
  again. It updates the existing warehouse's storage profile in place and logs
  `✅ Warehouse already existed; storage profile updated`. It did not always do this, and the failure
  mode was invisible: the stale profile survived every later Setup.
- **Do not try to fix a Space by deleting `lakekeeper-catalog.sql`.** The catalog dumper rewrites it
  within `PG_DUMP_INTERVAL` (60s) while the app is running, so the deletion silently undoes itself.
  Click Setup instead.
- **A warehouse-creation 400 is about credentials, not the gateway.** LakeKeeper validates by writing
  a probe object with its own S3 client, so it fails before Spark is ever involved:
  `IO Operation failed during Validation ... Unknown S3 error during write ... InternalError`.
  Re-generate the S3 credentials and click Setup again.
- **A private Space needs a session.** The browser is fine once they are logged in to HF, but anything
  scripted against `https://<you>-big-data-ai.hf.space` needs
  `-H "Authorization: Bearer $(hf auth token)"`, or it returns a 404 that looks like a broken deploy.
- **`Generate Query` returning a 500** is almost always the token: either it lacks *Make calls to
  Inference Providers*, or `APP_AI_BILL_TO` is set when it should not be (or the reverse). The real
  error is in the logs, never on the page.

## F1 — Make the analyst trustworthy (40 min)

The keystone lab. Three defects that were exercises in an earlier draft are now **already fixed in
the code they clone**, because attacking a working guard teaches more in forty minutes than building
one does — and because a room of twenty-five cannot all get a parser-based guard working before the
break.

| Already built | Where | What it does |
|---|---|---|
| `SqlGuard` | `app/SqlGuard.java` | parses the statement, rejects anything that is not a single read, caps rows |
| `SchemaStore` | `app/SchemaStore.java` | reads columns from the catalog with `DESCRIBE TABLE`, cached |
| `SqlAnswer` | `app/SqlAnswer.java` | the model returns JSON — `sql`, `columns_used`, `assumptions` |

Open `HomeController.runquery` and read it before anything else. **The interesting thing is that the
SQL is a form field.** `generatedsql` is a POST parameter — the generated query lands in an editable
textarea and whatever comes back gets executed. No prompt injection required.

### Exercise 1 — break the guard (15 min)

Type SQL straight into the box and press Run. Everything below has been tried; the point is to find
out *why* each answer is what it is.

| Attempt | What happens |
|---|---|
| `DROP TABLE lakekeeper.housing.staging_prices` | rejected — *would modify the warehouse (DropTable)* |
| `SELECT 1; DROP TABLE …` | rejected — fails to **parse**, so no second statement exists |
| `INSERT INTO … SELECT` | rejected — *that statement writes* |
| `MERGE INTO` / `DELETE FROM` | rejected |
| `CALL lakekeeper.system.rollback_to_snapshot(…)` | rejected — but with `NoClassDefFoundError`, not a parse failure |
| `WITH x AS (SELECT …) SELECT * FROM x` | **runs** — a CTE is still a read |
| `SELECT/**/town FROM …` | **runs** — comments do not change what the parser sees |

**The debrief is the last three rows.** A regex guard would have rejected the CTE and been fooled by
the comment; the parser gets both right, because it is asking Spark what the text *means* rather
than what it looks like. And the `CALL` case is why `SqlGuard` catches `Throwable` rather than
`Exception`: Iceberg's extended parser reaches for a Scala class that is not on the runtime
classpath and throws an `Error`. For a guard, "could not confidently parse this" and "will not run
it" are the same answer, whatever was thrown.

**The lesson, said out loud:** the model was never the vulnerability. An editable field that reaches
`spark.sql()` is.

### Exercise 2 — where a row limit belongs (10 min)

There is a **Rows to return** field next to the query box. Try 3, 7, then 500.

| Where you could put a limit | What happens |
|---|---|
| In the system prompt | the model complies *sometimes* — this project has the git history to prove it |
| At render time | `formatDataSet(df, n)` truncates, but `df.count()` already scanned everything |
| `.limit(n)` in the plan | actually bounds the work — what `SqlGuard` does |

500 comes back as 200: the per-query field is clamped by `app.sql.max-rows`. A user-supplied bound
still needs a bound.

### Exercise 3 — the deliverable: an eval set (20 min)

Everyone in the room has shipped an LLM feature with no way to tell whether a prompt change helped.
This is the smallest thing that fixes that, and F2 pays them back for it within the hour.

**The file** — `src/test/resources/eval/questions.csv`:

```csv
id,question,check_type,expected
q01,How many properties sold in Oxford in September 2015?,scalar,270
q02,What was the most expensive sale in Camden in 2015?,scalar,4750000
q03,Show me all flats sold in Bath,shape,property_type
q04,Average price by town in Surrey,shape,GROUP BY
q05,Which is the cheapest terraced house in Leeds?,scalar,42000
```

| `check_type` | Compares | Why it exists |
|---|---|---|
| `scalar` | the single value returned | unambiguous, no opinion about SQL style |
| `shape` | a substring the generated SQL must contain | catches "right answer, wrong reason" |
| `rowcount` | number of rows | for "show me all X" questions |

**Where the expected answers come from — attendees always stall here.** They do not invent them.
They *derive* them: run the query by hand in the box, read the answer, record it. Writing the ten
questions **is** the exercise, because it forces them to decide what "correct" means. `q01` above is
real — 270 is what the Oxford query returns against 2015.

**The runner** — `src/test/java/.../EvalHarnessTest.java`, and this skeleton is enough:

```java
@ParameterizedTest
@CsvFileSource(resources = "/eval/questions.csv", numLinesToSkip = 1)
void evaluates(String id, String question, String checkType, String expected) {
    long t0 = System.currentTimeMillis();
    String sql = aiService.generateAnswer(question).sql();      // generated?
    long latency = System.currentTimeMillis() - t0;

    var plan = spark.sessionState().sqlParser().parsePlan(sql);  // parses?
    Dataset<Row> result = spark.sql(sql);                        // executes?

    switch (checkType) {                                          // right answer?
        case "scalar"   -> assertThat(result.first().get(0).toString()).isEqualTo(expected);
        case "rowcount" -> assertThat(result.count()).isEqualTo(Long.parseLong(expected));
        case "shape"    -> assertThat(sql.toUpperCase()).contains(expected.toUpperCase());
    }
    System.out.printf("%s  %5d ms  %s%n", id, latency, checkType);
}
```

**The four gates are cumulative, and the order is the point:** *generated? → parses? → executes? →
right answer?* A model scoring 10/10/10/3 has a completely different problem from one scoring
10/4/4/4, and the lab only lands if they can see which.

**Two things to say before they start.** Ten questions is ten inference calls per run, on their own
credits — tell them to start with five. And **run it twice on the same model**: the scores will
differ. That non-determinism is the thing most of the room has never actually measured, and the
debrief question is *"how many runs before you would believe a prompt change helped?"*

**Useful anywhere** ✅ entirely portable · **Better on HF** ✅ the model swap in F2 is one string

---

## F2 — Model bake-off on Inference Providers (30 min)

Now the eval harness earns its keep — and **there is no code to write first.** The model is already
a runtime setting: the **Model Picker** at the bottom of `/admin` swaps it per request, with six
models across six providers preloaded and an **OTHER** box for any `model:provider` string.

```java
// AiService, already in the code they cloned
String model = modelStore.get();                                   // whatever /admin is set to
OpenAiChatOptions.Builder perRequest = OpenAiChatOptions.builder().model(model);
```

| Preloaded | Provider |
|---|---|
| `Qwen/Qwen3.6-27B` | ovhcloud |
| `Qwen/Qwen3.6-35B-A3B` | scaleway |
| `Qwen/Qwen3-Coder-Next` | novita |
| `openai/gpt-oss-120b` | groq |
| `google/gemma-3-27b-it` | deepinfra |
| `google/gemma-3-1b-it` | featherless-ai |

So the bake-off is: **set the picker, run F1's harness, write down the four scores and the latency,
repeat.** No deploys at all — which is the point, because at ~90 s a deploy, doing this the obvious
way would eat the hour.

Two things worth watching for, both real. `reasoning_effort` is not portable: `none` is fine on some
providers and returns `400: must be one of low, medium, or high` on others, so `AiService` drops it
and retries once rather than making the room care. And the 1B model is in the list deliberately —
it is the one most likely to ignore the JSON contract and hand back bare SQL, which is exactly the
failure F1's structured output makes visible.

> Don't reach for Space variables here. `SPRING_AI_OPENAI_CHAT_MODEL` does bind onto
> `spring.ai.openai.chat.model` via relaxed binding, but changing a variable puts the Space through
> `RUNNING_BUILDING` — it's a cached rebuild, roughly the cost of a push (measured 2026-09-02).
> Variables are useful because they can't introduce a compile error, not because they're free.

"the model is a request parameter, not a deployment" is also the right answer outside the workshop,
which makes this a good twenty minutes regardless of the bake-off.

```bash
# What's actually being served right now, and by whom
hf models list --warm --limit 40
hf models list --inference-provider groq --warm
hf models list --inference-provider cerebras --warm
```

Each attendee runs the ten-question eval against four configurations and fills in a table:

| Config | What it selects |
|--------|-----------------|
| `Qwen/Qwen3.6-27B:ovhcloud` | today's default — a pinned provider |
| `openai/gpt-oss-120b:fastest` | highest throughput available |
| `openai/gpt-oss-120b:cheapest` | lowest price per output token |
| `<a small model>` | is 27B even needed for text-to-SQL over 16 columns? |

Columns: **accuracy /10 · median latency · cost**. Then pool the room's results on a whiteboard.

Two settings that materially change the answer and are already in `application.properties`, so
have them try flipping each:

- `spring.ai.openai.chat.reasoning-effort=none` — the file records 21.7 s with thinking vs 1.2 s
  without. Does accuracy actually drop? Measure it, don't assume.
- `spring.ai.openai.chat.temperature=0.0` — raise it and watch reproducibility fall apart.

**What they learn** Provider routing as a real dial (`:fastest` / `:cheapest` / `:preferred` /
`:provider`); that the biggest model is often the wrong default; that "which model" is an empirical
question they now have the tooling to answer. Everything bills to each attendee's own credit
balance, so there is nothing to configure and nothing shared to exhaust.

**Useful anywhere** ✅ · **Better on HF** ✅✅ one token, one base URL, ~20 providers, no per-vendor
signup — this comparison is a week of procurement anywhere else

---

## H2 — Move the warehouse onto Hugging Face (5 min explanation, not a lab)

> **You chose bucket-backed Spaces, so this is already done** before anyone arrives — the warehouse
> is on a Hugging Face bucket from the first load. Keep it as a short explanation at the top of an
> hour: show the bucket page, open a Parquet file, and explain what would be there instead if MinIO
> were in the path (`<name>.parquet/xl.meta` directories nothing else can read). The mechanics below
> are what you would walk through, and what to fall back on if you switch attendees to MinIO and
> want it hands-on again.

**Verified end to end on 2026-09-05.** An Iceberg catalog knows *where* your table is; it doesn't
care whose object store that is. Cut MinIO out, put the data files in a Hugging Face bucket, and
change no query code.

```bash
hf buckets create <you>/warehouse --private
```

Then hf.co/settings/tokens → your token's ⋯ menu → **Generate S3 credentials** → an access key
starting `HFAK…` and a secret shown once. Configure the app:

```properties
app.s3.endpoint=https://s3.hf.co
app.s3.bucket=<your-namespace>       # the NAMESPACE, not the bucket - see below
app.s3.key-prefix=warehouse          # the bucket name goes here
app.s3.access-key=HFAK...
app.s3.secret-key=...
app.s3.sts-enabled=false
app.s3.create-bucket=false
app.s3.client-side-signing=true
app.warehouse.explicit-location=false
```

Reload a year, then open `https://huggingface.co/buckets/<you>/warehouse` and watch Iceberg's
`metadata/` and `data/` directories appear — as **real Parquet**, openable with
`pd.read_parquet("hf://buckets/…")`. That moment is the one people photograph, and it is the
difference between "the bytes are on the Hub" and "the data is on the Hub".

### Four things that must all be right

Each of the last three fails with an error pointing somewhere else entirely, which is why this lab
needs the recipe above rather than discovery:

| # | Requirement | What it looks like when wrong |
|---|-------------|-------------------------------|
| 1 | **AWS SDK ≥ 2.5x** with `apache-client` on the classpath | `403` with an empty body, or `ClassNotFoundException: ApacheHttpClient$Builder`. Iceberg 1.9.2 against a 2024-era SDK is the root cause; `S3FileIO` still builds an `ApacheHttpClient`, and that artifact stopped being transitive |
| 2 | **Endpoint with no path** | `Storage Profile 'endpoint' must not have a path`. HF buckets are `namespace/bucket` and S3 clients won't take a `/` in a bucket name, so the namespace becomes the S3 bucket and the bucket name becomes `key-prefix` |
| 3 | **No explicit `LOCATION`** | `Invalid location 's3://…'`. With a REST catalog the server places the table |
| 4 | **Token with read *and* write** on repo contents | `AccessDenied … Unknown` on `PutObject`, after a perfectly successful authentication. Read alone is not enough, and the S3 error names no permission |

> **Pre-flight, and make it a `PutObject`.** A `ListObjects` check passes with a read-only token and
> tells you nothing. Have attendees run an actual write before the lab.

### Why it is worth doing

Without this, the Space's MinIO stores objects under `${DATA_ROOT}/minio`, and `DATA_ROOT` is
already a Hugging Face bucket. So the data is on the Hub — as `…parquet/xl.meta` directories plus
opaque `part.1` files that only MinIO can read. Nothing else can open them: not pandas, not DuckDB,
not another Job, not the Hub's own file preview. Removing MinIO removes a translation layer that
sits between object storage and object storage, and turns the same bytes into something every tool
in the ecosystem understands.

## H4 — Get the ingest off the Space (30 min)

**The problem, stated honestly:** `Download` and `Load` run *inside an HTTP request*, on the Space's
2 vCPU, with no progress and no retry. One year is tolerable. Eleven years is not. This is the
batch/serving split, and it's an architecture lesson before it's an HF lesson.

Rewrite ingestion as a Job that mounts the same bucket:

```bash
hf jobs uv run ingest.py \
  --with duckdb --with huggingface_hub \
  -v hf://buckets/<you>/warehouse:/mnt/warehouse \
  --flavor cpu-performance \
  --timeout 1h

hf jobs logs <job-id> --follow
```

**Not `/data`.** Jobs reserves that path for its own artifacts when running a local script and
rejects the mount: *"Mount path '/data' is reserved for Jobs artifacts"*. Any other path works; H3
uses `/mnt/lakehouse` for the same reason.

Then right-size it: run the same job on `cpu-basic` ($0.01/h) and `cpu-performance` ($1.90/h) and
compare wall-clock against cost. The finding is usually that the expensive flavor is *cheaper per
job* — a genuinely counterintuitive result people remember.

**Stretch, pick one:**

```bash
# Monthly refresh — the Land Registry publishes new data every month
hf jobs scheduled run '0 6 1 * *' python:3.12 python ingest.py
```

```python
# Or: re-ingest whenever the upstream dataset repo changes
from huggingface_hub import create_webhook
create_webhook(job_id=job_id, watched=[{"type": "dataset", "name": "<org>/price-paid"}])
```

**What they learn** Never do heavy lifting in a request handler; hardware right-sizing as a
measurement not a guess; per-minute billing changes how you size things; a cron and a webhook turn
a script into a pipeline.

**Useful anywhere** ✅✅ · **Better on HF** ✅ no cluster, no scheduler, no IAM — one command, and
the volume mount means the Job and the Space share storage with zero glue

---

## H3 — Publish it, then check whether you needed Spark (25 min)

**Verified end to end on 2026-09-07**: 985,196 rows exported to 8 Parquet files (57 MB) and
published to a dataset repo by a Job, from a bucket-backed Space.

**Step 1 — export.** On `/admin`, click **Export to Parquet**. Spark reads the current snapshot and
writes `data/export/uk-price-paid/`, with the Parquet under `data/` and a dataset card beside it —
that layout matters, because `pandas` and `pyarrow` refuse a directory that mixes Parquet with
anything else.

**Step 2 — publish it with a Job**, not from the laptop:

```bash
hf jobs uv run scripts/publish-dataset.py \
  -v hf://buckets/<you>/lakehouse:/mnt/lakehouse \
  -e DATASET_REPO=<you>/uk-price-paid \
  -s HF_TOKEN \
  --flavor cpu-basic
```

**Why a Job and not `hf upload`.** The export is tens of megabytes each, times everyone in the room.
A Job mounts the same bucket the Space writes to and moves the bytes **inside** Hugging Face
infrastructure — nothing crosses the venue wifi. It is also the only place in the day that Jobs get
used for something real rather than as a demo.

**`/mnt/lakehouse`, not `/data`.** Jobs reserves `/data` for its own artifacts when running a local
script and refuses the mount outright: *"Mount path '/data' is reserved for Jobs artifacts"*. The
script defaults to `/mnt/lakehouse` to match.

The script finds the export itself, which is worth a word because the path is not obvious: on a Space
the entrypoint symlinks `/app/data` to `$DATA_ROOT/app`, so it lands at `app/export/<name>` in the
bucket, while under compose it is `export/<name>`. It checks both. Re-running is safe — it skips the
commit when nothing changed.

### Which bucket, and why it is not the one you expect

The Job mounts **`lakehouse`**, not `warehouse` — which reads backwards, so say it out loud:

| Bucket | Holds | Written by |
|--------|-------|------------|
| `warehouse` | the live Iceberg table: `<uuid>/<uuid>/data/*.parquet` and `metadata/` | Spark, over `s3.hf.co` |
| `lakehouse` (mounted at `/data`) | `app/export/<name>/` — plain Parquet plus a dataset card | the **Export to Parquet** button |

**Why not publish the warehouse Parquet directly?** It is already Parquet, and copying it would skip
a step. But the files in that bucket are not the table. Iceberg keeps superseded files alongside
current ones until snapshots expire, so after a compaction, an overwrite or a `MERGE`, a raw copy
**quietly republishes rows the table no longer contains**. Reading through the table makes Spark
resolve the current snapshot's manifest, so the export is what the table actually is — and you get
to pick the file count and give the files names a human can read.

### So what is the warehouse for?

This is the question the room will ask, and the answer is the point of the whole day.

**The warehouse is the definitive copy.** It is the thing Spark, Trino, Flink, PyIceberg and DuckDB's
Iceberg extension can all read *concurrently*, with snapshot isolation, time travel and safe
concurrent writes. What makes that work is that the table is not "the files in the bucket" — it is
**the catalog plus the manifests**, which say precisely which files belong to the current snapshot.
That indirection is what lets one engine compact while another reads, and it is why a plain `ls` of
the bucket tells you nothing.

**The export is not a second source of truth, it is a serving copy** — a flat, self-describing
snapshot for everything that does *not* speak Iceberg: the Hub's Dataset Viewer, `pd.read_parquet`,
someone's notebook. It is stale the moment the table changes, and that is fine, because that is what
publishing means.

If an attendee objects that this is two copies of the same data — good. That is the right instinct,
and the answer is that they are for different readers: one is a live table with transactions, the
other is a file anyone can download.

Then open the repo page: the **Dataset Viewer** and its SQL console are just *there*, for free, over
the data they loaded twenty minutes ago.

### Now the honest experiment. Same question, three engines

| Engine | How |
|--------|-----|
| Spark + Iceberg | the app's Run Query |
| DuckDB over the published Parquet | `hf datasets sql "SELECT town, avg(price) FROM 'hf://datasets/<you>/uk-price-paid/data/*.parquet' GROUP BY town"` |
| Hub Dataset Viewer SQL console | in the browser, no install |

`hf datasets sql` needs DuckDB locally (`brew install duckdb`, or `pip install duckdb`). If the room
has not got it, use the Hub's SQL console instead — same engine, nothing to install, and it makes the
point just as well.

Time all three on one year, then on eleven. **The crossover is the lesson.** For a single year on
one machine, DuckDB will very likely win, and saying so out loud buys enormous credibility — the
room already suspects it. Spark's case is the eleven-year load, the multi-node future, and Iceberg's
snapshots and schema evolution, not the single-node scan. Make that argument with numbers the
attendees generated themselves rather than asserting it from a slide.

**Useful anywhere** ✅✅ knowing when *not* to reach for Spark is a career skill · **Better on HF**
✅ the viewer, the SQL console and Xet dedup come free with the upload

---

## R5 — Turn the lakehouse into a tool an agent can call (20 min, finale)

`spring-ai-starter-mcp-server-webmvc` is already in `pom.xml` and **completely unused** — nothing
in `src/` mentions MCP. So there's a real, small, satisfying build here.

Expose two MCP tools over the Space's existing HTTP port:

- `describe_table()` → the live Iceberg schema
- `run_query(sql)` → the guarded executor from F1

Then point a coding agent at `https://<you>-big-data-ai.hf.space/mcp` and ask it a question in
English. It writes SQL, runs it against their warehouse, and reasons about the answer — with the
F1 guardrail refusing anything that isn't a `SELECT`.

The framing: a Space isn't just a demo page, it's a publicly addressable, permissioned tool
endpoint that any agent can use. And the guardrail from F1 is now load-bearing, because the
thing writing the SQL is fully autonomous.

**Useful anywhere** ✅✅ MCP is the integration surface everyone is being asked about ·
**Better on HF** ✅ a Space is a URL with auth and a scale-to-zero bill

---

## H5 — Dev Mode (PRO only)

If most of the room is on PRO, this lands better as a live fix-a-bug exercise, because a Docker
Space rebuild is 8–15 minutes and Dev Mode skips it entirely.

**This repo isn't Dev Mode compatible yet, and making it so is the exercise.** Against the
documented requirements, the Dockerfile already has: Debian base ✅, `/app` owned by uid 1000 ✅,
`curl` ✅. It's missing:

- `wget`, `procps`, `git`, `git-lfs` in the `apt-get install` line
- a **`CMD`** instruction — the image only has `ENTRYPOINT`

Add those four packages, convert `ENTRYPOINT` to `CMD`, push, then:

```bash
hf spaces dev-mode <you>/big-data-ai
hf spaces ssh <you>/big-data-ai        # or VS Code Remote-SSH to <subdomain>@ssh.hf.space
```

Edit a prompt in the running container, hit **Refresh**, see it live. Then `git commit && git push`
*from inside the Space* to persist it — changes made in Dev Mode are discarded otherwise, which is
a foot-gun worth demonstrating deliberately rather than discovering.

---

## Closing (20 min)

- Everyone makes their Space public and adds it to a shared **Collection** — instant gallery of 25
  working lakehouses, and a permanent artifact of the day.
- Pool the F2 bake-off numbers on the board. The winner is rarely the biggest model, and that
  slide writes itself from the room's own data.
- Show the org billing page. Four hours, 25 people, real infrastructure, ~$30.

**What to send afterwards:** the repo, the Collection link, the eval harness as a standalone gist,
and the one-line pitch for each service they touched.

---

## Risks, and what to do about them

| Risk | Mitigation |
|------|------------|
| 25 simultaneous cold Space builds at 0:15 | Not a real risk: the cold build is 70 s and it runs on HF's builders, not the venue's wifi. Everyone pushes during the intro and it is done before you finish talking. |
| Land Registry origin slow or down | Serve `pp-2015.csv` from your own public bucket (prep #3) |
| Attendees can't run Jobs — no credit balance | Everything bills to the workshop org: `--namespace`. Test with a free-tier account beforehand, not with the room. |
| H2 gateway incompatibility | Dry run + pinned JSON + the MinIO fallback, decided in advance |
| Spark OOM on a Space's 2 vCPU / 16 GB | One year only in the core path. Eleven years belongs in H4, where a Job has the RAM. |
| Room finishes early / late | Labs 4, 5 and 6 are independent — take one, two, or none |
| Dev Mode not available | It's PRO-gated; make it the alternative finale, not the main one |

---

## Why this set, and not the obvious one

The obvious Hugging Face workshop is *fine-tune a model, push it to the Hub, deploy it in a Space*.
There are a hundred of those, and the attendees who came to a data-engineering session don't want it.

This set instead uses HF as **infrastructure for a data platform** — object storage, batch compute,
a model router, a publishing surface, an agent endpoint — which is both the less-told story and the
one that actually maps onto the systems these attendees run at work. The Iceberg table on an HF
bucket is the image they'll describe to a colleague on Monday.

---

## Appendix — what "Full Access" actually grants

Every fine-grained permission Hugging Face offers, and whether this workshop touches it. Use
**Full Access** on the day; this table is for anyone who asks what they just agreed to.

| Group | Permission | Used here |
|-------|------------|-----------|
| Repositories | Read contents of your repos | ✅ clone, and H2's S3 reads |
| Repositories | Write contents/settings of your repos | ✅ create the Space and bucket, `git push`, publish the dataset, H2's S3 writes |
| Repositories | View access requests for your gated repos | — |
| Repositories | Read contents of public gated repos you can access | — |
| Inference | **Make calls to Inference Providers** | ✅ every AI lab |
| Inference | Make calls to your Inference Endpoints | — |
| Inference | Manage your Inference Endpoints | — |
| Jobs | **Start and manage Jobs** | ✅ H4, and publishing the dataset from a Job |
| Collections | Write to your collections | ✅ the closing demo only |
| Collections | Read your collections | — |
| Webhooks | Create and manage webhooks | ✅ H4 / P8 stretch only |
| Webhooks | Access webhooks data | — |
| Discussions & Posts | all three | — |
| Notifications | both | — |
| Billing | Read billing usage and payment method status | — |

**Why Full Access rather than the six that are ticked above.** A token missing one box
authenticates perfectly and then refuses the operation with `AccessDenied … Unknown`, naming no
permission — verified the hard way on 2026-09-05, where read-without-write cost an hour of
debugging with full context and a shell. In a room of twenty-five that is unrecoverable, and the
token is a throwaway scoped to one day's work on one account.
