# Bonus labs — build-something-real options

Companion to [`WORKSHOP.md`](WORKSHOP.md). Twelve labs you can swap into the last slot, run as a
second session, or hand out as take-home work. Every one of them is **a feature you write**, not a
sequence of buttons — the deliverable is a diff.

Two things already in this repo make several of these much cheaper than they look:

- `images/kafka_connect/Dockerfile` already installs **`iceberg/iceberg-kafka-connect:1.9.2`**, and
  `compose.kafka.yaml` already brings up a broker, Schema Registry, Connect and a Redpanda console.
  The streaming lab is configuration, not plumbing.
- `compose.yaml` already runs **Trino** on `:9999` against the same LakeKeeper catalog with
  vended credentials (`images/trino/lakekeeper.properties`). The multi-engine lab is a JDBC URL.

## Two lanes — decide this before you publish the prerequisites

| Lane | Needs | Labs |
|------|-------|------|
| **Space lane** — everything runs on Hugging Face, nothing local | an HF account | P1, P2, P3, P4, R2, R1, R3, R4, A11, P15 |
| **Compose lane** — needs Docker on the laptop | Docker + ~8 GB RAM free | **P5** (Kafka), **P6** (Trino) — neither service is in the Space image |

The core workshop is deliberately Space-only. If you want the streaming or multi-engine labs, say
so in the prerequisites email — discovering it in the room costs you twenty minutes.

## Picking for the room

| If the room is mostly… | Run these |
|------------------------|-----------|
| Data / platform engineers | **P2**, P4, P3, P1 |
| Application & backend devs | **R2**, A11, P6, P1 |
| ML / AI engineers | **R1**, R2, R3, R4 |
| Streaming / Kafka people | **P5**, then P3 to clean up the mess it makes |
| Mixed, and you want one showstopper | **P2** (write–audit–publish) or **R2** (agentic SQL) |
| Team that lives in Cursor/Claude Code | **P15**, using any other lab as the spec |

## Dependencies worth knowing

- **R2, R1, R3, R4 all need F1's eval harness.** Without it they're vibes. With it, each one
  produces a number that moves — which is the entire pedagogical payload.
- **R2 needs F1's SQL guard**, and needs it more than the core workshop does: the model executes
  SQL autonomously in a loop.
- **P5 creates the small-file problem that P3 solves.** Running them back to back is the best
  version of both.
- **R4 closes a loop** — production traces become next month's eval set.

---

# Track A · Iceberg internals

## A11 — Question → SQL → chart
**30 min · easy-medium · Space lane** — *good closer when energy is low*

**Build** Code computes the result shape and decides which chart types are legal; a second
structured-output call picks one of those and names the axes; code builds the Vega-Lite spec and
renders it inline with htmx.

The interesting constraint is that the model has to reason about **result shape** — one numeric
column grouped by one categorical is a bar chart; a date series is a line; two numerics are a
scatter; forty thousand rows are a table and nothing else. Getting it to refuse to chart is harder
than getting it to chart.

**Concepts** Constrain-then-ask: the set of legal answers is computed in code the team can test,
and the model chooses inside it. Separating "what does the data say" from "how should it be shown";
graceful degradation. Expect to spend time on the rendering itself &mdash; Vega fails *silently*,
laying out zero-sized marks with no exception and nothing in the console, so "the call succeeded" is
a long way from "there is a chart on the screen".

**Why it's here** It's the most screenshot-able thing anyone will build all day, and short enough to
finish. Sometimes that's the right lab.

---

# Track E · Working with an agent

## R1 — The semantic layer, or: why text-to-SQL actually fails
**45 min · medium · Space lane · needs F1**

**Build** The layer that fixes the failures no model upgrade will.

Ask the current app *"how many flats sold in London last year?"* and it fails twice, for reasons
that have nothing to do with model quality:

- `property_type` is `'F'`, not `'Flat'` — a coded column with no legend in the prompt
- "London" is not a `town` in any useful sense — it's a `town` value *and* a dozen `district`
  values, and the right answer depends on which the user meant
- `town` vs `district` vs `county` is ambiguous even to a human reading the schema

So build:

1. **A data dictionary** — YAML mapping each coded column to its legend and synonyms
   (`F → Flat, Flats, Maisonette, Apartment`), injected into the prompt.
2. **A distinct-value index** for `town`, `district` and `county` — a few thousand strings,
   embedded through HF Inference Providers (feature extraction is served by `hf-inference`,
   Scaleway and Together).
3. **Entity resolution at query time** — pull candidate place names out of the question, resolve
   them against the index, and hand the model resolved literals plus a disambiguation note:
   *"LONDON matches town='LONDON' (1.2M rows) and 33 districts — ask or pick."*

**Then measure it with the F1 harness.** This routinely moves accuracy more than any model
upgrade in F2's bake-off, which is a genuinely useful and slightly deflating finding to land on a
room that just spent thirty minutes shopping for models.

**Concepts** Grounding by values and not just schema; entity resolution; the fact that most
text-to-SQL failure is a data-modeling problem wearing an AI costume.

---

## R2 — From one-shot generation to an agentic loop ⭐
**50 min · medium-hard · Space lane · needs F1**

**Build** Replace `AiService.generateQuery()`'s single call with a tool-calling agent that explores
the schema before it writes anything.

Spring AI 2.0 is already on the classpath and does this declaratively: `@Tool`-annotated methods
registered via `.defaultTools(...)`, with local tools and remote MCP tools sharing the same
`ToolCallback` interface — so anything built here also works through the MCP endpoint from R5.

Give it four or five tools:

| Tool | Why the model needs it |
|------|------------------------|
| `listTables()` | stop hardcoding the table name |
| `describeTable(name)` | the live schema, not a string constant that rots |
| `sampleValues(column, n)` | discovers that `property_type` is `D/S/T/F/O`, not `"Flat"` |
| `runSql(sql)` | **the guarded, LIMIT-capped executor from F1** |
| `explain(sql)` | check the plan before running something expensive |

The loop: explore → write → execute → **read the error** → fix → answer. The retry-on-error path is
the whole point; make them deliberately break a query and watch the model recover.

**Then measure it.** Run the same ten-question eval from F1 against one-shot and agentic:

| | accuracy | p50 latency | tokens/question |
|---|---|---|---|
| one-shot | | | |
| agentic | | | |

Accuracy usually jumps; latency and cost usually go up three to five times. **That table is the
lab.** "Should this be an agent?" becomes a question with an answer instead of a preference.

**Concepts** Tool calling; letting a model read its own errors; why autonomous execution makes the
F1 guardrail load-bearing rather than decorative; measuring an architecture change instead of
asserting it.

---

## R3 — Semantic query cache
**35 min · medium · Space lane · pairs with R1**

**Build** Embed each incoming question, compare against previously answered ones, and on a close
enough match reuse the **SQL** — never the results, because the data moves — skipping the LLM
entirely. Surface hit rate, latency saved and cost saved on a small dashboard.

**The lab is the failure mode.** *"Average price in Camden in 2015"* and *"…in 2016"* embed almost
identically and the naive cache returns confidently wrong SQL. The fix is to normalize entities and
literals out of the question before embedding — which is exactly the machinery R1 built. Have them
ship the naive version first, watch it break, then fix it. Nobody forgets a cache that lied to them.

**Concepts** Embeddings for retrieval rather than generation; similarity thresholds as a tunable
risk; caching a *plan* rather than a *result*; and the general rule that a cache key must contain
everything that changes the answer.

---

## R4 — Query observability, published as a dataset
**40 min · medium · Space lane · needs F1**

**Build** Instrument every question end to end: model id, provider, prompt and completion tokens,
latency, the generated SQL, whether it parsed, whether it executed, rows returned, and Iceberg's own
scan metrics (files scanned, bytes read).

Write those traces **to an Iceberg table** — the app dogfooding its own warehouse — then publish the
table as an HF dataset repo so the Dataset Viewer and DuckDB console work over it for free.

**The loop that closes:** production traces become next month's eval set. Questions that failed
become test cases. That flywheel is the thing senior engineers in the room will recognize
immediately and want to copy.

**Concepts** Observability as a product surface, not a log file; scan metrics as the bridge between
"the query was slow" and "the table needs P3"; treating your own telemetry as data worth modeling.

---

## P1 — Time travel and a snapshot diff view
**45 min · medium · Space lane**

**Build** A `/history` page listing every snapshot of the table, and a query page that can run
against any of them.

Iceberg's metadata tables are just SQL, which makes this far less work than it sounds:

```sql
SELECT * FROM lakekeeper.housing.staging_prices.snapshots;
SELECT * FROM lakekeeper.housing.staging_prices.history;
SELECT * FROM lakekeeper.housing.staging_prices.files;
SELECT * FROM lakekeeper.housing.staging_prices.partitions;
```

Then wire a snapshot picker into the existing query flow:

```sql
SELECT town, avg(price) FROM lakekeeper.housing.staging_prices
  VERSION AS OF 7834659812345678901 GROUP BY town;

SELECT ... FROM lakekeeper.housing.staging_prices TIMESTAMP AS OF '2026-09-01 10:00:00';
```

**The payoff** They double-load 2015 (everyone does — `load()` appends), and then fix it in one
statement instead of reloading:

```sql
CALL lakekeeper.system.rollback_to_snapshot('housing.staging_prices', <snapshot_id>);
```

**Concepts** Snapshot isolation; metadata tables as a first-class API; why "undo" is cheap in a
table format and terrifying in a warehouse.

**Stretch** `CALL system.create_changelog_view(...)` and render an actual row-level diff between two
snapshots.

---

## P2 — Write–audit–publish with branches ⭐
**50 min · hard · Space lane** — *the showstopper*

**Build** Ingestion that cannot publish bad data, using Iceberg branches as a staging area that
costs nothing.

Right now `load()` appends straight to `main`. Change it to:

1. Create a branch for the load: `ALTER TABLE lakekeeper.housing.staging_prices CREATE BRANCH ingest_2016`
2. Write into it (`SET spark.wap.branch=ingest_2016`, or `writeTo(...).option("branch", ...)`)
3. Run an **audit** — a new `AuditService` with real assertions:
   - no null `transaction_id`
   - `price > 0`
   - every `date_of_transfer` inside the year being loaded
   - row count within ±20% of the previous year's load
   - `record_status` in `A`/`C`/`D`
4. Show the results in the admin UI with a **Publish** and a **Discard** button
5. Publish = `CALL lakekeeper.system.fast_forward('housing.staging_prices', 'main', 'ingest_2016')`
   — atomic. Discard = `ALTER TABLE ... DROP BRANCH ingest_2016`.

**Why this one lands** Nobody reading queries against `main` ever sees a partially-loaded or
failed batch. That's a property most people's production pipelines don't have, achieved in an
afternoon, with no staging table, no second bucket, and no copy of the data. The demo — deliberately
corrupt a CSV row, watch the audit fail, watch `main` stay clean — is the best five seconds of the
workshop.

**Concepts** Branching and tagging; write-audit-publish; atomic multi-file commits; why data quality
gates belong *inside* the table rather than in an orchestrator.

**Stretch** Tag each published load (`CREATE TAG load_2016_v1`) with a retention policy, so an
auditor can query exactly what shipped.

---

## P3 — Compaction, and the economics of file size
**40 min · medium · Space lane** — *best paired with P5 or Core H4*

**Build** A Maintenance panel on `/admin`, plus the measurements that justify it.

Load eleven years, then look at what you actually made:

```sql
SELECT count(*) AS files, avg(file_size_in_bytes)/1024/1024 AS avg_mb
FROM lakekeeper.housing.staging_prices.files;
```

Then wire up the procedures and instrument each one — file count, total size, and query latency
before and after:

```sql
CALL lakekeeper.system.rewrite_data_files(
  table => 'housing.staging_prices',
  strategy => 'sort', sort_order => 'town ASC, date_of_transfer ASC');

CALL lakekeeper.system.rewrite_manifests('housing.staging_prices');
CALL lakekeeper.system.expire_snapshots(table => 'housing.staging_prices', older_than => TIMESTAMP '...');
CALL lakekeeper.system.remove_orphan_files(table => 'housing.staging_prices');
```

Then partition evolution, which is the part that surprises people:

```sql
ALTER TABLE lakekeeper.housing.staging_prices ADD PARTITION FIELD years(date_of_transfer);
```

Metadata-only. No data rewritten. Old files keep their old layout and still get read correctly.
Have them prove it by comparing the `files` count scanned before and after on a year-filtered query.

**Concepts** The small-file problem; sort orders and Z-ordering as a pruning strategy; hidden
partitioning; and the real tension worth arguing about — **`expire_snapshots` destroys the time
travel from P1.** Retention is a policy decision, not a default.

**Stretch** Schedule the whole thing as a Hugging Face Job on a cron, which is exactly how you'd run
it in production.

---

## P4 — MERGE, and making `record_status` mean something
**45 min · medium-hard · Space lane**

**Build** Idempotent ingestion. This lab fixes a real defect: loading the same year twice doubles
the rows, and nothing in the app prevents it.

Introduce a curated table and a proper merge:

```sql
MERGE INTO lakekeeper.housing.prices t
USING (SELECT * FROM staged_batch) s
ON t.transaction_id = s.transaction_id
WHEN MATCHED AND s.record_status = 'D' THEN DELETE
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

The Land Registry's `record_status` column (`A` add / `C` change / `D` delete) has been sitting
unused in the schema this whole time — the monthly change files are genuine CDC, and this lab is
where the column finally earns its place.

Then flip the write mode and measure both sides:

```sql
ALTER TABLE lakekeeper.housing.prices SET TBLPROPERTIES (
  'write.delete.mode'='merge-on-read',
  'write.update.mode'='merge-on-read');
```

**Concepts** Copy-on-write vs merge-on-read — write amplification against read amplification, with
numbers they generated; idempotency as a design property; delete files and how compaction reclaims
them.

**Stretch** Run the merge twice and prove row counts are identical. That's the test that should
have existed from the start.

---

# Track B · Streaming

## P5 — Kafka → Iceberg, live
**60 min · hard · compose lane**

**Build** A streaming ingest path into the same table people have been querying all day.

Everything you need is already in the repo: `compose.kafka.yaml` brings up the broker, Schema
Registry, Kafka Connect (with the Iceberg sink already installed by
`images/kafka_connect/Dockerfile`) and a Redpanda console on `:8001`.

```bash
docker compose -f compose.kafka.yaml up -d
```

The work:

1. A `/admin/simulate` endpoint in the Spring app producing synthetic property transactions to a
   topic — reuse the existing 16-column schema so it lands in the same table.
2. `POST` a connector config to Connect's REST API on `:8083`: the Iceberg sink pointed at the
   LakeKeeper REST catalog, `iceberg.tables=housing.staging_prices`, and a
   `iceberg.control.commit.interval-ms` they get to tune.
3. Keep the query page open and **watch row counts climb while queries keep returning**. Then watch
   the snapshot list from P1 grow one entry per commit interval.

**Concepts** The commit interval as the latency/file-size dial (drop it to 1s and watch P3's
small-file problem appear in real time — that's the lab's best moment); exactly-once via the
connector's control topic; readers never blocked by writers, which is the property that makes a
table format viable for streaming at all.

**Stretch** Two connectors writing to the same table concurrently. Iceberg's optimistic concurrency
handles it — have them find the retry in the logs.

---

# Track C · Open format, many engines

## P6 — One table, three engines
**35 min · easy-medium · compose lane**

**Build** A `/compare` page that runs the same SQL through Spark and Trino and shows both result
sets and both timings side by side.

Trino is already configured against the same catalog (`images/trino/lakekeeper.properties`, port
`9999`, vended credentials on). Add the Trino JDBC driver, run the query both ways, diff the rows,
and time each.

Then the moment that makes the point: **write from Spark while Trino is connected**, and re-run the
Trino query. New snapshot, immediately, no cache invalidation, no catalog reconfiguration, no export.

**Concepts** What "open table format" actually buys — engine per workload (Spark for the eleven-year
load, Trino for interactive BI, DuckDB for a laptop), zero copies, no lock-in. This is the lab for a
room that suspects lakehouse is a marketing word.

**Stretch** Add DuckDB's Iceberg extension as a third engine and put all three in the table. Then
ask which one you'd actually keep.

---

# Track D · AI engineering

## P15 — Drive a coding agent through an unfamiliar Java codebase
**40 min · any level · Space lane**

**Build** Any other lab in this document — but with an agentic coding tool, and with the lab's real
subject being *how you drive it*.

Hand them a spec (P1 and A11 work best; both are self-contained and visually verifiable) and have
them:

1. Write an `AGENTS.md` / `CLAUDE.md` for this repo first — where the Spark session is configured,
   that Thymeleaf fragments return `template :: fragment`, that admin endpoints are htmx `POST`s,
   that Iceberg procedures live under `<catalog>.system.*`. Fifteen minutes here saves an hour.
2. **Give the agent the F1 eval harness as its feedback loop**, so it can check its own work
   instead of asking. This is the single biggest difference between agent runs that converge and
   agent runs that wander.
3. Constrain the diff — one endpoint, one template, no refactors — and review it properly.

**Then compare across the room.** Who finished, what the agent got wrong, and where it confidently
invented an Iceberg procedure name that doesn't exist. Version-specific API surface is where these
tools reliably fail, and watching it happen live is a better lesson on verification than any
warning slide.

**Concepts** Context engineering as a real skill; automated feedback loops as the thing that makes
agents converge; reviewing generated code with the same rigour as a colleague's.

---

## Reality check on timing

These durations assume attendees write real code and hit real errors. If your room is mixed in
experience, ship a **starter branch per lab** with interfaces stubbed, the tests written, and the
implementation removed. Nobody learns anything from the twenty minutes spent finding where to add a
controller method, and the confident half of the room will finish the stretch goals anyway.
