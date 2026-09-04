# Every exercise, in one table

Consolidated index of the 32 exercises across [`WORKSHOP.md`](WORKSHOP.md) (core labs),
[`WORKSHOP-BONUS-LABS.md`](WORKSHOP-BONUS-LABS.md) (B*, technology-framed) and
[`WORKSHOP-FEATURE-LABS.md`](WORKSHOP-FEATURE-LABS.md) (PP-*, product-framed).

> A richer version of this index, with a short description of what each exercise actually builds,
> is published as an artifact — see the link in the workshop notes.

**Hands** is what attendees actually type — the single best predictor of how much help they'll need.
**Deploys** matters because each one is a ~90 s round trip on the push-to-Space loop.

| Legend | |
|---|---|
| Hands | `none` · `CLI` · `SQL` (through the existing query box) · `Java` · mixtures |
| Lane | **Space** = push-only, nothing local · **Local** = needs Docker on the laptop |
| ⭐ | strongest candidate in its category |

---

## A · Platform & deployment

| ID | Exercise | Time | Hands | Deploys | HF surface | Notes |
|----|----------|------|-------|---------|-----------|-------|
| **Lab 0** ⭐ | Ship the thing — Space, bucket volume, secret, first push | 30m | CLI | 1 | Spaces, Buckets, volumes, secrets | Mandatory. Everything depends on it |
| **Alt finale** | Dev Mode — SSH into the running Space, edit live | 20m | CLI + Dockerfile | 0 | Spaces Dev Mode | **PRO-gated.** Repo isn't Dev-Mode-compatible yet — making it so *is* the exercise |
| **B12** | Drive a coding agent through the codebase | 40m | any | varies | — | Meta-lab; uses another exercise as its spec |

## B · Iceberg & data engineering

| ID | Exercise | Time | Hands | Deploys | HF surface | Notes |
|----|----------|------|-------|---------|-----------|-------|
| **B1** ⭐ | Time travel, snapshots, rollback | 45m | SQL | 0 | — | Safest exercise in the whole set. No code, no deploys |
| **B2** ⭐ | Write–audit–publish with branches | 50m | SQL + Java | 2–3 | — | Best "showstopper". Hardest of the four |
| **B3** | Compaction, retention, partition evolution | 40m | SQL | 0 | — | Pairs with B5 (makes the mess) or Lab 4 (schedules the fix) |
| **B4** | MERGE / CDC upserts, copy-on-write vs merge-on-read | 45m | SQL + Java | 1–2 | — | Fixes the real double-load defect |
| **Lab 3** | Move the warehouse onto an HF bucket (S3 gateway) | 35m | Java (config) | 1–2 | **Buckets + S3 API** | **Highest risk.** Needs a presenter dry-run; fallback documented |
| **Lab 5** | Publish as a dataset, then Spark vs DuckDB vs viewer | 25m | CLI | 0 | **Datasets, viewer, DuckDB console** | Ends on the honest "did you need Spark?" finding |

## C · Streaming & multi-engine — **needs local Docker**

| ID | Exercise | Time | Hands | Deploys | HF surface | Notes |
|----|----------|------|-------|---------|-----------|-------|
| **B5** | Kafka → Iceberg, live | 60m | config + Java | n/a | — | Connector already installed in `images/kafka_connect` |
| **B6** | One table, three engines (Spark / Trino / DuckDB) | 35m | Java (JDBC) | n/a | — | Trino already wired in `compose.yaml` |

> Both are **Local lane** — neither service is in the Space image, so they need Docker and ~700 MB
> of pulls per laptop. Say so in the prerequisites or don't run them.

## D · AI engineering

| ID | Exercise | Time | Hands | Deploys | HF surface | Notes |
|----|----------|------|-------|---------|-----------|-------|
| **Lab 1** ⭐ | Trustworthy analyst — SELECT-only guard + 10-question eval harness | 40m | Java | 2–3 | Inference Providers | **Keystone.** B7/B8/B9/B10 and Lab 2 all measure against it |
| **Lab 2** | Model bake-off across providers and routing policies | 30m | Java (small) | 1 | **Inference Providers, `:fastest`/`:cheapest`** | Needs Lab 1. First half = make the model a runtime parameter |
| **B7** | One-shot → agentic tool-calling loop | 50m | Java | 2–3 | Inference Providers | Needs Lab 1's guard. Produces the accuracy-vs-latency table |
| **B8** | Semantic layer — coded values + entity resolution | 45m | Java | 2–3 | **HF embeddings (feature extraction)** | Usually beats any model upgrade. Deflating, in a good way |
| **B9** | Semantic query cache | 35m | Java | 2 | HF embeddings | The lab *is* the failure mode. Pairs with B8 |
| **B10** | Query observability, traces published as a dataset | 40m | Java + CLI | 2 | **Datasets, viewer** | Closes the loop: traces become next month's eval set |

## E · Scale & automation

| ID | Exercise | Time | Hands | Deploys | HF surface | Notes |
|----|----------|------|-------|---------|-----------|-------|
| **Lab 4** ⭐ | Ingest as an HF Job; right-size the hardware | 30m | CLI | 0 | **Jobs, scheduled Jobs, webhooks** | No Java at all — good for a tired room. Strong HF story |

## F · Agents & integration

| ID | Exercise | Time | Hands | Deploys | HF surface | Notes |
|----|----------|------|-------|---------|-----------|-------|
| **Lab 6** | Expose the lakehouse as an MCP tool endpoint | 20m | Java | 1–2 | Spaces as a tool endpoint | MCP starters are already on the classpath and auto-configuring, unused |

## G · Product features — the Meridian backlog

Same codebase, framed as tickets from a fictional property firm with named personas. Pick from
*either* this section or the technology sections above — mixing both framings in one afternoon
muddles which mode people are in.

| ID | Exercise | Time | Hands | Deploys | HF surface | Notes |
|----|----------|------|-------|---------|-----------|-------|
| **PP-101** ⭐ | Chart the answer (result shape → Vega-Lite) | 40m | Java | 2–3 | — | Most screenshot-able. The lesson is when to *refuse* to chart |
| **PP-102** ⭐ | Comparable sales + defensible valuation range | 60m | Java | 3+ | Inference Providers | Flagship. Teaches compute-first-narrate-second |
| **PP-103** | Watchlists and alerts | 50m | Java | 2–3 | — | Naive version fires constantly; that gap is the lab |
| **PP-104** ⭐ | New data lands → the business acts, unattended | 55m | Java + CLI | 2–3 | **Jobs, webhooks** | Where the audit gate earns its place |
| **PP-105** | Portfolio upload, address matching, review queue | 55m | Java | 3+ | — | 15% won't match — that's the design problem |
| **PP-106** ⭐ | Price per m² by joining EPC data | 60m | Java + CLI | 3+ | **Datasets** (stage the extract) | The "bring in a second dataset" lab. Coverage honesty is the lesson |
| **PP-107** | Monthly market commentary, every figure verified | 45m | Java | 2–3 | Inference Providers | The verify step is the build |
| **PP-108** | Flag implausible transactions without deleting them | 40m | SQL + Java | 1–2 | — | "Bad data" is a property of the question |
| **PP-109** | Who's likely to sell — a ranking, not a prediction | 45m | SQL + Java | 2 | — | Designing an eval with delayed feedback |
| **PP-110** | Ask it from Slack | 40m | Java | 2–3 | — | Distribution changes the design requirements |
| **PP-111** | Freshness & coverage page — "can I trust this?" | 30m | Java | 1–2 | — | Least glamorous, decides whether anything gets adopted |
| **PP-112** | Mix-adjusted repeat-sales index | 60m | SQL + Java | 3+ | — | Hardest. Needs PP-105's matching |

---

## Overlaps to avoid double-picking

| These are the same exercise | |
|---|---|
| **B11** (bonus catalogue) and **PP-101** | Identical — chart the result. B11 is the technology framing, PP-101 the product one |
| **B2** and the audit gate inside **PP-104** | PP-104 uses B2's branch-and-fast-forward mechanism as one step |
| **B10** and **PP-111** | Adjacent: B10 is telemetry as a dataset, PP-111 is a trust page. Either, not both |
| **Lab 5** and **B10**'s publishing step | Both end in `hf upload --type dataset` |

## Quick filters

| If you want… | Take |
|---|---|
| Zero Java, zero deploys | B1, B3, Lab 4, Lab 5 |
| Maximum HF surface area | Lab 0 → Lab 2 → Lab 4 → Lab 5 (+ Lab 3 if the dry run passes) |
| One showstopper | B2 or B7 |
| Most visible payoff | PP-101, then PP-102 |
| A business-feature day | PP-101 → PP-102 → PP-104 |
| A data-engineering day | B1 → B4 → B2 → B3 |
| An AI-engineering day | Lab 1 → Lab 2 → B8 → B7 |
| Lowest risk of stranding people | Lab 0, B1, Lab 4, Lab 5, PP-111 |
