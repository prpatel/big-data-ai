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
| **H1** ⭐ | Ship the thing — Space, bucket volume, secret, first push | 30m | CLI | 1 | Spaces, Buckets, volumes, secrets | Mandatory. Everything depends on it |
| **H5** | Dev Mode — SSH into the running Space, edit live | 20m | CLI + Dockerfile | 0 | Spaces Dev Mode | **PRO-gated.** Repo isn't Dev-Mode-compatible yet — making it so *is* the exercise |
| **P15** | Drive a coding agent through the codebase | 40m | any | varies | — | Meta-lab; uses another exercise as its spec |

## B · Iceberg & data engineering

| ID | Exercise | Time | Hands | Deploys | HF surface | Notes |
|----|----------|------|-------|---------|-----------|-------|
| **P1** ⭐ | Time travel, snapshots, rollback | 45m | SQL | 0 | — | Safest exercise in the whole set. No code, no deploys |
| **P2** ⭐ | Write–audit–publish with branches | 50m | SQL + Java | 2–3 | — | Best "showstopper". Hardest of the four |
| **P3** | Compaction, retention, partition evolution | 40m | SQL | 0 | — | Pairs with P5 (makes the mess) or H4 (schedules the fix) |
| **P4** | MERGE / CDC upserts, copy-on-write vs merge-on-read | 45m | SQL + Java | 1–2 | — | Fixes the real double-load defect |
| **H2** ⭐ | Move the warehouse onto an HF bucket (S3 gateway) | 35m | Java (config) | 1–2 | **Buckets + S3 API** | **Verified working 2026-09-05.** Fiddly, not risky: four settings must be right and three fail with misleading errors, so hand out the recipe |
| **H3** | Publish as a dataset, then Spark vs DuckDB vs viewer | 25m | CLI | 0 | **Datasets, viewer, DuckDB console** | Ends on the honest "did you need Spark?" finding |

## C · Streaming & multi-engine — **needs local Docker**

| ID | Exercise | Time | Hands | Deploys | HF surface | Notes |
|----|----------|------|-------|---------|-----------|-------|
| **P5** | Kafka → Iceberg, live | 60m | config + Java | n/a | — | Connector already installed in `images/kafka_connect` |
| **P6** | One table, three engines (Spark / Trino / DuckDB) | 35m | Java (JDBC) | n/a | — | Trino already wired in `compose.yaml` |

> Both are **Local lane** — neither service is in the Space image, so they need Docker and ~700 MB
> of pulls per laptop. Say so in the prerequisites or don't run them.

## D · AI engineering

| ID | Exercise | Time | Hands | Deploys | HF surface | Notes |
|----|----------|------|-------|---------|-----------|-------|
| **F1** ⭐ | Trustworthy analyst — SELECT-only guard + 10-question eval harness | 40m | Java | 2–3 | Inference Providers | **Keystone.** R2/R1/R3/R4 and F2 all measure against it |
| **F2** | Model bake-off across providers and routing policies | 30m | Java (small) | 1 | **Inference Providers, `:fastest`/`:cheapest`** | Needs F1. First half = make the model a runtime parameter |
| **R2** | One-shot → agentic tool-calling loop | 50m | Java | 2–3 | Inference Providers | Needs F1's guard. Produces the accuracy-vs-latency table |
| **R1** | Semantic layer — coded values + entity resolution | 45m | Java | 2–3 | **HF embeddings (feature extraction)** | Usually beats any model upgrade. Deflating, in a good way |
| **R3** | Semantic query cache | 35m | Java | 2 | HF embeddings | The lab *is* the failure mode. Pairs with R1 |
| **R4** | Query observability, traces published as a dataset | 40m | Java + CLI | 2 | **Datasets, viewer** | Closes the loop: traces become next month's eval set |

## E · Scale & automation

| ID | Exercise | Time | Hands | Deploys | HF surface | Notes |
|----|----------|------|-------|---------|-----------|-------|
| **H4** ⭐ | Ingest as an HF Job; right-size the hardware | 30m | CLI | 0 | **Jobs, scheduled Jobs, webhooks** | No Java at all — good for a tired room. Strong HF story |

## F · Agents & integration

| ID | Exercise | Time | Hands | Deploys | HF surface | Notes |
|----|----------|------|-------|---------|-----------|-------|
| **R5** | Expose the lakehouse as an MCP tool endpoint | 20m | Java | 1–2 | Spaces as a tool endpoint | MCP starters are already on the classpath and auto-configuring, unused |

## G · Product features — the Meridian backlog

Same codebase, framed as tickets from a fictional property firm with named personas. Pick from
*either* this section or the technology sections above — mixing both framings in one afternoon
muddles which mode people are in.

| ID | Exercise | Time | Hands | Deploys | HF surface | Notes |
|----|----------|------|-------|---------|-----------|-------|
| **A11** ⭐ | Chart the answer (result shape → Vega-Lite) | 40m | Java | 2–3 | — | Most screenshot-able. The lesson is when to *refuse* to chart |
| **A10** ⭐ | Comparable sales + defensible valuation range | 60m | Java | 3+ | Inference Providers | Flagship. Teaches compute-first-narrate-second |
| **P7** | Watchlists and alerts | 50m | Java | 2–3 | — | Naive version fires constantly; that gap is the lab |
| **P8** ⭐ | New data lands → the business acts, unattended | 55m | Java + CLI | 2–3 | **Jobs, webhooks** | Where the audit gate earns its place |
| **P9** | Portfolio upload, address matching, review queue | 55m | Java | 3+ | — | 15% won't match — that's the design problem |
| **P10** ⭐ | Price per m² by joining EPC data | 60m | Java + CLI | 3+ | **Datasets** (stage the extract) | The "bring in a second dataset" lab. Coverage honesty is the lesson |
| **A12** | Monthly market commentary, every figure verified | 45m | Java | 2–3 | Inference Providers | The verify step is the build |
| **P11** | Flag implausible transactions without deleting them | 40m | SQL + Java | 1–2 | — | "Bad data" is a property of the question |
| **P12** | Who's likely to sell — a ranking, not a prediction | 45m | SQL + Java | 2 | — | Designing an eval with delayed feedback |
| **A13** | Ask it from Slack | 40m | Java | 2–3 | — | Distribution changes the design requirements |
| **P13** | Freshness & coverage page — "can I trust this?" | 30m | Java | 1–2 | — | Least glamorous, decides whether anything gets adopted |
| **P14** | Mix-adjusted repeat-sales index | 60m | SQL + Java | 3+ | — | Hardest. Needs P9's matching |

---

## Overlaps to avoid double-picking

| These are the same exercise | |
|---|---|
| **A11** appears twice | Once in the bonus catalogue (technology framing), once as a Meridian ticket (product framing). One exercise, pick either write-up |
| **P2** and the audit gate inside **P8** | P8 uses P2's branch-and-fast-forward mechanism as one step |
| **R4** and **P13** | Adjacent: R4 is telemetry as a dataset, P13 is a trust page. Either, not both |
| **H3** and **R4**'s publishing step | Both end in `hf upload --type dataset` |

## Quick filters

| If you want… | Take |
|---|---|
| Zero Java, zero deploys | P1, P3, H4, H3 |
| Maximum HF surface area | H1 → F2 → H4 → H3 (+ H2 if the dry run passes) |
| One showstopper | P2 or R2 |
| Most visible payoff | A11, then A10 |
| A business-feature day | A11 → A10 → P8 |
| A data-engineering day | P1 → P4 → P2 → P3 |
| An AI-engineering day | F1 → F2 → R1 → R2 |
| Lowest risk of stranding people | H1, P1, H4, H3, P13 |
