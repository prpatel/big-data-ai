# Feature labs — a product backlog, not a tech tour

Companion to [`WORKSHOP.md`](WORKSHOP.md) and [`WORKSHOP-BONUS-LABS.md`](WORKSHOP-BONUS-LABS.md).

Same codebase, opposite framing. The bonus labs are organised around technology (*"today we learn
branches"*). These are organised around **someone wanting something**, and the technology is
whatever it takes to give it to them. Hand attendees a ticket, not a tutorial.

## The setup

> You've joined the platform team at **Meridian Property Partners**, a mid-sized property firm.
> They have eleven years of Land Registry sales data in a lakehouse and an app that turns questions
> into SQL. Nobody outside the team uses it. Here is the backlog.

Five people file the tickets:

| | Who | What they actually care about |
|---|---|---|
| **Priya** | Market analyst | Client decks, monthly numbers, being able to explain a chart in a meeting |
| **Marcus** | Valuations | Defensible numbers. He signs his name to them. |
| **Ash** | Investor client | Never logging in. Being told when something matters. |
| **Dani** | Fund operations | A spreadsheet of 40 addresses and a quarterly deadline |
| **Sam** | Marketing | Words, on a schedule, in the house voice |

This framing does real work. *"Build a Vega-Lite renderer"* produces a Vega-Lite renderer.
*"Priya can't put a 200-row table in a client deck"* produces a conversation about which chart, why
that one, and when to refuse to chart at all — which is the part worth learning.

## How to run these

- **Pick two, maybe three.** These are richer than the tech labs; most are 45–60 minutes of real work.
- **Read the ticket aloud, then stop talking.** The acceptance criteria are the spec. Resist
  explaining the solution.
- **The "interesting problem" section is for you, not the handout.** It's what to steer toward when
  a table goes quiet, and what to draw out in the debrief.
- **Demo by persona.** "Show me as Marcus" beats "show me your code" and keeps the room honest about
  whether the feature actually works.

| Ticket | Feature | Time | Standout |
|--------|---------|------|----------|
| A11 | Chart the answer | 40 min | |
| A10 | Comparable sales &amp; a valuation range | 60 min | ⭐ |
| P7 | Watchlists and alerts | 50 min | |
| P8 | New data lands → the business acts | 55 min | ⭐ |
| P9 | Upload a portfolio, get it valued | 55 min | |
| P10 | Price per square metre (new dataset) | 60 min | ⭐ |
| A12 | The monthly market commentary | 45 min | |
| P11 | Which of these sales are wrong? | 40 min | |
| P12 | Who's likely to sell? | 45 min | |
| A13 | Ask it from Slack | 40 min | |
| P13 | Can I trust this number? | 30 min | |
| P14 | The mix problem, and a real index | 60 min | |

---

## A10 — "Give me comps I can defend" ⭐
**Marcus · 60 min**

> "I'm valuing 14 Acacia Road, SW11. I need the comparable sales and a range I'd be willing to put
> in front of a lender. Right now I'm doing this by hand and it takes forty minutes a property."

**What you ship** An address in; a comparable-sales table, a valuation range, and a short written
justification out.

**Acceptance criteria**
- Between 5 and 15 comparables, or an explicit *"insufficient comparable evidence"* — never a
  confident answer from two sales
- Selection rules are visible: same district, same property type, same tenure, within 12 months —
  and when the rules are relaxed to find enough evidence, **the relaxation is shown**
- A range, not a point estimate, derived from the comparable spread
- Every figure in the written justification traces to a row in the table

**The interesting problem** Comparable selection is *policy*, not a query. What counts as comparable
is a business rule with real consequences, and the rules must degrade gracefully: widen the radius,
then the time window, then the property type — announcing each step. The valuation range is a
statistics decision (interquartile range? trimmed mean? what do you do with six comps?). And the
model's only job is prose: it must not produce a number that isn't already in the table.

That last constraint — **compute first, narrate second** — is the single most useful pattern in this
whole document, and this is the ticket where it's most obviously right.

**Step-by-step build guide:** [https://claude.ai/code/artifact/fc399127-31ae-415b-a336-f7bf5c4232bd](https://claude.ai/code/artifact/fc399127-31ae-415b-a336-f7bf5c4232bd)

**Why it's the flagship** This is a product people pay for. It also happens to teach evidence-based
generation, graceful degradation, and the difference between an answer and a defensible answer.

---

## A11 — "Don't make me read a table"
**Priya · 40 min**

> "When I ask how prices have moved in Camden I get two hundred rows of numbers. I need the picture.
> I'm pasting these into client decks and I'm currently rebuilding every one of them in Excel."

**What you ship** Results render as the right chart automatically, with a download, and an override
when the automatic choice is wrong.

**Acceptance criteria**
- A time series renders as a line; one category plus one measure renders as bars; two measures
  render as a scatter
- Money is formatted as £ with sensible axis steps; dates are readable
- Results the tool shouldn't chart — more than two dimensions, tens of thousands of rows, a single
  scalar — render as a table **with a one-line reason**, not a broken chart
- Priya can override the chart type, and her override sticks for that question

**The interesting problem** Choosing the chart is easy; *refusing* is hard, and it's where every
naive version of this feature falls over. The decision is a function of result shape — cardinality,
column types, row count — not of the question. Push them to compute the shape first and let the
model choose within what's legal, rather than asking the model to decide freely and hoping. That
ordering (constrain, then ask) is the transferable lesson.

**Debrief question** "What happens when the query returns one number?" Most teams won't have
handled it, and a giant single-bar chart in a client deck is worse than a table.

**Step-by-step build guide:** [https://claude.ai/code/artifact/5d0a622c-5893-44db-8b9b-8552c7e16b11](https://claude.ai/code/artifact/5d0a622c-5893-44db-8b9b-8552c7e16b11)

<sub>Under the hood: code computes the result shape and decides which chart types are legal; a
structured-output call picks one of those and names the axes; code builds the Vega-Lite spec. The
model never returns a spec, so a bad reply can produce no chart but never a broken one.</sub>

---

## A12 — "Write the commentary. Every month. In our voice."
**Sam · 45 min**

> "Four paragraphs on the market, out on the third working day, in the house style. I currently
> write it from a spreadsheet Priya sends me and it takes half a day."

**What you ship** A generated draft with real figures, editable before it goes out.

**Acceptance criteria**
- **Every numeral in the output is machine-verified against the computed metrics** — a figure that
  isn't in the bundle fails the draft
- A failed verification regenerates, and after N attempts falls back to a template, rather than
  publishing something unverified
- House voice comes from examples of previous commentary, not from an adjective in a prompt
- Sam can edit, and the edit is what ships

**The interesting problem** The reliable pattern is **compute → narrate → verify**, and the verify
step is the build: extract every number from the generated prose and assert it appears in the
metrics bundle. It's about thirty lines of code, it's the difference between a feature Sam trusts
and one she checks by hand every month, and almost nobody writes it.

Second-order problem worth surfacing: the model will happily write *"prices rose sharply"* off a
0.4% move. Verified numbers, unverified adjectives. What do you do about that?

---

## A13 — "I'm not opening another dashboard"
**Everyone · 40 min**

> "Put it in Slack. If I have to open a tab, I won't use it."

**What you ship** A bot that answers in-channel, with the chart, and the SQL behind a fold.

**Acceptance criteria**
- Answer plus chart posted in the thread where it was asked
- The SQL is one click away and always available — never hidden
- Channel context is used where it's unambiguous and asked about where it isn't
- Cost and rate are bounded; a runaway thread can't run up a bill

**The interesting problem** A wrong answer in a public channel is much worse than a wrong answer in
an app, because it gets screenshotted, forwarded and quoted in a meeting three weeks later with no
provenance. So the surface needs to carry its own caveats: what it assumed, what it filtered, how
fresh the data is. Distribution changes the design requirements — that's the point of the ticket,
and it's why "just wrap it in a bot" is usually wrong.

---

## P7 — "Tell me when something changes"
**Ash · 50 min**

> "I watch six districts. I'm not going to log into your app every month. Tell me when my areas
> move, and don't tell me anything else."

**What you ship** A watchlist, rules evaluated whenever new data lands, and a digest that only
arrives when it should.

**Acceptance criteria**
- Ash can add districts and set a threshold in plain terms ("tell me about moves over 5%")
- An alert says *why* it fired, with the before and after numbers
- **No alert fires on a district with too few sales to be meaningful** — and the threshold for "too
  few" is a decision the team made and can defend
- Muting works, and a muted rule stays muted

**The interesting problem** Build the naive version first — median month over month, threshold
crossed, send — and then run it over the real eleven years. It fires constantly, because a district
with forty sales a month swings fifteen percent on noise alone. **The gap between "the feature
works" and "the feature is useful" is the entire lab**, and it's a gap most engineers have shipped
into production at least once.

The fixes are all legitimate and all have costs: minimum sample size (quiet districts get no
service), rolling windows (slower to react), year-on-year comparison (handles seasonality, misses
turning points). Make them choose and justify.

**Debrief question** "How many alerts did your rule fire over eleven years? Would Ash still be a
client?"

---

## P8 — "New data landed. Do the thing." ⭐
**The business · 55 min**

> "Land Registry publishes monthly. Right now somebody has to remember to press Load, and then
> remember to re-run Ash's alerts, and then remember to send Sam's commentary. Last month nobody
> remembered."

**What you ship** The whole chain, unattended: file appears → ingested → validated → published →
watchlists recomputed → digests and commentary delivered → status page updated.

**Acceptance criteria**
- A good file: everything downstream happens within N minutes, no human involved
- **A bad file — truncated, wrong schema, thirty percent short on rows — publishes nothing, sends
  nothing, and pages someone**
- The run is visible after the fact: what arrived, what passed, what changed, what went out
- Re-running the same file twice does not double anything

**The interesting problem** This is the ticket where every earlier one becomes a system, and the
interesting engineering is entirely about the failure path. It runs at 3am. The upstream publisher
will, eventually, ship a truncated file, rename a column, or publish twice. The business action —
emailing four hundred clients a number — is not reversible.

So the gate is the feature. That's what makes the audit-then-publish pattern land here in a way it
never does when it's introduced as "let's learn about branches": nobody argues about the cost of a
staging step once you frame the alternative as *sending four hundred clients a wrong number at 3am*.

**Debrief question** "Walk me through what happens if the file arrives with only the first three
days of the month in it." The good answer isn't "we'd catch it" — it's a specific assertion someone
wrote.

<sub>Under the hood: schedule or webhook → job → ingest to a branch → assertions → fast-forward or
quarantine → downstream fan-out.</sub>

---

## P9 — "Here's our portfolio. Value the book."
**Dani · 55 min**

> "Forty addresses in a spreadsheet. I need last sale, current estimate and which ones have moved,
> and I need it back as a spreadsheet because that's what goes to the committee."

**What you ship** CSV in, matched and enriched portfolio out, plus a review queue and an export.

**Acceptance criteria**
- Every uploaded row gets a match **confidence**, not just a match
- Rows that can't be matched confidently go to a **review queue** — never silently dropped, never
  silently guessed
- The export preserves Dani's original columns and adds yours; she has to hand it to a committee
- Re-uploading a corrected file doesn't create duplicates

**The interesting problem** Address matching is the whole ticket, and it's gloriously unglamorous.
`"Flat 3, 14 Acacia Rd"` has to find `SAON="FLAT 3", PAON="14", STREET="ACACIA ROAD"`. Postcode is
the reliable anchor; everything else is fuzzy. Around fifteen percent won't match cleanly and
**that fifteen percent is the design problem**: the instinct is to force a match, and the right
answer is a review queue with the top three candidates and a human click.

The transferable lesson — don't automate the ambiguous cases, surface them — applies to roughly
every data product anyone in the room will build.

---

## P10 — "Everyone else shows price per square metre" ⭐
**Priya · 60 min · brings in a second dataset**

> "£450,000 tells a client nothing. £6,200 per square metre tells them everything. Every portal
> shows it and we can't, and I get asked about it in every meeting."

**The catch** Land Registry Price Paid data has no floor area. It isn't in the file, it isn't
derivable, and no amount of modelling will conjure it. The only way to ship this feature is to bring
in another dataset — which is exactly the point of the ticket.

**The second dataset** Energy Performance Certificates for England and Wales carry a total floor
area and an address for around thirty million properties. The old
`epc.opendatacommunities.org` service was retired on 30 May 2026; the current home is
**[get-energy-performance-data.communities.gov.uk](https://get-energy-performance-data.communities.gov.uk/)**,
which offers bulk CSV per local authority and an API.

> **Presenter prep:** pre-stage a trimmed extract — a few local authorities, address fields, postcode
> and floor area only — and publish it as a Hugging Face dataset repo. Attendees pull it in one
> command instead of twenty-five people registering for a government download service in the same
> ten minutes. It also demonstrates dataset repos as a distribution mechanism without you having to
> make a slide about it.

**Acceptance criteria**
- £/m² appears wherever a price does — and **so does the coverage percentage** for whatever is being
  shown
- Where a property has several certificates over the years, the one chosen is the one nearest the
  sale date, and the rule is stated
- Implausible floor areas are excluded by a documented rule, not by eyeballing
- Below a coverage floor, the number is not shown at all

**The interesting problem** The join is imperfect and always will be — the same address matching as
P9, plus multiple certificates per property, plus self-reported areas with real outliers (a
twelve square metre house, a four thousand square metre flat). Coverage will land somewhere around
sixty to eighty percent and it will be **biased**: newer and rented properties are far likelier to
have a certificate.

So the honest feature displays its own coverage. *"£6,200/m², based on 68% of sales in this
district"* is a professional answer. A bare £/m² computed from a biased two-thirds, presented as
fact, is the kind of thing that ends up in a client report and then in a complaint.

**Debrief question** "Whose sales are missing from your 68%, and does that push the number up or
down?" Most teams won't have asked. That's the lesson.

---

## P11 — "Some of these sales are obviously wrong"
**Marcus · 40 min**

> "There's a £100 terrace in my comps and a £4 million semi in Burnley. They're wrecking my ranges."

**What you ship** Flagged transactions, excluded from valuations by default, visible on a toggle.

**Acceptance criteria**
- Flags cover at minimum: nominal-value transfers, prices far off the district median for that type,
  duplicates, and impossible dates
- Each flag has a stated rule and a reason string a human can read
- **Nothing is deleted.** Flagged rows stay queryable
- A10's comps exclude flagged rows by default; a market-share report includes them

**The interesting problem** These aren't errors. A £100 sale is a real transfer between related
parties. A £4m Burnley semi is probably a portfolio sale of thirty houses recorded against one
address — which is exactly what the `ppd_category_type` column's category B was created to mark.
The data is correct; it's the *interpretation* that's context-dependent.

So the feature is classification, not cleaning, and the exclusion rule belongs to the consumer:
valuations exclude bulk transfers, market-share analysis must include them or it undercounts.
"Bad data" is a property of the question, not of the row — a distinction that saves people from
destructive cleaning pipelines for the rest of their careers.

---

## P12 — "Who's likely to sell this year?"
**Prospecting team · 45 min**

> "Two hundred addresses in my patch, ranked, so I know where to post letters."

**What you ship** A scored, filterable, exportable lead list.

**Acceptance criteria**
- A transparent score with visible components, not a black box
- The list is filterable by area and property type and exports cleanly
- The feature is honest about what it is: **a ranking, not a prediction**
- There is a written plan for how you'd ever know whether it worked

**The interesting problem** There's no ground truth. You cannot train on "will sell" because the
label is in the future, so this is a heuristic — years since last sale, district turnover trend,
property type demand — and the professional move is to say so in the UI rather than dress it as a
model.

Then the real question: **how would you evaluate it?** The answer is a backtest — score as of 2022,
check who actually transacted in 2023–24, compare against a random baseline and against
"sorted by years since last sale" alone. If the fancy score doesn't beat the one-line heuristic,
ship the one-liner. Getting a room to design an evaluation for something with delayed feedback is
worth the whole session.

**Worth raising:** direct marketing to addresses has rules. A feature that generates a mailing list
has a compliance surface, and noticing that unprompted is a mark of seniority.

---

## P13 — "Can I trust this number?"
**The CFO, once, at the worst possible moment · 30 min**

> "You've told the board the average price rose 4%. Is that right? How would I know?"

**What you ship** A status page: data through when, loaded when, row counts by year, join coverage,
known gaps, last validation result, and what changed in the most recent load.

**Acceptance criteria**
- Answers "how fresh?", "how complete?" and "what changed?" without anyone reading a log
- Every number elsewhere in the product can be traced to a specific load
- Known gaps are stated, not implied by absence

**The interesting problem** It's the least glamorous ticket here and it determines whether anyone
uses any of the others. The first time a number is questioned and the team can't answer within
thirty seconds, adoption stops.

Good ticket for someone who finishes early, and a genuinely good one to demo last — it reframes
everything built that day as a product someone has to trust rather than a demo someone has to like.

---

## P14 — "Our chart says prices fell. Everyone knows they rose."
**Priya · 60 min · the hardest one here**

> "Average price is down 3% this month. But we sold a lot more flats this month. The chart is
> technically correct and completely wrong, and I have to explain it in every meeting."

**What you ship** A mix-adjusted index alongside the naive average, and an explanation of the gap.

**Acceptance criteria**
- Both series shown together, with the divergence explained in the UI
- The adjusted series is built from **repeat sales** — the same property transacting twice — so
  composition can't move it
- Pair counts are shown; a district with nine pairs doesn't get a confident line
- Someone can explain the method in two sentences to a client

**The interesting problem** Mix effect is the most common way property statistics lie, and it's the
same failure as Simpson's paradox in a suit. Repeat-sales pairing needs the address matching from
P9 — the same property, two dates — and then real decisions: what about properties extended
between sales, or sold twice in three months, or where one of the two sales was a flagged transfer
from P11?

This is where the day's earlier tickets compound, and where a room of developers discovers that the
hard part of analytics was never the SQL.

---

## Suggested pairings

| Session shape | Tickets |
|---|---|
| One afternoon, mixed room | **A11** then **A10** — visible payoff, then depth |
| Data-platform heavy room | **P8** then **P11** |
| "Show me an AI feature that isn't a chatbot" | **A10** and **A12** — both are compute-then-narrate |
| Bring-your-own-data theme | **P10**, with **P9**'s matching as the prerequisite |
| Analytics / stats-minded room | **P14**, with **P7** as the warm-up |
| Two full days | 101 → 108 → 102 → 106 → 104 → 112, in that order |

Every ticket except A11 and P13 is easier if the eval harness from the core workshop's F1
already exists — not because the tickets are about AI, but because "did that change make it better?"
is a question they will all raise, and answering it by opinion wastes the afternoon.
