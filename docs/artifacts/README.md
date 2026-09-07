# Workshop pages (artifact sources)

The HTML source for every published workshop page. These are the **editable originals** —
the published pages at the URLs below are rendered from them.

Until now these lived only in a session scratchpad under `/private/tmp`, which is wiped
between sessions. They are here so the pages an attendee reads are version-controlled
alongside the Markdown that links to them.

## Step-by-step lab guides

Self-guided walkthroughs: which file to open, what to type, what you should see, and what
to do when it doesn't work. Written to be usable with no instructor in the room.

| Lab | File | Published at |
|---|---|---|
| F1 | `lab-f1-trustworthy-analyst.html` | https://claude.ai/code/artifact/250819f3-61a5-4ddb-a20d-8573e8be30ab |
| F2 | `lab-f2-model-bakeoff.html` | https://claude.ai/code/artifact/04543121-4442-407f-9c7f-1c228bc131d1 |
| H3 | `lab-h3-publish-to-hub.html` | https://claude.ai/code/artifact/1bb55baa-e526-4420-acd1-a75c91a5541d |
| A10 | `lab-a10-defensible-comps.html` | https://claude.ai/code/artifact/fc399127-31ae-415b-a336-f7bf5c4232bd |
| A11 | `lab-a11-chart-the-result.html` | https://claude.ai/code/artifact/5d0a622c-5893-44db-8b9b-8552c7e16b11 |

## Reference pages

| Page | File | Published at |
|---|---|---|
| Workshop Handbook | `workshop-handbook.html` | https://claude.ai/code/artifact/0a44117f-66ed-449d-b02f-d7054665a284 |
| Workshop Program | `lakehouse-workshop-program.html` | https://claude.ai/code/artifact/89842f15-33a1-4a49-bd4b-7eae2c0e4df0 |
| Exercise Picker | `lakehouse-exercise-picker.html` | https://claude.ai/code/artifact/140df8c3-feba-4540-870a-bc4bea2d98e9 |
| Lab Catalogue | `lakehouse-lab-catalogue.html` | https://claude.ai/code/artifact/e095dd80-a75e-4329-b97c-d3c38250a3ba |
| Meridian Backlog | `meridian-property-backlog.html` | https://claude.ai/code/artifact/3a8d31cd-3d4c-49ca-887b-8a199b5fccca |
| Iceberg Setup Runbook | `iceberg-setup-runbook.html` | https://claude.ai/code/artifact/c4160111-f64c-4a4e-af7a-a1c1dda8b390 |

## Editing one

Edit the file here, then republish it to **its existing URL** — publishing without the URL
creates a second, separate page instead of updating the one everything links to.

The links between these pages are hard-coded URLs, so the URLs must stay stable. Changing
one means updating every page and Markdown file that points at it.

## Where the links live

Each lab guide is reachable from three places, and all three need to agree:

- the lab's own section in `WORKSHOP.md` (F1, F2, H3) or `WORKSHOP-FEATURE-LABS.md` (A10, A11)
- the lab's slot in the Program — where attendees are during the day
- the guides table in the Handbook — where instructors look

These pages also restate material that exists in the Markdown. When something changes,
check all copies: setup steps, lab instructions and the `hf` commands have each drifted
out of sync at least once.
