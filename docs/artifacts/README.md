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
| F1 | `lab-f1-trustworthy-analyst.html` | lab-f1-trustworthy-analyst.html |
| F2 | `lab-f2-model-bakeoff.html` | lab-f2-model-bakeoff.html |
| H3 | `lab-h3-publish-to-hub.html` | lab-h3-publish-to-hub.html |
| A10 | `lab-a10-defensible-comps.html` | lab-a10-defensible-comps.html |
| A11 | `lab-a11-chart-the-result.html` | lab-a11-chart-the-result.html |

## Reference pages

`attendee-start-here.html` is the only one written for attendees; the rest are for whoever
is running the day.

| Page | File | Published at |
|---|---|---|
| **Attendee Start Here** | `attendee-start-here.html` | attendee-start-here.html |
| Workshop Handbook | `workshop-handbook.html` | `./workshop-handbook.html` |
| Workshop Program | `lakehouse-workshop-program.html` | lakehouse-workshop-program.html |
| Exercise Picker | `lakehouse-exercise-picker.html` | lakehouse-exercise-picker.html |
| Lab Catalogue | `lakehouse-lab-catalogue.html` | lakehouse-lab-catalogue.html |
| Meridian Backlog | `meridian-property-backlog.html` | meridian-property-backlog.html |
| Iceberg Setup Runbook | `iceberg-setup-runbook.html` | iceberg-setup-runbook.html |

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
