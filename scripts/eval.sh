#!/usr/bin/env bash
#
# Score the text-to-SQL analyst against a set of questions with known answers.
#
# Needs only curl and the hf CLI - no Java, no Maven, no Python. It talks to a deployed
# Space over HTTP, which is the same thing a person using the app talks to.
#
#   ./scripts/eval.sh <you>/big-data-ai            # uses scripts/questions.csv
#   ./scripts/eval.sh <you>/big-data-ai my.csv
#
# Four cumulative gates per question, in this order:
#
#   generated?  the model returned some SQL at all
#   ran?        the guard accepted it and Spark executed it
#   correct?    the expected value appears in the result
#
# A run that scores 10/10/3 has a completely different problem from one scoring 10/4/4.
set -uo pipefail

SPACE="${1:?usage: eval.sh <owner>/<space> [questions.csv]}"
CSV="${2:-scripts/questions.csv}"
HOST="https://$(echo "$SPACE" | tr '/' '-').hf.space"
TOKEN="$(hf auth token 2>/dev/null | tr -d '[:space:]')"
AUTH=(-H "Authorization: Bearer ${TOKEN}")

[[ -f "$CSV" ]] || { echo "no such file: $CSV" >&2; exit 1; }

# Which model is answering. Without this a bake-off is four tables that do not say what
# produced them, and the whole point is comparing them.
MODEL=$(curl -s "${HOST}/admin" "${AUTH[@]}" --max-time 60 \
        | grep -oE '<option[^>]*selected[^>]*>[^<]+' | sed 's/.*>//' | head -1)
echo "model:     ${MODEL:-unknown}"
echo "questions: ${CSV}"
echo

gen=0; ran=0; ok=0; total=0
printf "%-5s %-9s %-7s %-9s %s\n" "id" "generated" "ran" "correct" "ms"
printf -- "----------------------------------------------------------\n"

# skip the header, then read id,question,expected - question may contain no commas
tail -n +2 "$CSV" | while IFS=, read -r id question expected; do
    [[ -z "${id:-}" ]] && continue
    total=$((total + 1))

    started=$(date +%s000)
    # Capture the status separately. A model that ignores the JSON contract makes the endpoint
    # throw, and Spring answers 500 with a JSON error body - which is non-empty text that looks
    # like an answer. Scoring that as "generated" would point you at the wrong gate: the model
    # produced nothing usable, which is a generation failure, not an execution one.
    response=$(curl -s -w '\n%{http_code}' -G "${HOST}/ai/generateQuery" \
                   --data-urlencode "query=${question}" "${AUTH[@]}" --max-time 180)
    status="${response##*$'\n'}"
    sql="${response%$'\n'*}"
    elapsed=$(( $(date +%s000) - started ))

    g="no"; r="no"; c="no"
    if [[ "${status}" == "200" && -n "${sql}" && "${sql}" != *"<html"* ]]; then
        g="yes"; gen=$((gen + 1))
        out=$(curl -s -X POST "${HOST}/runquery" \
                  --data-urlencode "generatedsql=${sql}" --data-urlencode "maxrows=50" \
                  "${AUTH[@]}" --max-time 180 | sed 's/<[^>]*>//g')
        if [[ "${out}" != *"Rejected:"* && "${out}" != *"Query failed"* ]]; then
            r="yes"; ran=$((ran + 1))
            if [[ -z "${expected// /}" ]]; then
                # No expected value yet. Say so rather than scoring it: an empty string is a
                # substring of everything, so treating a blank as a pass would mark an
                # unfinished question green - the exact failure this harness exists to prevent.
                c="--"
            elif [[ "${out}" == *"${expected}"* ]]; then
                c="yes"; ok=$((ok + 1))
            fi
        fi
    fi
    printf "%-5s %-9s %-7s %-9s %s\n" "$id" "$g" "$r" "$c" "$elapsed"
done

echo
echo "correct = '--' means that question has no expected value yet. Fill it in:"
echo "run the query yourself, read the answer, put it in the csv."
echo
echo "Run it twice on the same model before drawing any conclusion."
