#!/bin/bash
set -euo pipefail

hadoop fs -get /output computed_output

# sample top 100 metrics of baseline.txt and see if `computed_output` contains
# these strings
baseline="${BASELINE_FILE:-./baseline.txt}"
top_words=$(head -n 100 "$baseline" | cut -d',' -f1)

missing=0
checked=0

for word in $top_words; do
    checked=$((checked+1))
    if ! grep -qrE "^${word}[[:space:]]" computed_output/part-*; then
        echo "MISSING: ${word}"
        missing=$((missing+1))
    fi
done

echo "${checked} words checked, ${missing} missing"

if [ "$missing" -ne 0 ]; then
    exit 1
fi
