#!/usr/bin/env bash
#
# Pobiera CSV-ki ze wszystkich udanych runów workflowa scrape (GitHub Actions)
# do data/raw_data/, skąd bierze je ingest.py (FIFO).
#
# Wymaga: gh (GitHub CLI), zalogowany `gh auth login`.
# Użycie:  ./scripts/pull_artifacts.sh
#
set -euo pipefail

WORKFLOW="scrape.yml"
LIMIT=50

# Katalog repo (skrypt działa niezależnie od tego, skąd go odpalisz).
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEST="${REPO_ROOT}/data/raw_data"
PROCESSED="${DEST}/processed"

mkdir -p "$DEST"

run_ids="$(gh run list --workflow="$WORKFLOW" --status=success \
  -L "$LIMIT" --json databaseId -q '.[].databaseId')"

if [[ -z "$run_ids" ]]; then
  echo "Brak udanych runów workflowa ${WORKFLOW}."
  exit 0
fi

# gh rozpakowuje każdy artefakt do własnego podkatalogu (books-<run_id>/...).
# Ściągamy do katalogu tymczasowego, potem spłaszczamy CSV-ki do DEST.
STAGING="$(mktemp -d)"
trap 'rm -rf "$STAGING"' EXIT

for id in $run_ids; do
  echo "Pobieram artefakty z runu ${id}..."
  gh run download "$id" --dir "$STAGING" 2>/dev/null || true
done

# Przenieś same pliki .csv płasko do DEST. Pomiń te, które już są
# w processed/ (już zingerowane) — idempotencja, brak ponownego przetwarzania.
moved=0
skipped=0
while IFS= read -r -d '' file; do
  name="$(basename "$file")"
  if find "$PROCESSED" -type f -name "$name" -print -quit 2>/dev/null | grep -q .; then
    echo "  pomijam ${name} — juz w processed/"
    skipped=$((skipped + 1))
    continue
  fi
  mv -f "$file" "${DEST}/"
  moved=$((moved + 1))
done < <(find "$STAGING" -type f -name '*.csv' -print0)

echo "Gotowe. Przeniesiono: ${moved}, pominięto (juz w processed): ${skipped}"
