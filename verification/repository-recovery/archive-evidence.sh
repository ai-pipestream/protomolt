#!/usr/bin/env bash
# Copy the machine-checked evidence of a rehearsal output directory into a dated evidence
# folder: markers, XML results, command logs with exit codes, manifests, identity records
# and host logs. Backup payloads (dumps, volume archives, protobuf receipts) stay out.
set -euo pipefail
[[ $# -eq 2 ]] || { echo "usage: $0 <rehearsal-out-dir> <evidence-dir>" >&2; exit 2; }
src="$1"; dst="$2"
[[ -f "$src/summary.json" ]] || { echo "no summary.json under $src" >&2; exit 2; }
mkdir -p "$dst"
cp "$src/summary.json" "$src/runtime-inventory.tsv" "$dst/"
for run in "$src"/run-* "$src"/negative-*; do
  [[ -d "$run" ]] || continue
  name="$(basename "$run")"
  mkdir -p "$dst/$name"
  for f in markers.log results.xml commands.log seed-host.log recovered-host.log; do
    [[ -f "$run/$f" ]] && cp "$run/$f" "$dst/$name/"
  done
  if [[ -d "$run/recovered" ]]; then
    mkdir -p "$dst/$name/recovered"
    for f in markers.log results.xml READY; do [[ -f "$run/recovered/$f" ]] && cp "$run/recovered/$f" "$dst/$name/recovered/"; done
  fi
  for b in backup backup-copy; do
    if [[ -d "$run/$b" ]]; then
      mkdir -p "$dst/$name/$b/identities"
      for f in manifest.json SEALED schema/catalog.tsv; do
        [[ -f "$run/$b/$f" ]] && { mkdir -p "$dst/$name/$b/$(dirname "$f")"; cp "$run/$b/$f" "$dst/$name/$b/$f"; }
      done
      for f in identities.json markers.log seed-results.xml; do
        [[ -f "$run/$b/identities/$f" ]] && cp "$run/$b/identities/$f" "$dst/$name/$b/identities/"
      done
    fi
  done
done
echo "archived $(find "$dst" -type f | wc -l) files under $dst"
