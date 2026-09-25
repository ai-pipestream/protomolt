#!/bin/sh
# Export one signed task snapshot and every referenced local artifact; Docker only.
set -eu
if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
  echo 'usage: ./export-record.sh <task-uuid> [output.zip]' >&2
  exit 2
fi
cd "$(dirname "$0")"
output=${2:-task-$1.zip}
if [ -e "$output" ]; then
  echo 'output already exists; choose a new filename' >&2
  exit 2
fi
temporary=$(mktemp "${output}.tmp.XXXXXX")
trap 'rm -f "$temporary"' EXIT HUP INT TERM
docker compose exec -T serve java -cp '/opt/protomolt-serve/lib/*' \
  ai.protomolt.proto.serve.StarterRecordExport "$1" > "$temporary"
mv "$temporary" "$output"
echo "Exported $output"
