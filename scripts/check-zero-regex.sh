#!/usr/bin/env bash
# Enforce the zero-regex rule (#532) in production Java sources: no java.util.regex
# and no regex-interpreting String methods in tracked */src/main/java/**/*.java files.
#
# What is flagged, line by line:
#   - any mention of java.util.regex (imports or qualified use)
#   - Pattern.compile( , Pattern.matches( , Pattern.quote(
#   - .split(" , .replaceAll(" , .replaceFirst(" , .matches(" with a string-literal
#     first argument, the shape every regex call site in this repository used
#
# How known non-regex look-alikes stay out without loosening the check:
#   - comment and javadoc lines are skipped (lines whose first non-blank characters
#     are //, *, or /*), so prose like HostText's "the way String.split(\"\\n\") does"
#     never trips the scan
#   - the string-literal argument requirement skips Map.replaceAll(lambda), custom
#     matches(...) methods (NIO PathMatcher, LuceneMetricLeaf's cursor, PostalMasks,
#     StringFormat), and single-quoted JavaScript inside served-page text blocks;
#     names that merely contain "Pattern", like DecimalRangePattern.range(...), do
#     not match the compile/matches/quote tokens
#   - a line ending in "// zero-regex: allowed" is exempt, for a deliberate,
#     reviewed exception that is cheaper to mark than to allowlist
#
# Allowlist: two permanent files, then temporary entries tied to open PRs that are
# removing the remaining sites; drop each temporary block when its PR merges.
#
# Self-test: run with --self-test to exercise the detector against fixtures with a
# known violation and known false positives.
set -euo pipefail
cd "$(dirname "$0")/.."

# Permanent:
#   protobuf/validation ValueChecks evaluates user-supplied protovalidate
#   string.pattern rules, so the regex engine is the feature there.
#   search/console SearchConsolePage embeds client-side JavaScript in Java text
#   blocks; its split calls are JS, not Java regex.
allowlist=(
  'protobuf/validation/src/main/java/ai/protomolt/proto/validate/ValueChecks.java'
  'search/console/src/main/java/ai/protomolt/proto/search/console/SearchConsolePage.java'
  # Temporary, PR #567 (asset and parse zero-regex), remove when merged:
  'asset/bridge-parse/src/main/java/ai/protomolt/proto/asset/bridge/parse/RecoveredTextMeasures.java'
  'asset/bridge/src/main/java/ai/protomolt/proto/asset/bridge/TarReader.java'
  'asset/characterize/src/main/java/ai/protomolt/proto/asset/characterize/Characterizer.java'
  'asset/characterize/src/main/java/ai/protomolt/proto/asset/characterize/FormatGrammars.java'
  'asset/characterize/src/main/java/ai/protomolt/proto/asset/characterize/container/ContainerFormats.java'
  'parse/playground/src/main/java/ai/protomolt/proto/parse/playground/ParsePlaygroundServer.java'
  'parse/service/src/main/java/ai/protomolt/proto/parse/service/ParseCoordinatorMain.java'
  # Temporary, PR #568 (nested message projection), remove when merged:
  'transform/projection/src/main/java/ai/protomolt/proto/projection/MessageProjection.java'
  # Temporary, PR #577 (slice E zero-regex), remove when merged:
  'acquire/gather/maven/src/main/java/ai/protomolt/proto/acquire/gather/maven/MavenProtoGatherer.java'
  'apps/document-platform/src/main/java/ai/protomolt/proto/platform/DocumentPlatformConfig.java'
  'metric/iceberg/src/main/java/ai/protomolt/proto/metric/iceberg/DuckDbSql.java'
  'metric/iceberg/src/main/java/ai/protomolt/proto/metric/iceberg/IcebergRollupSink.java'
  'metric/iceberg/src/main/java/ai/protomolt/proto/metric/iceberg/IcebergRollupSubjects.java'
  'samples/src/main/java/ai/protomolt/proto/samples/AuthoringPolicySeed.java'
  'samples/src/main/java/ai/protomolt/proto/samples/CourtDocuments.java'
  'schema/confluent/src/main/java/ai/protomolt/proto/schema/confluent/ConfluentSchemaRegistryLoader.java'
  'schema/registry/core/src/main/java/ai/protomolt/proto/registry/DescriptorSetArtifacts.java'
  'schema/registry/core/src/main/java/ai/protomolt/proto/registry/SchemaRegistryStores.java'
  'schema/registry/core/src/main/java/ai/protomolt/proto/registry/StoredSchemaSources.java'
  'schema/registry/git/src/main/java/ai/protomolt/proto/schema/registry/git/RegistryFederation.java'
  'schema/registry/service/src/main/java/ai/protomolt/proto/registry/service/SchemaRegistryServer.java'
  'search/console/src/main/java/ai/protomolt/proto/search/console/SearchConsoleServer.java'
  'sink/emit/core/src/main/java/ai/protomolt/proto/emit/Bundle.java'
  'sink/emit/git/src/main/java/ai/protomolt/proto/emit/git/GitSink.java'
  'sink/emit/okf/src/main/java/ai/protomolt/proto/emit/okf/OkfRegistryBundles.java'
  'sink/emit/okf/src/main/java/ai/protomolt/proto/emit/okf/OkfRenderer.java'
  'sink/emit/parquet-s3/src/main/java/ai/protomolt/proto/emit/parquet/s3/S3ParquetSink.java'
  'sink/iceberg/src/main/java/ai/protomolt/proto/iceberg/LocalFileIO.java'
  'sink/kafka/connect-iceberg/src/main/java/ai/protomolt/proto/kafka/connect/iceberg/ConnectDescriptors.java'
  'sink/kafka/connect/src/main/java/ai/protomolt/proto/kafka/connect/GrpcConnectorSupport.java'
  'sink/kafka/connect/src/main/java/ai/protomolt/proto/kafka/connect/GrpcSourceTask.java'
  'sink/kafka/serde/src/main/java/ai/protomolt/proto/kafka/serde/SerdeDescriptors.java'
)

allowed() {
  local candidate="$1" entry
  for entry in "${allowlist[@]}"; do
    [[ "$candidate" == "$entry" ]] && return 0
  done
  return 1
}

# Prints the offending lines of one file; returns 0 when the file is clean.
scan_file() {
  local file="$1"
  grep -nE 'java\.util\.regex|Pattern\.(compile|matches|quote)\(|\.(split|replaceAll|replaceFirst|matches)\("' "$file" 2>/dev/null \
    | grep -vE '^[0-9]+:[[:space:]]*(//|\*|/\*)' \
    | grep -v 'zero-regex: allowed' || true
}

self_test() {
  local dir hits
  dir="$(mktemp -d)"
  # shellcheck disable=SC2064  # expand now: dir is function-local, gone at exit time
  trap "rm -rf '$dir'" EXIT

  cat > "$dir/Violation.java" <<'EOF'
class Violation {
    String[] parts = "a,b".split(",");
}
EOF
  hits="$(scan_file "$dir/Violation.java")"
  if [[ -z "$hits" ]]; then
    echo 'self-test FAIL: the violation fixture was not flagged' >&2
    return 1
  fi

  cat > "$dir/FalsePositive.java" <<'EOF'
/**
 * Prose about {@code String.split("\n")} stays unflagged.
 */
class FalsePositive {
    // a comment about value.replaceAll("x", "y") stays unflagged
    void run(java.util.Map<String, String> map, java.nio.file.PathMatcher m,
             java.nio.file.Path p, Cursor filter, int doc) {
        map.replaceAll((k, v) -> v);
        boolean a = m.matches(p);
        boolean b = filter.matches(doc);
        String marked = "x".split(","); // zero-regex: allowed
    }
    interface Cursor { boolean matches(int doc); }
}
EOF
  hits="$(scan_file "$dir/FalsePositive.java")"
  if [[ -n "$hits" ]]; then
    echo 'self-test FAIL: a false-positive fixture line was flagged:' >&2
    echo "$hits" >&2
    return 1
  fi
  echo 'zero-regex self-test OK'
}

if [[ "${1:-}" == "--self-test" ]]; then
  self_test
  exit $?
fi

status=0
while IFS= read -r -d '' file; do
  [[ -f "$file" ]] || continue
  if allowed "$file"; then
    continue
  fi
  hits="$(scan_file "$file")"
  if [[ -n "$hits" ]]; then
    while IFS= read -r line; do
      printf 'FAIL: %s:%s regex API in production code; use a character scan (see #532), or extend the allowlist in %s\n' \
        "$file" "$line" "scripts/check-zero-regex.sh" >&2
    done <<< "$hits"
    status=1
  fi
done < <(git ls-files --cached -z -- ':(glob)**/src/main/java/**/*.java')

if (( status == 0 )); then
  printf 'Zero-regex check OK\n'
fi
exit "$status"
