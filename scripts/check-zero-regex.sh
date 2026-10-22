#!/usr/bin/env bash
# Enforce the zero-regex rule (#532) in production Java sources: no java.util.regex
# and no regex-interpreting String methods in tracked **/src/main/**/*.java files.
#
# What is flagged, line by line, whatever the argument is (literal, constant, variable):
#   - any mention of java.util.regex (imports or qualified use)
#   - Pattern.compile( , Pattern.matches( , Pattern.quote(
#   - .split( , .replaceAll( , .replaceFirst( , .matches(
#
# How known non-regex look-alikes stay out without loosening the check:
#   - comment and javadoc lines are skipped (lines whose first non-blank characters
#     are //, *, or /*)
#   - .replaceAll(( and .replaceAll(x -> are skipped: a lambda argument is the
#     Map/List replaceAll, which never interprets a regex
#   - .matches( with an unquoted comma in its argument list is skipped: String.matches
#     takes one argument, so a two-argument matches is a custom method (PostalMasks);
#     a comma inside a quoted regex such as "a{1,2}" does not trigger the skip
#   - a line ending in "// zero-regex: allowed" is exempt; used for single-argument
#     custom matchers that are textually identical to String.matches (NIO PathMatcher,
#     FormatGrammar, ByteMatcher, metric row cursors), each marked at the call site
#
# Allowlist: two permanent files, then temporary entries tied to open PRs that are
# removing the remaining sites; drop each temporary block when its PR merges.
#
# Self-test: run with --self-test to exercise the detector against fixtures with
# known violations (literal and variable arguments) and known false positives.
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
  # Temporary, PR #568 (nested message projection) rewrites this file; remove when merged:
  'transform/projection/src/main/java/ai/protomolt/proto/projection/MessageProjection.java'
)

allowed() {
  local candidate="$1" entry
  for entry in "${allowlist[@]}"; do
    [[ "$candidate" == "$entry" ]] && return 0
  done
  return 1
}

# Prints the offending lines of one file; prints nothing when the file is clean.
scan_file() {
  local file="$1"
  grep -nE 'java\.util\.regex|Pattern\.(compile|matches|quote)\(|\.(split|replaceAll|replaceFirst|matches)\(' "$file" 2>/dev/null \
    | grep -vE '^[0-9]+:[[:space:]]*(//|\*|/\*)' \
    | grep -vE '\.replaceAll\((\(|[A-Za-z_][A-Za-z0-9_]*[[:space:]]*->)' \
    | grep -vE "\.matches\([^(),\"']*," \
    | grep -v 'zero-regex: allowed' || true
}

self_test() {
  local dir hits
  dir="$(mktemp -d)"
  # shellcheck disable=SC2064  # expand now: dir is function-local, gone at exit time
  trap "rm -rf '$dir'" EXIT

  cat > "$dir/Violation.java" <<'EOF'
class Violation {
    static final String DELIM = ",";
    String[] a = "a,b".split(",");
    String[] b = "a,b".split(DELIM);
    boolean c = "x".matches(DELIM);
    String d = "x".replaceAll(DELIM, "_");
    String e = "x".replaceFirst(DELIM, "_");
}
EOF
  hits="$(scan_file "$dir/Violation.java")"
  if [[ "$(printf '%s\n' "$hits" | grep -c .)" -ne 5 ]]; then
    echo 'self-test FAIL: expected all five violation fixture lines flagged, got:' >&2
    echo "$hits" >&2
    return 1
  fi

  cat > "$dir/FalsePositive.java" <<'EOF'
/**
 * Prose about {@code String.split("\n")} stays unflagged.
 */
class FalsePositive {
    // a comment about value.replaceAll("x", "y") stays unflagged
    void run(java.util.Map<String, String> map, java.util.List<String> list,
             java.nio.file.PathMatcher m, java.nio.file.Path p, Cursor filter, int doc,
             String code, String mask) {
        map.replaceAll((k, v) -> v);
        list.replaceAll(s -> s);
        boolean a = m.matches(p); // zero-regex: allowed
        boolean b = filter.matches(doc); // zero-regex: allowed
        boolean c = Masks.matches(code, mask);
        String marked = "x".split(","); // zero-regex: allowed
    }
    interface Cursor { boolean matches(int doc); }
    interface Masks { static boolean matches(String code, String mask) { return false; } }
}
EOF
  hits="$(scan_file "$dir/FalsePositive.java")"
  if [[ -n "$hits" ]]; then
    echo 'self-test FAIL: a false-positive fixture line was flagged:' >&2
    echo "$hits" >&2
    return 1
  fi

  cat > "$dir/QuotedComma.java" <<'EOF'
class QuotedComma {
    boolean counted = "aa".matches("a{1,2}");
}
EOF
  hits="$(scan_file "$dir/QuotedComma.java")"
  if [[ -z "$hits" ]]; then
    echo 'self-test FAIL: a quoted-comma regex slipped through the multi-argument skip' >&2
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
done < <(git ls-files --cached -z -- ':(glob)**/src/main/**/*.java')

if (( status == 0 )); then
  printf 'Zero-regex check OK\n'
fi
exit "$status"
