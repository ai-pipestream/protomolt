#!/usr/bin/env bash
# Enforce the zero-regex rule (#532) in production Java sources: no java.util.regex
# and no regex-interpreting String methods in **/src/main/**/*.java files, tracked or
# untracked (ignored build outputs stay out, as in check-test-naming.sh).
#
# What is flagged, line by line, whatever the argument is (literal, constant, variable,
# parenthesized expression), with legal whitespace tolerated around the dot and the
# opening parenthesis:
#   - any mention of java.util.regex (imports or qualified use)
#   - Pattern.compile( , Pattern.matches( , Pattern.quote(
#   - .split( , .splitWithDelimiters( , .replaceAll( , .replaceFirst( , .matches(
#
# Out of this line-by-line token scan's scope: method references ("a+"::matches,
# String::split), calls split across a newline after the dot, Scanner.useDelimiter,
# and getPathMatcher("regex:...").
#
# How known non-regex look-alikes stay out without loosening the check. The skips are
# token-level: the non-regex call is removed from the line and whatever remains is
# scanned again, so a line carrying both a collection lambda and a real regex call
# still fails.
#   - comment-only lines are skipped (//, javadoc continuations, and block comments);
#     code after a closing block comment on the same line is still scanned
#   - replaceAll with a lambda argument ((k, v) -> or x ->) is the Map/List form,
#     which never interprets a regex; a merely parenthesized first argument such as
#     replaceAll((expression), "_") is the String overload and is flagged
#   - matches with an unquoted comma in its argument list is a custom two-argument
#     method (PostalMasks); a comma inside a quoted regex such as "a{1,2}" still flags
#   - a line ending in "// zero-regex: allowed" is exempt; used for single-argument
#     custom matchers that are textually identical to String.matches (NIO PathMatcher,
#     FormatGrammar, ByteMatcher, metric row cursors), each marked at the call site
#
# Allowlist: two permanent files. Temporary entries tied to open PRs may be added
# below with the PR number; drop each when its PR merges.
#
# Self-test: run with --self-test to exercise the detector against fixtures with
# known violations and known false positives.
set -euo pipefail
cd "$(dirname "$0")/.."

# The grep and awk token pattern; kept identical in both so a line grep finds is
# re-tested the same way after the token-level removals.
PATTERN='java\.util\.regex|Pattern[[:space:]]*\.[[:space:]]*(compile|matches|quote)[[:space:]]*\(|\.[[:space:]]*(split(WithDelimiters)?|replaceAll|replaceFirst|matches)[[:space:]]*\('

# Permanent:
#   protobuf/validation ValueChecks evaluates user-supplied protovalidate
#   string.pattern rules, so the regex engine is the feature there.
#   search/console SearchConsolePage embeds client-side JavaScript in Java text
#   blocks; its split calls are JS, not Java regex.
allowlist=(
  'protobuf/validation/src/main/java/ai/protomolt/proto/validate/ValueChecks.java'
  'search/console/src/main/java/ai/protomolt/proto/search/console/SearchConsolePage.java'
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
  # The pattern rides in through the environment: awk -v would reprocess the
  # backslash escapes, ENVIRON hands the text over untouched.
  grep -nE "$PATTERN" "$file" 2>/dev/null | ZERO_REGEX_PATTERN="$PATTERN" awk '
    BEGIN { pattern = ENVIRON["ZERO_REGEX_PATTERN"] }
    /zero-regex: allowed/ { next }
    {
      text = $0
      sub(/^[0-9]+:[[:space:]]*/, "", text)
      if (text ~ /^\/\//) next
      if (text ~ /^\*/) next
      if (text ~ /^\/\*/) {
        # A leading block comment: drop it (comments do not nest) and keep
        # scanning any code after the closing */; a comment spanning the whole
        # line, or continuing past it, carries no code to scan.
        if (index(text, "*/") == 0) next
        sub(/^\/\*.*\*\//, "", text)
      }
      # Token-level skips: remove the non-regex calls, then re-test the rest.
      gsub(/\.[[:space:]]*replaceAll[[:space:]]*\([[:space:]]*\([^)]*\)[[:space:]]*->/, "", text)
      gsub(/\.[[:space:]]*replaceAll[[:space:]]*\([[:space:]]*[A-Za-z_][A-Za-z0-9_]*[[:space:]]*->/, "", text)
      gsub(/\.[[:space:]]*matches[[:space:]]*\([^(),"'"'"']*,/, "", text)
      if (text ~ pattern) print $0
    }' || true
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
    String f = "x".replaceAll(("a"), "_");
    String g = "x".replaceAll(("a" + "b"), "_");
    String[] h = "a,b". split(",");
    String[] i = "a,b".split (",");
    /* block */ String[] j = "a,b".split(",");
    boolean k = "aa".matches("a{1,2}");
    String[] l = "a,b".splitWithDelimiters(",", 0);
}
EOF
  hits="$(scan_file "$dir/Violation.java")"
  if [[ "$(printf '%s\n' "$hits" | grep -c .)" -ne 12 ]]; then
    echo 'self-test FAIL: expected all twelve violation fixture lines flagged, got:' >&2
    echo "$hits" >&2
    return 1
  fi

  cat > "$dir/FalsePositive.java" <<'EOF'
/**
 * Prose about {@code String.split("\n")} stays unflagged.
 */
class FalsePositive {
    // a comment about value.replaceAll("x", "y") stays unflagged
    /* a block comment about value.split(",") stays unflagged */
    void run(java.util.Map<String, String> map, java.util.List<String> list,
             java.nio.file.PathMatcher m, java.nio.file.Path p, Cursor filter, int doc,
             String code, String mask) {
        map.replaceAll((k, v) -> v);
        map.replaceAll( (k, v) -> v);
        list.replaceAll(s -> s);
        list.replaceAll( s -> s);
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

  cat > "$dir/MixedLine.java" <<'EOF'
class MixedLine {
    void run(java.util.Map<String, String> map, String s) {
        map.replaceAll((k, v) -> v); String[] x = s.split(",");
    }
}
EOF
  hits="$(scan_file "$dir/MixedLine.java")"
  if [[ -z "$hits" ]]; then
    echo 'self-test FAIL: a collection lambda on the same line hid a real split' >&2
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
done < <(git ls-files --cached --others --exclude-standard -z -- ':(glob)**/src/main/**/*.java')

if (( status == 0 )); then
  printf 'Zero-regex check OK\n'
fi
exit "$status"
