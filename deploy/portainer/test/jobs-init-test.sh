#!/usr/bin/env bash
#
# jobs-init-test.sh: proves the portainer stack's jobs-init one-shot creates the
# workflow-runs database only when it is missing, and that the coordinator waits
# for it.
#
# Why this exists: the first attempt created the database from
# docker-entrypoint-initdb.d, which PostgreSQL runs only against an empty data
# directory. On the NAS the repo-postgres volume already existed, so nothing
# was created and the coordinator was pointed at a database that was not there.
# jobs-init runs psql on each deploy instead, so the same stack file has to be
# correct on a fresh volume and on the one that is already in service.
#
# How it runs without containers: the jobs-init shell script is read out of
# deploy/portainer/compose.yml itself (the text Compose would hand to sh, with
# $$ unescaped), and a fake psql is placed first on PATH. The fake answers the
# existence probe from FAKE_PG_DATABASES and records every invocation, so the
# test can assert what SQL was sent and where. The compose wiring around the
# service (depends_on, image parity, coordinator environment) is checked from
# the same parsed file.
#
# Usage: deploy/portainer/test/jobs-init-test.sh     (exit 0 = all cases pass)

set -uo pipefail

TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE="$TEST_DIR/../compose.yml"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/jobs-init-test.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT

failures=0
pass() { printf 'ok   %s\n' "$*"; }
fail() { printf 'FAIL %s\n' "$*" >&2; failures=$((failures + 1)); }

# ── read the service out of the compose file ──────────────────────────────────
# A small line scanner rather than a YAML library, so the test has no
# dependency the CI runner might lack. It understands exactly the shapes this
# compose file uses: two-space service keys, "key: value" scalars, and a block
# scalar under "command:" introduced by "- |".
python3 - "$COMPOSE" "$WORK" <<'PY'
import re, sys

compose_path, work = sys.argv[1], sys.argv[2]
lines = open(compose_path).read().splitlines()


def service_block(name):
    start = None
    for i, line in enumerate(lines):
        if line == f"  {name}:":
            start = i
            break
    if start is None:
        return None
    body = []
    for line in lines[start + 1:]:
        if line and not line.startswith("   ") and not line.startswith("  #"):
            break
        body.append(line)
    return body


def scalar(block, key, indent):
    prefix = " " * indent + key + ":"
    for line in block:
        if line.startswith(prefix):
            return line[len(prefix):].strip()
    return None


def depends_condition(block, dependency):
    for i, line in enumerate(block):
        if line == f"      {dependency}:":
            return scalar(block[i + 1:i + 2], "condition", 8)
    return None


def command_script(block):
    for i, line in enumerate(block):
        if line.strip() == "- |":
            script = []
            for follow in block[i + 1:]:
                if follow and not follow.startswith("        "):
                    break
                script.append(follow[8:] if follow else "")
            # Compose interpolation: $$ becomes a literal $.
            return "\n".join(script).replace("$$", "$") + "\n"
    return None


def environment(block):
    env = {}
    for i, line in enumerate(block):
        if line == "    environment:":
            for follow in block[i + 1:]:
                if not follow.startswith("      "):
                    break
                m = re.match(r"^      ([A-Z0-9_]+): (.*)$", follow)
                if m:
                    env[m.group(1)] = m.group(2)
    return env


def write(name, value):
    with open(f"{work}/{name}", "w") as handle:
        handle.write("" if value is None else str(value))


init = service_block("jobs-init")
serve = service_block("serve")
postgres = service_block("repo-postgres")
write("init-present", "yes" if init is not None else "no")
if init is not None:
    write("init-script.sh", command_script(init))
    write("init-image", scalar(init, "image", 4))
    write("init-depends-postgres", depends_condition(init, "repo-postgres"))
    write("init-env", "\n".join(f"{k}={v}" for k, v in environment(init).items()))
write("serve-depends-init", depends_condition(serve, "jobs-init"))
write("serve-env", "\n".join(f"{k}={v}" for k, v in environment(serve).items()))
write("postgres-image", scalar(postgres, "image", 4))
PY

if [ "$(cat "$WORK/init-present")" != "yes" ]; then
  fail "compose.yml defines no jobs-init service"
  exit 1
fi
SCRIPT="$WORK/init-script.sh"
if [ ! -s "$SCRIPT" ]; then
  fail "jobs-init has no '- |' command script to run"
  exit 1
fi

# ── the fake psql ─────────────────────────────────────────────────────────────
# Records argv and stdin of every call to $FAKE_PSQL_LOG, one call per line as
# "argv<TAB>stdin". Answers a pg_database existence probe with "1" when the
# datname it asks about is listed in FAKE_PG_DATABASES. FAKE_PSQL_EXIT, when
# set, makes every call fail with that status and print nothing, which is what
# an unreachable server looks like to the script.
mkdir -p "$WORK/bin"
cat >"$WORK/bin/psql" <<'FAKE'
#!/usr/bin/env bash
set -uo pipefail
stdin=""
if [ ! -t 0 ]; then stdin="$(cat)"; fi
printf '%s\t%s\n' "$*" "$stdin" >>"$FAKE_PSQL_LOG"
if [ -n "${FAKE_PSQL_EXIT:-}" ]; then exit "$FAKE_PSQL_EXIT"; fi
sql=""
prev=""
for a in "$@"; do
  if [ "$prev" = "-c" ] || [ "$prev" = "-tAc" ]; then sql="$a"; fi
  prev="$a"
done
[ -n "$sql" ] || sql="$stdin"
if [[ "$sql" == *pg_database* ]]; then
  name="$(printf '%s' "$sql" | sed -n "s/.*datname *= *'\([^']*\)'.*/\1/p")"
  for present in ${FAKE_PG_DATABASES:-}; do
    if [ "$present" = "$name" ]; then echo 1; exit 0; fi
  done
  exit 0
fi
exit 0
FAKE
chmod +x "$WORK/bin/psql"

# Runs the extracted script the way the container would: sh -ec, with the
# environment the compose service declares (resolved to the stack defaults).
run_init() {
  local log="$1"; shift
  : >"$log"
  FAKE_PSQL_LOG="$log" PATH="$WORK/bin:$PATH" \
    PGUSER="documents" PGPASSWORD="s3cret-from-portainer" JOBS_DB="${JOBS_DB_OVERRIDE:-jobs}" \
    "$@" sh -ec "$(cat "$SCRIPT")" </dev/null >"$log.out" 2>"$log.err"
}

creates() { grep -c 'CREATE DATABASE' "$1" || true; }

# ── case: the database is missing (fresh volume) ──────────────────────────────
LOG="$WORK/missing.log"
if run_init "$LOG" env FAKE_PG_DATABASES="documents"; then
  [ "$(creates "$LOG")" = "1" ] \
    && pass "missing database: exactly one CREATE DATABASE issued" \
    || fail "missing database: expected one CREATE DATABASE, log: $(cat "$LOG")"
  grep -q 'CREATE DATABASE "jobs" OWNER "documents"' "$LOG" \
    && pass "missing database: created as \"jobs\" owned by the repo role" \
    || fail "missing database: CREATE names the wrong database or owner: $(grep CREATE "$LOG")"
  grep 'CREATE DATABASE' "$LOG" | grep -q -- '-d postgres' \
    && pass "missing database: CREATE runs on the maintenance database, not on jobs" \
    || fail "missing database: CREATE did not connect to the postgres maintenance database"
  grep -q -- '-h repo-postgres' "$LOG" \
    && pass "missing database: psql dials repo-postgres" \
    || fail "missing database: psql did not dial repo-postgres: $(cat "$LOG")"
else
  fail "missing database: script exited nonzero: $(cat "$LOG.err")"
fi

# ── case: the database already exists (the NAS volume) ────────────────────────
LOG="$WORK/present.log"
if run_init "$LOG" env FAKE_PG_DATABASES="documents jobs"; then
  [ "$(creates "$LOG")" = "0" ] \
    && pass "present database: no CREATE DATABASE issued" \
    || fail "present database: CREATE DATABASE was issued again: $(grep CREATE "$LOG")"
  grep -q 'pg_database' "$LOG" \
    && pass "present database: existence was probed rather than assumed" \
    || fail "present database: no pg_database probe found"
else
  fail "present database: script exited nonzero, redeploys on the existing volume would fail: $(cat "$LOG.err")"
fi

# ── case: another database is present but not jobs ───────────────────────────
LOG="$WORK/other.log"
run_init "$LOG" env FAKE_PG_DATABASES="documents jobs_archive" || true
[ "$(creates "$LOG")" = "1" ] \
  && pass "similar name present: jobs is still created (exact datname match)" \
  || fail "similar name present: probe matched loosely and skipped the CREATE"

# ── case: the server does not answer ─────────────────────────────────────────
LOG="$WORK/down.log"
if run_init "$LOG" env FAKE_PSQL_EXIT=2 FAKE_PG_DATABASES=""; then
  fail "server down: script reported success with no database"
else
  [ "$(creates "$LOG")" = "0" ] \
    && pass "server down: script fails and does not attempt a blind CREATE" \
    || fail "server down: script attempted CREATE after a failed probe"
fi

# ── case: an unsafe database name from the stack variable ────────────────────
LOG="$WORK/unsafe.log"
if JOBS_DB_OVERRIDE='jobs"; DROP DATABASE documents; --' run_init "$LOG" env FAKE_PG_DATABASES=""; then
  fail "unsafe name: script accepted a name that is not a plain identifier"
else
  [ "$(wc -l <"$LOG")" = "0" ] \
    && pass "unsafe name: refused before any psql call" \
    || fail "unsafe name: psql was still called: $(cat "$LOG")"
fi

# ── the password never rides argv ────────────────────────────────────────────
for log in "$WORK"/missing.log "$WORK"/present.log; do
  if grep -q 's3cret-from-portainer' "$log"; then
    fail "password appeared on a psql command line ($log); it must travel as PGPASSWORD"
  fi
done
pass "password stays in the environment, off the command line"

# ── compose wiring around the service ────────────────────────────────────────
[ "$(cat "$WORK/init-depends-postgres")" = "service_healthy" ] \
  && pass "jobs-init waits for repo-postgres to be healthy" \
  || fail "jobs-init must depend on repo-postgres: condition: service_healthy (got '$(cat "$WORK/init-depends-postgres")')"

[ "$(cat "$WORK/serve-depends-init")" = "service_completed_successfully" ] \
  && pass "serve waits for jobs-init to complete" \
  || fail "serve must depend on jobs-init: condition: service_completed_successfully (got '$(cat "$WORK/serve-depends-init")')"

[ "$(cat "$WORK/init-image")" = "$(cat "$WORK/postgres-image")" ] \
  && pass "jobs-init uses the same postgres image as repo-postgres (psql matches the server)" \
  || fail "jobs-init image '$(cat "$WORK/init-image")' differs from repo-postgres '$(cat "$WORK/postgres-image")'"

grep -q '^PGPASSWORD=\${PROTOMOLT_REPO_DB_PASSWORD:?' "$WORK/init-env" \
  && pass "jobs-init takes the repo role password from the required stack variable" \
  || fail "jobs-init must set PGPASSWORD from \${PROTOMOLT_REPO_DB_PASSWORD:?...}"

grep -q '^JOBS_DB=\${PROTOMOLT_JOBS_DB_NAME:-jobs}$' "$WORK/init-env" \
  && pass "jobs-init names the database from PROTOMOLT_JOBS_DB_NAME, default jobs" \
  || fail "jobs-init must set JOBS_DB=\${PROTOMOLT_JOBS_DB_NAME:-jobs}"

grep -q '^PROTOMOLT_JOBS_JDBC=jdbc:postgresql://repo-postgres:5432/\${PROTOMOLT_JOBS_DB_NAME:-jobs}$' "$WORK/serve-env" \
  && pass "serve points PROTOMOLT_JOBS_JDBC at the database jobs-init creates" \
  || fail "serve must set PROTOMOLT_JOBS_JDBC to jdbc:postgresql://repo-postgres:5432/\${PROTOMOLT_JOBS_DB_NAME:-jobs}"

grep -q '^PROTOMOLT_JOBS_USER=\${PROTOMOLT_REPO_DB_USER:-documents}$' "$WORK/serve-env" \
  && pass "serve connects as the role that owns the database" \
  || fail "serve must set PROTOMOLT_JOBS_USER=\${PROTOMOLT_REPO_DB_USER:-documents}"

grep -q '^PROTOMOLT_JOBS_PASSWORD=\${PROTOMOLT_REPO_DB_PASSWORD:?' "$WORK/serve-env" \
  && pass "serve takes the jobs password from the required stack variable" \
  || fail "serve must set PROTOMOLT_JOBS_PASSWORD from \${PROTOMOLT_REPO_DB_PASSWORD:?...}"

if [ "$failures" -gt 0 ]; then
  printf '%d failure(s)\n' "$failures" >&2
  exit 1
fi
echo "PASS: jobs-init"
