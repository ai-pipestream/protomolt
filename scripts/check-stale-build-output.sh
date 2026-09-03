#!/usr/bin/env bash
# Finds build output that no longer belongs to any source tree.
#
# Git does not delete untracked directories on checkout, so a module that is
# renamed or moved leaves its build/ behind, and a package that is renamed
# leaves its old .class files sitting in the compiled output. Both survive
# indefinitely, and neither is visible to git status.
#
# The second kind breaks builds in a way that looks like a code fault. JUnit
# scans the whole test output directory, loads an orphaned class, and fails
# with NoClassDefFoundError naming a type that was deleted weeks earlier. The
# tree is fine; only the working copy is stale. CI never sees it because CI
# starts empty every time, so this only ever bites a developer.
#
# The ADR-002 search rename on 2026-08-20 left 27 orphaned module trees and
# 595 MB behind in one working copy, and stale classes from the old chunk
# package failed the suite on 2026-09-01.
#
# Run from the repository root. Exits non-zero and names what to remove.
set -euo pipefail
cd "$(dirname "$0")/.."

say() { printf '== %s\n' "$1"; }
found=0

say "modules whose build output has no source"
while read -r build_dir; do
    module="$(dirname "$build_dir")"
    if [ -e "$module/build.gradle" ] || [ -e "$module/build.gradle.kts" ]; then continue; fi
    if [ -d "$module/src" ]; then continue; fi
    printf '   orphaned module output: %s\n' "$module"
    found=1
# -prune stops the descent at each build directory, so a generated Java package
# that happens to be named build (protovalidate emits build.buf.validate) is
# never mistaken for a module's output directory.
done < <(find . -type d -name build \
    -not -path './.git/*' -not -path '*/node_modules/*' -not -path '*/.claude/*' \
    -prune -print 2>/dev/null)
[ "$found" -eq 0 ] && printf '   none\n'

say "compiled test packages with no matching source package"
python_status=0
python3 - <<'PYEOF' || python_status=$?
import os, sys

stale = []
for root, dirs, files in os.walk('.'):
    dirs[:] = [d for d in dirs if d not in ('.git', 'node_modules', '.claude')]
    if not root.endswith(os.path.join('build', 'classes', 'java', 'test')):
        continue
    module = root[: -len(os.path.join('build', 'classes', 'java', 'test'))]
    for pkg_root, _, pkg_files in os.walk(root):
        if not any(f.endswith('.class') for f in pkg_files):
            continue
        package = os.path.relpath(pkg_root, root)
        if package == '.':
            continue
        # Accept the package if any source or generated tree declares it.
        candidates = [
            os.path.join(module, 'src', 'test', 'java', package),
            os.path.join(module, 'src', 'test', 'kotlin', package),
        ]
        generated = os.path.join(module, 'build', 'generated')
        if os.path.isdir(generated):
            for gen_root, _, _ in os.walk(generated):
                candidates.append(os.path.join(gen_root, package))
        if not any(os.path.isdir(c) for c in candidates):
            stale.append(os.path.join(pkg_root))

for path in sorted(stale):
    print('   orphaned compiled package:', path)
if not stale:
    print('   none')
sys.exit(1 if stale else 0)
PYEOF

if [ "$found" -ne 0 ] || [ "$python_status" -ne 0 ]; then
    printf '\n'
    say "FAIL: stale build output"
    printf '   This working copy carries output from modules or packages that no longer\n'
    printf '   exist. Tests can load those classes and fail naming types that were deleted.\n'
    printf '   Remove the paths above, or run: git clean -xdf -e .idea -e .env\n'
    exit 1
fi

say "PASS: no stale build output"
