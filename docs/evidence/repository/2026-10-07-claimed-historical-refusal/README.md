# Claimed historical publication refusal

Baseline: `61553bce2` (reviewed execution design, production unchanged from
`775e2814a`). Tested changes are fingerprinted in `source-sha256.txt` and committed
with this evidence. This is an early-refusal fix, not claimed historical execution
qualification.

`Historical.publish` previously promoted/consumed its assessment and staged schema
claims before `commitHistorical` rejected an execution-claim-backed owner. The
refusal now happens at method entry, before either effect. The downstream commit
guard remains intact.

## Failure before fix

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentHistoricalPublicationIT.unsupportedClaimedPublicationDoesNotConsumeAssessmentOrStageSchemas' \
  --max-workers=2 --console=plain
```

Exit 1, 8 seconds, one executed test failed. It received the expected unsupported
publication error, then inspecting that same assessment failed with
`Publication assessment is closed or transferred`. See `red/` for the original
log and XML. No fixture bring-up failure or test adjustment preceded this result.

## Verification after fix

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentHistoricalPublicationIT' \
  --tests '*DocumentHistoricalMixedMemberPublicationIT' \
  --tests '*DocumentHistoricalMultiRevisionPublicationIT' \
  --max-workers=2 --console=plain
```

Exit 0, 23 seconds. Nine tests executed: five historical publication, two mixed
member and two multiple-revision cases; zero failures, errors or skips. See
`green/` for separate XML files and Gradle output.

The new test creates a real PostgreSQL 18 historical capture, execution claim,
fixed modes and admitted owner. It attempts publication twice and verifies after
each refusal that the assessment can still be inspected, the target operation
has zero schema artifact claims and its replay remains PENDING. Initial source
provider observations are synthetic, as disclosed by the existing fixture. This
test does not measure provider I/O or qualify claimed CREATE, restart, successor
execution, transport or public restore.

Sol reviewed the design and this production/test diff with no remaining blocker.
The private method now refuses unsupported claimed owners before caller checks;
no public claimed execution path is mounted. Local tests, review and remote push
are separate from hosted CI, merge and deployment; this evidence claims only the
local validation and review described above.
