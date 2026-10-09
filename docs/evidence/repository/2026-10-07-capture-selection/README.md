# Capture selection qualification

Base `4db6d6c35bc227658e2341c750e9248a8aee3041`, branch
`refactor/repository-composition`, plus the tested file in `source-sha256.txt`.
No production changes or public behavior are introduced in this checkpoint.

Command: exit 0, 15 seconds, nine tests, no failures/errors/skips.

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentCaptureAdmissionClosureIT' --max-workers=2 --console=plain
```

Four added cases exercise the private capture coverage helper:

- A later capture completes through its real owning lifecycle. Qualification still
  refuses the earlier undrained capture. Actual release and reader quiescence then
  permit recovery of the original capture and qualification of both batches.
- Repeating the historical selector in another destination still yields one pin
  tuple per capture and follows that same two-capture lifecycle.
- A two-part canonical selection gets an additional batch recording only one of
  its real live pins, with correct SQL count/digest. All SQL guards remain enabled.
  Both batches get genuine recovery receipts after their reader is quiescent, but
  qualification refuses the incomplete selection with DATA_LOSS.
- Two actual captures of one object supply an internally consistent batch with
  two pin IDs for the same source/revision/object tuple. After actual cleanup and
  recovery of all batches, qualification refuses the duplication with DATA_LOSS.

The negative fixtures call private low-level capture insertion deliberately;
they neither disable guards nor fabricate success/drain rows. They demonstrate
why SQL structural validity and drain evidence do not alone prove canonical
object coverage. PostgreSQL and pin/reader lifecycle behavior are real. Historical
provider observations remain explicitly synthetic seed-fixture inputs.

Sol reviewed the new cases and their lifecycle cleanup without a blocker.

The earlier/later captures share one claim owner. Cross-revision reuse of one
physical object, multiple execution-owner epochs, historical legacy initial-batch
gaps and atomic root release remain separate acceptance requirements. No whole-stack
or performance conclusion follows from this run. Retained JUnit XML trailing
whitespace is normalized.
