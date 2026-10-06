# Historical command identity and replay authorization

Canonical construction accepts validated historical selectors while execution
entry points explicitly refuse them. Historical selectors contribute to aggregate
source/byte limits, preserve slots and use canonical revision/object UUIDs. Tests
distinguish operation identity from selected revision identity and retain existing
golden command fixtures.

Real PostgreSQL tests authorize historical sources for successful replay, rejection
replay and precondition checks, then revoke READ and verify refusal in all three.
Historical revisions do not become current-head CAS conditions. Typed operation
admission, generic publication-codec admission and claim acquisition leave no
operation or claim rows when refused. Candidate and assessment entry points refuse
before touching supplied fragments, invoking a resolver or reserving payload bytes.

SQL assessment fixtures now admit real canonical protobuf command envelopes. Their
low-level artifact declarations remain deliberately synthetic SQL guard fixtures;
they are not end-to-end publication evidence. No database guard was relaxed.

Validation:

- SPI command tests and container historical authorization, operation admission,
  replay, session, journal, assessment and retention cases passed in 35 seconds.
  Local log: `/tmp/protomolt-historical-canonical-qualified.log`.
- After adding the candidate/assessment refusal regression, all
  `DocumentPublicationAssessmentTest` and `DocumentAssessment*IT` cases passed:
  92 tests, zero failures, errors or skips, in 32 seconds. Local log:
  `/tmp/protomolt-historical-assessment-qualification.log`.
- Sol reviewed the execution gates, typed admission and replay authorization;
  no material blocker remained. `git diff --check` passed.

This is a branch checkpoint, not restore availability. Historical assessment-slot
staging, schema provenance, commit reference binding and recovery still need shared
execution integration. Ordinary journaled publication and automatic claim transfer
remain disabled. No hosted CI, merge or deployment is established here.
