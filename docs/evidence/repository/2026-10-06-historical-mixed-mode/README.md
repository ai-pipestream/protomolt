# Historical mixed-mode source qualification

The historical assessment suite passed 42 cases with no failures or skips.
Command: `./gradlew :protomolt-repo-container:test --tests '*DocumentHistoricalRestoreAssessmentIT' --console=plain`.
The final run completed in 44 seconds. Compressed JUnit output is in
`assessment.xml.gz`.

The added two cases combine historical CORE and PARSED selections from different
revisions into an opaque target member. Exactly one source has a sealed typed
admission, in either source position. Both cases require the specific refusal
`Historical source cannot be downgraded to opaque mode`, no registry resolution,
and release of both historical pins and payload reservations. Source publications
and the target use explicit successive policy generations.

This uses PostgreSQL and real descriptor/admission paths, with the existing
fixture's explicitly synthetic physical provider observations. It does not qualify
provider reads, public restore activation, or recovery successors. Sol reviewed
the test and the separately recorded next host-drain integration boundary.
