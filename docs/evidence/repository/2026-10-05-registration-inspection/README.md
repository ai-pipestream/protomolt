# Registration state inspection

Local verification on 2026-10-05 against PostgreSQL 18 Alpine:

```
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentPublicationRegistrationInspectionIT' \
  --tests '*DocumentPublicationPreparationJournalIT' \
  --tests '*DocumentPublicationModesJournalIT' \
  --tests '*DocumentAssessmentStartJournalIT' --console=plain
```

39 tests passed, no failures or skips; Gradle completed in 28 seconds.
The five new cases exercise registration boundaries through actual journals,
private authority and original claim fencing, real lease expiry, concurrent mode
registration, and cancellation with all payload reservations released. Existing
journal suites provide 34 additional regression checks. The first run exposed
three test-fixture mistakes: seed object equality and two expiry updates refused
by database guards. The final fixture compares identity fields and waits for
legitimate short leases to expire; production guards remain intact.

Sol reviewed the inspector with no blocking findings. It is internal state
observation using existing claim fences, not a restored execution handle. The
fixture uses synthetic document observations and proves neither provider behavior
nor process restart. OWNER_ADMITTED is live only at the database observation;
ASSESSMENT_STARTED establishes the durable start marker, not a committed assessment.
TERMINAL requires separate authorized replay before delivering a receipt.

No host activation, automatic transfer, deployment or hosted CI is established.
