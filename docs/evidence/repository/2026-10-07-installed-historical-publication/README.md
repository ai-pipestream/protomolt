# Installed historical owner: publication across calls

Base `706f556a9`, plus the test sources recorded in `sources.sha256`. No production
implementation or protobuf changes in this checkpoint. Sol reviewed the call/resource
lifetimes and assertions without finding a blocking defect.

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --max-workers=2 --console=plain
```

Exit 0, BUILD SUCCESSFUL in 8m43s. The aggregate JUnit test has zero failures/errors/
skips. The initial host log contains `SCOPED_INSTALLED_HISTORICAL_MULTICALL_PUBLICATION_OK`
and `OBSERVED_SQL_HOST_OK`; the terminal JUnit report and Gradle output establish that
subsequent restart/crash/recovery checks also finished. The initial host log alone is
not evidence of those later phases. No operation deadline or aggregate timeout changed.

The new scenario reuses the production-JAR runtime observation, real PostgreSQL,
provider container and historical mixed-successor fixture. It uses a scoped execution
caller with a credential binding, and separate host process authority for recovery.
The predecessor actually expires. Historical provider versions are read again; fresh
payloads are explicitly resubmitted and checked against their declared size and hash.

Four distinct client calls exercise the private installed owner:

1. Attach the exact source Work, open execution, admit uploads, START and prepare the
   assessment. The runtime request barrier becomes idle while the owner stays retained.
2. Resume after source admission closes. START identity stays fixed; replacing the
   assessment refuses. Upload fresh bytes to the provider, verify the selected attempt,
   and CREATE from the original retained assessment and acknowledged START.
3. Resume and publish the owner's retained stage. Verify exact receipt/owner/command,
   authorized replay, current revision, every provider version's bytes, reused physical
   identity and fresh upload provenance.
4. Resume and refuse a repeated publication using the retained sticky execution flag.

SQL asserts two STARTs (original/successor), one assessment and one publication.
Shutdown drains the retained assessment/execution/capture and returns all owner byte
reservations to the starting value. Existing direct successor and mixed publication
cases remain; readback assertions were extracted into a shared test helper.

This qualifies private owner continuity, not managed runtime entry points. Public
historical routing remains disabled. Lost CREATE reply adoption, terminal retirement,
pre-install ownership, broader multi-source cases and library/gRPC qualification remain.
Hosted CI is not established by this local result.
