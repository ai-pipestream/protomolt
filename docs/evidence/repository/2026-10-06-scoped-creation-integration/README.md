# Scoped creation integration qualification

Base: `6e2616f692993b9fcc218cc802f897b4f2e98af8`.

Journaled initial admission, upload capture/staging, assessment creation, final
publication and successor activation/attachment now use the configured host drive
gate and exact live creation grant. The placement digest is prepared outside SQL;
selected snapshots and backend profiles are checked under locks before credential
and grant locks. Source READ and existing-target policy checks remain in place,
and complete authorization precedes revision-conflict disclosure. Older internal
constructors without the host gate remain closed to scoped absent creation.

Pending/rejected observations check the original key and live grant for absent
targets. Committed receipt delivery checks current READ, original key identity and
live credential generation, while ignoring grant expiry/revocation. Observation
checks do not establish selected-placement or execution authority.

## Evidence

The archives retain XML and binary results from successive implementation stages;
earlier archives are not claims that later code existed at those stages.

| Archive | Coverage and result |
| --- | --- |
| `initial-admission-red.tar.gz` | One expected failure: installed live grant still refused by the original shared admission path |
| `initial-admission-green.tar.gz` | 28 passing cases: initial admission, history policy, exact grant success, revoked/substituted key, changed placement, rejecting host gate |
| `execution-green.tar.gz` | 150 passing cases: upload, assessment, commit and initial admission regression; real SQL staging refuses grant revocation |
| `observation-green.tar.gz` | 68 passing cases: pending replay, rejection, commit and grant/key identity; pending observation refuses grant revocation |
| `runtime-affected-green.tar.gz` | 114 passing cases: journaled/session/successor, grant and real-provider commit suites |
| `scoped-publication-green.tar.gz` | Initial opaque scoped publication and grant-vs-key revocation receipt semantics |
| `scoped-typed-publication-green.tar.gz` | Two passing cases: scoped typed and opaque publication, exact stored fragment readback, typed historical validation with retained schemas |

All green runs had zero skipped tests. SQL tests use PostgreSQL 18. Real provider
cases use LocalStack's versioned S3 adapter, not a performance substitute for RustFS.
The typed scoped test supplies real descriptor definitions from generated fixture
types; it does not exercise live schema-registry transport. Historical validation
has no resolver callback and restores the original StringValue Any payload.

Commands used for the final runtime and typed checks:

```sh
./gradlew :protomolt-repo-container:test --tests '*RepositoryCreationGrantsIT' --tests '*RepositorySuccessor*IT' --tests '*DocumentPublicationCommitIT' --tests '*DocumentJournaledSessionsIT' --tests '*DocumentPublicationSessionIT' --console=plain
./gradlew :protomolt-repo-container:test --tests '*DocumentPublicationCommitIT.scopedJournaledPublicationUsesRealProviderAndKeepsReceiptAfterGrantRevocation' --console=plain
```

Sol reviewed the integration without finding a production blocker. Review covered
lock ordering, preservation of configured backend gates, full source/target policy
checks, separation of observation from execution proof, and distinct grant/key
revocation behavior. The initial integration regression changed unbound denial to
UNAUTHENTICATED; this was corrected to preserve the existing NOT_FOUND behavior.

## Remaining qualification

Scoped successor publication, precise concurrent final-commit/revocation races,
mixed source/target policy cases under a valid grant, and expiry between execution
phases remain. Internal provisioning still needs bounded retention/lifecycle rules
before an external endpoint can be exposed. No protobuf contract or public RPC was
added. This checkpoint does not close the broader recovery, archival, pruning,
provider conformance, RustFS performance or progressive-hydration goal.

## Packaged qualification

`./gradlew :protomolt-repo-container:admissionStorageTest --console=plain` passed
on the integrated production source. The packaged runtime test passed without
skips; XML and binary results are retained in `packaged-green.tar.gz`. This is
local qualification, not hosted CI, merge, deployment or performance certification.
