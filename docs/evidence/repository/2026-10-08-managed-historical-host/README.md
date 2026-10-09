# Explicit managed historical publication

Parent: `2d73c42af57952af9003bac71044df9a7795d1f4`.

`ManagedPublicationOptions` now has an explicit historical opt-in with positive
generation capacity. It requires a separately configured recovery authority and
preserves that selection through transport/authority option changes. The existing
constructors leave historical execution disabled. `ManagedDocumentServices` selects
the shared historical runtime only on this opt-in. Wire operations and protobuf
definitions are unchanged; this extends Java host composition.

The public `buildBoundedDocumentsHosted` probe runs with actual PostgreSQL, Redis
and Git schema storage. In each enabled case it publishes an ordinary typed document,
then reuses its historical core and uploads fresh parsed content. Library-first
and authenticated in-process gRPC-first calls both commit once; retries through
both paths return the exact receipt. Only the new parsed root resolves a live schema.
Retained-schema reads recover both original and new documents. Every reused ordinal
keeps its object ID, backend generation, realm, namespace, key, size, digest and
provider version. Redis has no provider version here; the test preserves absence.

Disabled cases refuse history through the existing library unsupported-operation
exception and gRPC UNIMPLEMENTED status, without schema or recovery work. Ordinary
publication still succeeds with either option. The hosted reader becomes QUIESCED
on close and the service closes schema admission once.

Construction now retains schema ownership with the caller until the full facade is
ready. Failure after runtime construction drains that runtime. Tests using an actual
Git registry resolver check untransferred ownership, repeated shutdown and retry
after injected close acknowledgement loss. Observation failures in both ordinary
and historical host configurations preserve usable caller schema access and leave
no additional active reader. This does not inject every possible post-construction
failure or prove disposal after arbitrary VM errors.

```sh
./gradlew :protomolt-repo-service:test \
  --tests '*ManagedPublicationOptionsTest' \
  --tests '*ManagedSchemaOwnershipTest' \
  --tests '*BoundedDocumentOptionsTest' \
  --tests '*ManagedSchemaHostIT' \
  :protomolt-repo-container:admissionStorageTest \
  --tests '*DocumentAssessmentStorageRuntimeTest.managedHistoricalHostBinding' \
  --tests '*DocumentAssessmentStorageRuntimeTest.boundedPublicHostBinding' \
  --max-workers=2 --console=plain
```

The lifecycle and host run passed in 27 seconds. After strengthening ordinal coverage,
the final run passed in 25 seconds; unchanged service tests were up-to-date. Archived
XML records 17 cases with zero failures, errors or skips. Two cases are aggregate
production-JAR host probes. Sol reviewed API validation, ownership transfer, retryable
close and physical reuse; its suggested close and ordinal checks were added.

Initial fixture corrections addressed a null Redis provider version and the existing
disabled-mode exception mapping. Neither required changing production provider semantics.

Still required: cold recovery through the hosted builder, unavailable retained backend
identity, and shutdown while historical work is held. The managed host still accepts
only its configured generation/profile. These results do not establish multiple-backend
routing, listener/socket failure recovery, performance, horizontal scale or deployment.
