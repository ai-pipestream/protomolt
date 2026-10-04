package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.repo.admission.DocumentAdmissionPolicy;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.blob.spi.BackendIdentity;
import ai.protomolt.proto.repo.codec.*;
import ai.protomolt.proto.repo.container.lifecycle.DocumentEventFactory;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.StringValue;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiConsumer;
import jakarta.persistence.EntityManager;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.Context;

/** Real byte/descriptor admission and PostgreSQL, with explicitly synthetic provider observations. */
final class DocumentSchemaRetentionFixture {
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    record Fixture(DocumentPublicationCommand command, RepositoryOperationLedger.Owner owner, DocumentSchemaBatch batch,
            DocumentSchemaRetention retention, DocumentOperationUploadAdmission.Prepared prepared,
            DocumentSelectedAttemptLedger.Selected selected, DocumentCommandContent content) {}

    static Fixture prepare(Context c) throws Exception { return prepare(c, true); }

    static Fixture prepare(Context c, boolean typed) throws Exception { return prepare(c, typed, false); }

    static Fixture prepare(Context c, boolean typed, boolean explicitSchema) throws Exception {
        var ownership = OwnershipContext.newBuilder().setAccountId("account").setDatasourceId("source")
                .setSecurity(DocumentSecurity.getDefaultInstance()).build();
        var document = Document.newBuilder().setDocId("typed-fixture").setOwnership(ownership)
                .setStructuredData(Any.pack(StringValue.of("real payload"), "type.test")).build();
        var profile = new ManagedBackendLedger.Profile(new BackendIdentity("test-location", "test-location/v1",
                Map.of("endpoint", "synthetic")), "schema-retention");
        new ManagedBackendLedger(c.tx()).bind("schema-retention", profile);
        var drive = new DriveRecord(); drive.driveId = UUID.randomUUID(); drive.accountId = "account";
        drive.name = "schema-retention"; drive.bucket = "bucket"; drive.prefix = "root";
        drive.provider = "test-location"; drive.driveType = "CUSTOM"; drive.status = "ACTIVE";
        new DriveLedger(c.tx()).insert(drive);
        var member = DocumentPublicationMember.newBuilder().setMemberId("member").setDriveId(drive.driveId.toString())
                .setOwnership(ownership).setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE)
                .setDestination(DocumentRevisionCondition.newBuilder().setIfAbsent(true).setAddress(NodeAddress.newBuilder()
                        .setAccountId("account").setDocId(document.getDocId()).setGraphId("graph").setGraphAddressId("node")));
        var fragments = new HashMap<Integer, ByteString>();
        for (var part : DocumentPartCodec.split(document, PartLayouts.document())) {
            fragments.put(member.getPartsCount(), ByteString.copyFrom(part.bytes()));
            member.addParts(DocumentPublicationPart.newBuilder().setSlot(DocumentPublicationSlot.newBuilder()
                    .setPart(part.part()).setSubKey(part.subKey())).setUpload(PublicationUpload.newBuilder()
                    .setSizeBytes(part.bytes().length).setSha256(DocumentPartCodec.sha256Hex(part.bytes()))
                    .setContentType("application/protobuf")));
        }
        if (explicitSchema) member.setStructuredSchema(PublicationSchemaCondition.newBuilder()
                .setTypeName(StringValue.getDescriptor().getFullName())
                .setDescriptorFingerprint(DescriptorFingerprints.fingerprint(DescriptorFingerprints.closure(StringValue.getDescriptor()))));
        var command = new DocumentPublicationCommand(DocumentPublicationIntent.newBuilder().setEncodingVersion(1)
                .setAccountId("account").setOperationId(UUID.randomUUID().toString()).addMembers(member).build());
        var owner = new RepositoryOperationLedger(c.tx()).admit(new RepositoryOperationLedger.Key("account", "principal", command.operationId()),
                command, UUID.randomUUID(), Duration.ofMinutes(5)).owner().orElseThrow();
        var policy = DocumentAdmissionPolicy.of(DocumentSchemaPolicy.newBuilder().setEncodingVersion(1).setAccountId("account")
                .setMode(typed ? DocumentSchemaPolicyMode.DOCUMENT_SCHEMA_POLICY_MODE_TYPED_REQUIRED : DocumentSchemaPolicyMode.DOCUMENT_SCHEMA_POLICY_MODE_OPAQUE_ALLOWED).setAnyResolvedSchema(true)
                .setValidationProfile("protomolt-retained-schema-admission/v1").setLimits(DocumentSchemaPolicyLimits.newBuilder()
                        .setMaxFragments(32).setMaxFragmentBytes(4_000_000).setMaxRoots(100).setMaxEvidenceBytes(4_000_000)
                        .setMaxBindings(20).setMaxRetainedBytes(16_000_000).setMaxDecodedBytes(1_000_000)).build(), () -> {});
        var payload = definition(StringValue.getDescriptor(), true);
        var proof = policy.prepareAndCheck(ByteString.copyFrom(HexFormat.of().parseHex(command.sha256())),
                command.intent().getMembers(0), fragments, definition(Document.getDescriptor()), ignored -> payload, () -> {});
        var batch = DocumentSchemaBatch.prepare(command, new DocumentSchemaPolicies.Selection("account", 1, policy), typed ? Map.of("member", proof) : Map.of(), () -> {});
        batch.stage(new RepositorySchemaArtifacts(c.tx()), owner, () -> {});
        var placements = Map.of(drive.driveId, DocumentUploadPlan.Placement.sample(drive, "schema-retention", profile));
        var prepared = DocumentOperationUploadAdmission.prepare(command, placements, Map.of("member", UUID.randomUUID()), Duration.ofMinutes(5));
        var attempt = new DocumentOperationUploadAdmission(c.tx(), new DriveLedger(c.tx())).admit(CALLER, owner, prepared).getFirst();
        var selected = new DocumentSelectedAttemptLedger.Selected("member", 1, attempt.id(), attempt.token());
        var uploads = prepared.plan().members().getFirst().attempt().orElseThrow().uploads();
        // These are synthetic physical observations; no provider SDK call is claimed.
        new DocumentSelectedAttemptLedger(c.tx()).verifyBatch(owner, selected, uploads.stream().map(upload -> {
            var object = upload.object();
            return new DocumentSelectedAttemptLedger.Observation(object.objectKey(), object.size(), object.sha256(),
                    object.contentType(), "fixture-version", "fixture-etag");
        }).toList());
        var content = explicitSchema ? DocumentCommandContent.fromSchema(batch, "member", () -> {})
                : DocumentCommandContent.check(command, "member", fragments, false,
                new DocumentRevisionAssembly.Limits(4_000_000, 100, 100, 100, 1_000_000), () -> {});
        return new Fixture(command, owner, batch, typed ? DocumentSchemaRetention.prepare(batch, "member") : null, prepared, selected, content);
    }

    static UUID publish(Context c, Fixture f, BiConsumer<EntityManager, UUID> beforeSeal) {
        return publish(c, f, false, beforeSeal);
    }

    @FunctionalInterface interface ManifestCheck {
        void accept(EntityManager em, UUID revision, DocumentSchemaManifest manifest);
    }

    static UUID publishWithManifest(Context c, Fixture f, ManifestCheck beforeSeal) {
        return publish(c, f, false, beforeSeal);
    }

    /** Omission injects an incomplete SQL candidate, not a successful provider response. */
    static UUID publish(Context c, Fixture f, boolean omitCore, BiConsumer<EntityManager, UUID> beforeSeal) {
        return publish(c, f, omitCore, (em, revision, manifest) -> beforeSeal.accept(em, revision));
    }

    static UUID publishWithSnapshot(Context c, Fixture f, BiConsumer<EntityManager, DocumentCommitWriter.Candidate> beforeWrite) {
        return publish(c, f, false, (em, revision, manifest) ->
                f.retention().write(em, f.owner(), revision, () -> {}), beforeWrite);
    }

    private static UUID publish(Context c, Fixture f, boolean omitCore, ManifestCheck beforeSeal) {
        return publish(c, f, omitCore, beforeSeal, (em, candidate) -> {});
    }

    static UUID publishBound(Context c, Fixture f, BiConsumer<EntityManager, DocumentCommitWriter.Candidate> afterBinding,
            ManifestCheck beforeSeal) {
        return publish(c, f, false, beforeSeal, afterBinding, true);
    }

    private static UUID publish(Context c, Fixture f, boolean omitCore, ManifestCheck beforeSeal,
            BiConsumer<EntityManager, DocumentCommitWriter.Candidate> beforeWrite) {
        return publish(c, f, omitCore, beforeSeal, beforeWrite, false);
    }

    private static UUID publish(Context c, Fixture f, boolean omitCore, ManifestCheck beforeSeal,
            BiConsumer<EntityManager, DocumentCommitWriter.Candidate> beforeWrite, boolean bound) {
        return c.tx().inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em, f.owner());
            RepositoryOperationLedger.requireCommand(em, f.owner().key(), f.command());
            if (bound) f.batch().lockPolicy(em, f.owner(), () -> {});
            else DocumentSchemaPolicies.lockUnboundWriter(em, "account");
            var plan = f.prepared().plan();
            var locked = DocumentAdmissionAuthorization.lockAndAuthorize(em, CALLER, plan, DocumentAdmissionAuthorization.prepare(plan));
            plan.members().getFirst().placement().drive().lock(em, new DriveLedger(c.tx()));
            var parts = DocumentCommitParts.bind(em, f.owner(), plan, Map.of("member", f.selected()), DocumentReuseAdmission.prepare(plan), () -> {});
            var manifest = DocumentSchemaManifest.prepare(f.batch(), "member", parts, () -> {});
            f.batch().lockArtifacts(em, f.owner(), () -> {});
            var candidate = DocumentCommitWriter.prepare(plan.members().getFirst(), f.content(), parts, locked, Map.of(), Instant.now(), () -> {});
            String decision = bound ? DocumentSchemaAdmissionBinding.insert(em, f.owner(), f.batch(), candidate, parts, () -> {}) : "OPAQUE";
            beforeWrite.accept(em, candidate);
            var row = em.merge(candidate.row()); em.flush(); em.refresh(row);
            var event = DocumentEventFactory.savedWithoutDelivery(row, row.updatedAt);
            var revision = candidate.revision();
            // Direct SQL storage fixture leaves an unsealed revision for the retention helper.
            em.createNativeQuery("""
                    INSERT INTO document_revision_commits(revision_id,node_id,publication_revision,account_id,principal,operation_id,
                     owner_generation,member_id,member_ordinal,selection_revision,event_id,metadata_version,admission_mode)
                    VALUES(:revision,:node,:mutation,'account','principal',:operation,:generation,'member',0,1,:event,1,:decision)
                    """).setParameter("revision", revision).setParameter("node", row.nodeId).setParameter("mutation", row.mutationRevision)
                    .setParameter("operation", f.command().operationId()).setParameter("generation", f.owner().generation())
                    .setParameter("event", event.eventId).setParameter("decision", decision).executeUpdate();
            em.createNativeQuery("""
                    INSERT INTO document_revision_publications(revision_id,node_id,publication_revision,body,published_at,native_binding)
                    SELECT :revision,node_id,mutation_revision,document_publication_body(documents),clock_timestamp(),:revision
                    FROM documents WHERE node_id=:node
                    """).setParameter("revision", revision).setParameter("node", row.nodeId).executeUpdate();
            em.createNativeQuery("""
                    INSERT INTO document_revision_parts(revision_id,revision_ordinal,part,sub_key,object_id)
                    SELECT :revision,q.ordinal,q.part,q.sub_key,q.object_id
                    FROM jsonb_to_recordset(CAST(:parts AS jsonb)) q(ordinal integer,part integer,sub_key text,object_id uuid)
                    WHERE NOT :omit OR q.part<>1
                    """).setParameter("revision", revision).setParameter("parts", candidate.references())
                    .setParameter("omit", omitCore).executeUpdate();
            beforeSeal.accept(em, revision, manifest);
            em.createNativeQuery("UPDATE document_revision_publications SET projection_sealed=true WHERE revision_id=:id")
                    .setParameter("id", revision).executeUpdate();
            em.createNativeQuery("INSERT INTO document_revision_current(node_id,revision_id) VALUES(:node,:revision)")
                    .setParameter("node", row.nodeId).setParameter("revision", revision).executeUpdate();
            em.persist(event); em.flush();
            var success = DocumentPublicationResult.newBuilder().setOperationId(f.command().operationId().toString())
                    .setAccountId("account").setPrincipal("principal").setOwnerGeneration(f.owner().generation())
                    .setCommandEncodingVersion(1).setCommandSha256(f.command().sha256()).addMembers(DocumentPublishedRevision.newBuilder()
                            .setMemberId("member").setAddress(f.command().intent().getMembers(0).getDestination().getAddress())
                            .setRevisionId(revision.toString()).setMutationRevision(row.mutationRevision)).build();
            var encoded = DocumentPublicationResultCodec.encode(f.command(), success, "principal", f.owner().generation());
            em.createNativeQuery("""
                    INSERT INTO repository_operation_success(account_id,principal,operation_id,owner_generation,
                     command_codec,command_version,command_sha256,result_codec,result_version,result_bytes,result_sha256,member_count)
                    SELECT account_id,principal,operation_id,:generation,command_codec,command_version,command_sha256,
                     'document-publication-result',1,:bytes,sha256(:bytes),1 FROM repository_operations WHERE operation_id=:operation
                    """).setParameter("generation", f.owner().generation()).setParameter("bytes", encoded.bytes().toByteArray())
                    .setParameter("operation", f.command().operationId()).executeUpdate();
            em.createNativeQuery("SET CONSTRAINTS ALL IMMEDIATE").executeUpdate();
            return revision;
        });
    }

    static DocumentSchemaAdmission.Definition definition(com.google.protobuf.Descriptors.Descriptor type) {
        return definition(type, false);
    }

    private static DocumentSchemaAdmission.Definition definition(com.google.protobuf.Descriptors.Descriptor type, boolean withSource) {
        var closure = DescriptorFingerprints.closure(type); var bytes = closure.toByteString();
        var source = withSource ? ByteString.copyFromUtf8("synthetic retained source bundle fixture; integrity only") : null;
        var compilation = SchemaCompilationProvenance.newBuilder()
                .setOrigin(SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_IMPORTED_DESCRIPTOR)
                .setEvidence(SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_UNKNOWN)
                .setUnknownCompilerReason("synthetic fixture compiler provenance unavailable")
                .setAdmissionRuntime(SchemaToolIdentity.newBuilder().setName("test-runtime").setVersion("1"));
        if (source != null) compilation.setSourceArtifactSha256(DocumentPartCodec.sha256Hex(source.toByteArray()));
        var metadata = RepositorySchemaAsset.newBuilder().setTypeUrl("type.test/" + type.getFullName())
                .setArtifactSha256(DocumentPartCodec.sha256Hex(bytes.toByteArray()))
                .setSchema(PublicationSchemaCondition.newBuilder().setTypeName(type.getFullName())
                        .setDescriptorFingerprint(DescriptorFingerprints.fingerprint(closure)))
                .setCompilation(compilation).build();
        return new DocumentSchemaAdmission.Definition(metadata, bytes, Optional.ofNullable(source));
    }
}
