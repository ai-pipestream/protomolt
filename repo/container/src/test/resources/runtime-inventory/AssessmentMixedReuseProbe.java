package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.*;
import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.codec.*;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.StringValue;
import java.time.*;
import java.util.*;

/** Mixed candidate built from a real native publication and real versioned provider uploads. */
public final class AssessmentMixedReuseProbe {
    private static final RepositoryCaller ADMIN = new RepositoryCaller("principal", true);
    private static final DocumentRevisionAssembly.Limits LIMITS =
            new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000);
    record Source(DocumentPublicationMember candidate, Map<Integer,ByteString> fragments,
            DocumentUploadPlan.Placement placement, UUID node, UUID revision, UUID object, int sourceOrdinal) {}
    record Uploads(DocumentOperationUploadAdmission.Prepared prepared,
            Map<String,DocumentSelectedAttemptLedger.Selected> selected) {}

    /** Publish before the account enables schema admission; no SQL success fixtures are inserted. */
    static Source publishSource(Tx tx, AssessmentProviderProbe provider) throws Exception {
        return publishSource(tx, provider, "mixed");
    }

    static Source publishSource(Tx tx, AssessmentProviderProbe provider, String prefix) throws Exception {
        return publishSource(tx, provider, prefix, false);
    }

    static Source publishSource(Tx tx, AssessmentProviderProbe provider, String prefix, boolean writable) throws Exception {
        var security = DocumentSecurity.newBuilder();
        com.google.protobuf.util.JsonFormat.parser().merge(
                "{\"permissions\":[{\"identityType\":\"public\",\"identity\":\"public\",\"access\":\"ACCESS_READ\"}]}", security);
        if (writable) security.addPermissions(AccessRule.newBuilder().setIdentityType("public")
                .setIdentity("public").setAccess(Access.ACCESS_WRITE));
        var ownership = OwnershipContext.newBuilder().setAccountId("account").setDatasourceId("source")
                .setSecurity(security).build();
        var document = Document.newBuilder().setDocId(prefix + "-source").setOwnership(ownership)
                .setStructuredData(Any.pack(StringValue.of("retained payload"), "type.test"))
                .setSearchMetadata(SearchMetadata.newBuilder().addSemanticResults(
                        SemanticProcessingResult.newBuilder().setResultId("chunk"))).build();
        var drive = new DriveRecord(); drive.driveId = UUID.randomUUID(); drive.accountId = "account";
        drive.name = "mixed-" + drive.driveId; drive.bucket = "namespace"; drive.prefix = "root";
        drive.provider = "s3"; drive.driveType = "CUSTOM"; drive.status = "ACTIVE";
        var drives = new DriveLedger(tx); drives.insert(drive);
        new ManagedBackendLedger(tx).bind("assessment-s3", provider.profile());
        var placement = DocumentUploadPlan.Placement.sample(drive, "assessment-s3", provider.profile());
        var member = DocumentPublicationMember.newBuilder().setMemberId("a").setDriveId(drive.driveId.toString())
                .setOwnership(ownership).setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE)
                .setDestination(DocumentRevisionCondition.newBuilder().setIfAbsent(true).setAddress(NodeAddress.newBuilder()
                        .setAccountId("account").setDocId(document.getDocId()).setGraphId(prefix + "-source").setGraphAddressId("node")));
        var bytes = new HashMap<Integer,ByteString>();
        for (var part : DocumentPartCodec.split(document, PartLayouts.document())) {
            bytes.put(member.getPartsCount(), ByteString.copyFrom(part.bytes()));
            member.addParts(DocumentPublicationPart.newBuilder().setSlot(DocumentPublicationSlot.newBuilder()
                    .setPart(part.part()).setSubKey(part.subKey())).setUpload(PublicationUpload.newBuilder()
                    .setSizeBytes(part.bytes().length).setSha256(DocumentPartCodec.sha256Hex(part.bytes()))
                    .setContentType("application/protobuf")));
        }
        var command = command(member.build()); var owner = owner(tx, command);
        var uploaded = upload(tx, provider, command, owner, placement, bytes);
        var content = DocumentCommandContent.check(command, "a", bytes, false, LIMITS, () -> {});
        var result = new DocumentPublicationCommit(tx, drives, false, false).commit(ADMIN, owner,
                uploaded.prepared(), Map.of("a", content), uploaded.selected(), () -> {});
        require(result.getMembersCount() == 1, "real source publication committed");
        UUID node = ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(member.getDestination().getAddress());
        var current = new DocumentLedger(tx).findByNodeId(node).orElseThrow();
        var publication = new DocumentPublicationLedger(tx).findForRead(current).orElseThrow();
        var core = publication.boundParts().stream().filter(p -> p.part().part() == DocumentPart.DOCUMENT_PART_CORE)
                .findFirst().orElseThrow();
        Object[] source = tx.readOnly(em -> (Object[]) em.createNativeQuery("""
                SELECT object_id,revision_ordinal FROM document_revision_parts
                WHERE revision_id=:revision AND part=:part AND sub_key=:key
                """).setParameter("revision", publication.revisionId()).setParameter("part", core.part().part().getNumber())
                .setParameter("key", core.part().subKey()).getSingleResult());
        var identity = PublicationObjectIdentity.newBuilder().setObjectId(source[0].toString())
                .setBackendGeneration(core.binding().generation()).setStorageRealm(core.binding().profile().storageRealm())
                .setNamespace(core.binding().namespace()).setObjectKey(core.part().key()).setProviderVersion(core.part().providerVersion())
                .setSizeBytes(core.part().size()).setSha256(core.part().sha256()).setContentType(core.part().contentType());
        var condition = member.getDestination().toBuilder().clearIfAbsent().setExpectedMutationRevision(current.mutationRevision);
        var candidate = member.clone().clearParts().setDestination(member.getDestination().toBuilder()
                .setAddress(member.getDestination().getAddress().toBuilder().setGraphId(prefix + "-destination")));
        // An empty first slot makes candidate ordinals differ from source ordinals.
        candidate.addParts(DocumentPublicationPart.newBuilder().setSlot(DocumentPublicationSlot.newBuilder()
                .setPart(DocumentPart.DOCUMENT_PART_BLOBS)).setEmpty(true));
        var shifted = new HashMap<Integer,ByteString>();
        for (int i = 0; i < member.getPartsCount(); i++) {
            var declaration = member.getParts(i);
            shifted.put(i + 1, bytes.get(i));
            candidate.addParts(declaration.getSlot().getPart() == DocumentPart.DOCUMENT_PART_CORE
                    ? declaration.toBuilder().clearUpload().setReuse(PublicationReuse.newBuilder()
                            .setSource(condition).setSourceSlot(declaration.getSlot()).setObject(identity)).build()
                    : declaration);
        }
        require(candidate.getPartsList().stream().anyMatch(part -> part.hasUpload()
                && part.getSlot().getPart() == DocumentPart.DOCUMENT_PART_CHUNKS), "candidate uploads CHUNKS");
        require(candidate.getPartsList().stream().filter(DocumentPublicationPart::hasReuse).count() == 1
                && candidate.getPartsList().stream().anyMatch(part -> part.hasReuse()
                        && part.getSlot().getPart() == DocumentPart.DOCUMENT_PART_CORE), "candidate reuses only CORE");
        var destination = AssessmentRestartProbe.seedDestinations(tx, List.of(candidate.build())).getFirst();
        return new Source(destination, Map.copyOf(shifted), placement, node, publication.revisionId(),
                (UUID) source[0], ((Number) source[1]).intValue());
    }

    static void run(Tx tx, AssessmentProviderProbe provider, Source source, DocumentSchemaPolicies.Selection active,
            DocumentAssessmentRuntimeObserver.Observation observation) throws Exception {
        var command = command(source.candidate()); var owner = owner(tx, command);
        var uploaded = upload(tx, provider, command, owner, source.placement(), source.fragments());
        var budget = new PayloadBudget(64_000_000);
        var definition = ObservedAssessmentProbe.asset(StringValue.getDescriptor());
        DocumentAssessmentCreation.Created retained;
        try (var assessment = DocumentPublicationAssessment.prepare(command, active,
                Map.of("a", DocumentPublicationCandidate.Mode.TYPED), Map.of("a", source.fragments()),
                Optional.of(ObservedAssessmentProbe.asset(Document.getDescriptor())),
                (member, occurrence) -> definition, budget, LIMITS, Instant.now(), () -> {})) {
            require(assessment.failure().isEmpty(), "mixed candidate passes real validator");
            retained = assessment.withRetentionEvidence(owner, observation, () -> {}, evidence -> {
                new RepositorySchemaArtifacts(tx).stage(owner, command, List.copyOf(evidence.artifacts(() -> {}).values()), () -> {});
                return new DocumentAssessmentCreation(tx, new DriveLedger(tx)).create(ADMIN, owner, uploaded.prepared(),
                        uploaded.selected(), evidence, UUID.randomUUID(), Instant.now().plusSeconds(120)
                                .truncatedTo(java.time.temporal.ChronoUnit.MICROS), budget, () -> {});
            });
        }
        require(budget.reservedBytes() == 0, "mixed assessment releases validation memory");
        Object[] slot = tx.readOnly(em -> (Object[]) em.createNativeQuery("""
                SELECT object_id,source_revision,source_ordinal,revision_ordinal FROM document_assessment_slots
                WHERE assessment_id=:id AND declaration='REUSE'
                """).setParameter("id", retained.assessment()).getSingleResult());
        require(slot[0].equals(source.object()) && slot[1].equals(source.revision())
                && ((Number) slot[2]).intValue() == source.sourceOrdinal()
                && ((Number) slot[3]).intValue() == source.sourceOrdinal() + 1, "exact original source and shifted ordinal");
        var rejected = RejectedAssessmentSourceProbe.prepare(tx, provider, source, active, observation);
        advanceSource(tx, provider, source, active);
        var selections = DocumentAssessmentRetainedSlots.uploadSelections(uploaded.selected());
        AssessmentReplayInputsProbe.run(tx, owner, command, selections, retained, source.node(), active.policy().sha256());
        provider.verifyReads(tx, owner, command, selections, retained, budget, Map.of("a", source.fragments()), source.node());
        AssessmentOperationReplayProbe.verifyMixed(tx, provider, owner, command, selections, retained, source.node(), observation);
        require(budget.reservedBytes() == 0, "mixed capture releases verification memory");
        long publications = tx.readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM document_revision_commits WHERE operation_id=:op")
                .setParameter("op", command.operationId()).getSingleResult()).longValue());
        require(publications == 0, "staged mixed candidate is not published");
        RejectedAssessmentSourceProbe.verify(tx, provider, source.node(), rejected, observation);
        System.out.println("ASSESSMENT_MIXED_REUSE_OK");
    }

    private static void advanceSource(Tx tx, AssessmentProviderProbe provider, Source source,
            DocumentSchemaPolicies.Selection active) throws Exception {
        var retainedCore = source.candidate().getPartsList().stream().filter(DocumentPublicationPart::hasReuse)
                .findFirst().orElseThrow();
        var member = source.candidate().toBuilder().setDestination(retainedCore.getReuse().getSource());
        var fragments = new HashMap<>(source.fragments());
        for (int ordinal = 0; ordinal < member.getPartsCount(); ordinal++) {
            var part = member.getParts(ordinal);
            if (part.hasEmpty()) continue;
            var bytes = fragments.get(ordinal);
            if (part.getSlot().getPart() == DocumentPart.DOCUMENT_PART_CORE) {
                bytes = Document.parseFrom(bytes).toBuilder().setStructuredData(
                        Any.pack(StringValue.of("new source payload"), "type.test")).build().toByteString();
                require(!bytes.equals(fragments.put(ordinal, bytes)), "new source CORE bytes differ");
            }
            member.setParts(ordinal, part.toBuilder().clearReuse().setUpload(PublicationUpload.newBuilder()
                    .setSizeBytes(bytes.size()).setSha256(DocumentPartCodec.sha256Hex(bytes.toByteArray()))
                    .setContentType("application/protobuf")));
        }
        var command = command(member.build()); var owner = owner(tx, command);
        var uploaded = upload(tx, provider, command, owner, source.placement(), fragments);
        // Policy explicitly allows this source's opaque mode; this is not a typed success claim.
        var schemas = DocumentSchemaBatch.prepare(command, active, Map.of(), () -> {});
        var content = DocumentCommandContent.check(command, "a", fragments, false, LIMITS, () -> {});
        new DocumentPublicationCommit(tx, new DriveLedger(tx), false, false).commit(ADMIN, owner, uploaded.prepared(),
                Map.of("a", content), uploaded.selected(), schemas, () -> {});
        var current = new DocumentLedger(tx).findByNodeId(source.node()).orElseThrow();
        var publication = new DocumentPublicationLedger(tx).findForRead(current).orElseThrow();
        var core = publication.boundParts().stream().filter(part -> part.part().part() == DocumentPart.DOCUMENT_PART_CORE)
                .findFirst().orElseThrow();
        require(!publication.revisionId().equals(source.revision())
                && current.mutationRevision > retainedCore.getReuse().getSource().getExpectedMutationRevision(),
                "source revision advanced through native commit");
        require(!core.part().sha256().equals(retainedCore.getReuse().getObject().getSha256())
                && !core.part().key().equals(retainedCore.getReuse().getObject().getObjectKey()),
                "current source has new physical CORE content");
        System.out.println("ASSESSMENT_SOURCE_ADVANCED_OK");
    }

    static Uploads upload(Tx tx, AssessmentProviderProbe provider, DocumentPublicationCommand command,
            RepositoryOperationLedger.Owner owner, DocumentUploadPlan.Placement placement, Map<Integer,ByteString> fragments) {
        var prepared = DocumentOperationUploadAdmission.prepare(command, Map.of(placement.drive().id(), placement),
                Map.of("a", UUID.randomUUID()), Duration.ofMinutes(5));
        var admitted = new DocumentOperationUploadAdmission(tx, new DriveLedger(tx)).admit(ADMIN, owner, prepared);
        var attempt = admitted.getFirst();
        var selected = new DocumentSelectedAttemptLedger.Selected("a", 1, attempt.id(), attempt.token());
        var measured = prepared.plan().members().getFirst().attempt().orElseThrow().uploads().stream().map(upload -> {
            var object = upload.object();
            var observed = DocumentPartTransfer.upload(provider.store(), "namespace", object,
                    fragments.get(upload.revisionOrdinal()).toByteArray(), Map.of(), () -> {}, () -> {});
            return new DocumentSelectedAttemptLedger.Observation(object.objectKey(), object.size(), object.sha256(),
                    object.contentType(), observed.version(), observed.etag());
        }).toList();
        new DocumentSelectedAttemptLedger(tx).verifyBatch(owner, selected, measured);
        return new Uploads(prepared, Map.of("a", selected));
    }
    static DocumentPublicationCommand command(DocumentPublicationMember member) {
        return new DocumentPublicationCommand(DocumentPublicationIntent.newBuilder().setEncodingVersion(1).setAccountId("account")
                .setOperationId(UUID.randomUUID().toString()).addMembers(member).build());
    }
    static RepositoryOperationLedger.Owner owner(Tx tx, DocumentPublicationCommand command) {
        return new RepositoryOperationLedger(tx).admit(new RepositoryOperationLedger.Key("account", "principal", command.operationId()),
                command, UUID.randomUUID(), Duration.ofMinutes(5)).owner().orElseThrow();
    }
    private static void require(boolean condition, String message) { if (!condition) throw new AssertionError(message); }
}
