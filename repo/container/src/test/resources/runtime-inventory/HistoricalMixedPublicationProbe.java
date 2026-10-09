package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.ByteString;
import java.time.*;
import java.util.*;

/** Real provider uploads and retained reads in one historical/current/fresh atomic command. */
public final class HistoricalMixedPublicationProbe {
    static void run(Tx tx, AssessmentProviderProbe provider, AssessmentMixedReuseProbe.Source source,
            DocumentPublishedRevision original, DocumentSchemaAdmission.Definition container,
            DocumentSchemaAdmission.Definition payload) throws Exception {
        for (boolean sameMember : new boolean[]{false, true})
            for (boolean verified : new boolean[]{false, true})
                run(tx, provider, source, original, container, payload, verified, sameMember);
    }

    private static void run(Tx tx, AssessmentProviderProbe provider, AssessmentMixedReuseProbe.Source source,
            DocumentPublishedRevision original, DocumentSchemaAdmission.Definition container,
            DocumentSchemaAdmission.Definition payload, boolean verified, boolean sameMember) throws Exception {
        var caller = new RepositoryCaller("principal", true);
        var reads = new DocumentReadLedger(tx, UUID.randomUUID());
        var history = reads.captureHistorical(caller, original.getAddress(), UUID.fromString(original.getRevisionId()));
        var budget = new PayloadBudget(128_000_000);
        try {
            var current = new DocumentLedger(tx).findByNodeId(source.node()).orElseThrow();
            var historical = source.candidate().toBuilder().clearParts();
            var fresh = source.candidate().toBuilder().clearParts();
            var reuse = source.candidate().toBuilder().clearParts();
            var fragments = new HashMap<Integer, ByteString>();
            var retainedManifest = new HashMap<Integer, PartManifestEntry>();
            var producer = WriteProvenance.newBuilder().setNodeId("mixed-fresh-producer").build();
            try (var use = history.use()) {
                for (var entry : use.plan().entries()) {
                    retainedManifest.put(entry.revisionOrdinal(), use.plan().manifest().getParts(entry.revisionOrdinal()));
                    var part = entry.part().part(); var binding = entry.part().binding();
                    var slot = DocumentPublicationSlot.newBuilder().setPart(part.part()).setSubKey(part.subKey()).build();
                    var object = PublicationObjectIdentity.newBuilder().setObjectId(entry.objectId().toString())
                            .setBackendGeneration(binding.generation()).setStorageRealm(binding.profile().storageRealm())
                            .setNamespace(binding.namespace()).setObjectKey(part.key()).setProviderVersion(part.providerVersion())
                            .setSizeBytes(part.size()).setSha256(part.sha256()).setContentType(part.contentType());
                    fragments.put(historical.getPartsCount(), ByteString.copyFrom(provider.store().getBounded(
                            binding.namespace(), part.key(), part.providerVersion(), Math.toIntExact(part.size())).data()));
                    historical.addParts(DocumentPublicationPart.newBuilder().setSlot(slot).setHistoricalReuse(
                            PublicationHistoricalReuse.newBuilder().setSource(original.getAddress()).setRevisionId(original.getRevisionId())
                                    .setRevisionOrdinal(entry.revisionOrdinal()).setSourceSlot(slot).setObject(object)));
                    reuse.addParts(DocumentPublicationPart.newBuilder().setSlot(slot).setReuse(PublicationReuse.newBuilder()
                            .setSource(DocumentRevisionCondition.newBuilder().setAddress(original.getAddress())
                                    .setExpectedMutationRevision(current.mutationRevision)).setSourceSlot(slot).setObject(object)));
                    fresh.addParts(DocumentPublicationPart.newBuilder().setSlot(slot).setUpload(PublicationUpload.newBuilder()
                            .setSizeBytes(part.size()).setSha256(part.sha256()).setContentType(part.contentType()).setWrittenBy(producer)));
                }
            }
            String prefix = "mixed-history-" + UUID.randomUUID();
            int parsedOrdinal = historical.getPartsCount();
            var newShape = com.google.protobuf.Any.pack(com.google.protobuf.StringValue.of("fresh parsed value"), "type.test");
            var intent = DocumentPublicationIntent.newBuilder().setEncodingVersion(1).setAccountId("account")
                    .setOperationId(UUID.randomUUID().toString());
            if (sameMember) {
                var parsed = Document.newBuilder().setDocId(Document.parseFrom(fragments.get(0)).getDocId())
                        .putParserResults("parsed", ParserResult.newBuilder().setDocument(ParserDocument.newBuilder().setShape(newShape)).build())
                        .build().toByteString();
                fragments.put(parsedOrdinal, parsed);
                historical.addParts(DocumentPublicationPart.newBuilder().setSlot(DocumentPublicationSlot.newBuilder()
                        .setPart(DocumentPart.DOCUMENT_PART_PARSED)).setUpload(PublicationUpload.newBuilder()
                        .setSizeBytes(parsed.size()).setSha256(ai.protomolt.proto.repo.codec.DocumentPartCodec.sha256Hex(parsed.toByteArray()))
                        .setContentType("application/protobuf").setWrittenBy(producer)));
                intent.addMembers(destination(historical, "fresh", prefix));
            } else intent.addMembers(destination(historical, "historical", prefix))
                    .addMembers(destination(reuse, "current", prefix)).addMembers(destination(fresh, "fresh", prefix));
            var command = new DocumentPublicationCommand(intent.build());
            var policy = new DocumentSchemaPolicies(tx).read("account", () -> {});
            var resolved = new ArrayList<String>();
            try (var assessment = DocumentPublicationAssessment.prepareHistorical(command, policy,
                    sameMember ? Map.of("fresh", DocumentPublicationCandidate.Mode.TYPED) : Map.of("historical", DocumentPublicationCandidate.Mode.TYPED, "current", DocumentPublicationCandidate.Mode.TYPED,
                            "fresh", DocumentPublicationCandidate.Mode.TYPED),
                    sameMember ? Map.of("fresh", fragments) : Map.of("historical", fragments, "current", fragments, "fresh", fragments), Optional.of(container),
                    (member, occurrence) -> {
                        if (sameMember) require(occurrence.ordinal() == parsedOrdinal, "retained root never resolves current definitions");
                        resolved.add(member.getMemberId()); return payload;
                    }, budget,
                    new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000), Instant.now(), caller,
                    List.of(history), RepositoryReadControl.NONE)) {
                require(resolved.equals(sameMember ? List.of("fresh") : List.of("current", "fresh")), "only fresh occurrences resolve current schema definitions");
                var prepared = assessment.preparePhysical(Map.of(source.placement().drive().id(), source.placement()),
                        Map.of("fresh", UUID.randomUUID()), Duration.ofMinutes(5), Map.of("fresh", UUID.randomUUID()), RepositoryReadControl.NONE);
                var owner = new RepositoryOperationLedger(tx).admitHistorical(caller,
                        new RepositoryOperationLedger.Key("account", "principal", command.operationId()), prepared,
                        UUID.randomUUID(), Duration.ofMinutes(5)).owner().orElseThrow();
                var attempts = new DocumentOperationUploadAdmission(tx, new DriveLedger(tx)).admit(caller, owner, prepared);
                require(attempts.size() == 1, "only fresh member needs an upload attempt");
                var attempt = attempts.getFirst();
                var selected = new DocumentSelectedAttemptLedger.Selected("fresh", 1, attempt.id(), attempt.token());
                var freshPlan = prepared.members().stream().filter(member -> member.intent().getMemberId().equals("fresh")).findFirst().orElseThrow();
                var observations = freshPlan.attempt().orElseThrow().uploads().stream().map(upload -> {
                    var object = upload.object();
                    var stored = DocumentPartTransfer.upload(provider.store(), source.placement().drive().namespace(), object,
                            fragments.get(upload.revisionOrdinal()).toByteArray(), Map.of(), () -> {}, () -> {});
                    return new DocumentSelectedAttemptLedger.Observation(object.objectKey(), object.size(), object.sha256(),
                            object.contentType(), stored.version(), stored.etag());
                }).toList();
                if (verified) new DocumentSelectedAttemptLedger(tx).verifyBatch(owner, selected, observations);
                history.close(); require(!history.isDrained(), "mixed operation retains source pin");
                DocumentPublicationResult result = null;
                try {
                    result = assessment.publish(caller, owner, prepared, Map.of("fresh", selected), new RepositorySchemaArtifacts(tx),
                            new DocumentPublicationCommit(tx, new DriveLedger(tx), true, false), RepositoryReadControl.NONE);
                    require(verified, "unverified upload must not publish any member");
                } catch (DocumentPartAttemptLedger.FenceException failure) {
                    if (verified) throw failure;
                }
                var replay = new DocumentPublicationReplay(tx).observe(caller, command);
                require(replay.state() == (verified ? DocumentPublicationReplay.State.COMMITTED : DocumentPublicationReplay.State.PENDING),
                        "whole operation has one expected outcome");
                if (verified) require(replay.result().orElseThrow().equals(result), "mixed success replays exactly");
                for (var member : command.intent().getMembersList()) {
                    var row = new DocumentLedger(tx).findByNodeId(DocumentIds.nodeId(member.getDestination().getAddress()));
                    require(row.isPresent() == verified, "no member becomes visible before complete physical verification");
                    if (!verified) continue;
                    require(row.orElseThrow().readManifest().getPartsCount() == member.getPartsCount(), "complete part set");
                    List<?> physical = tx.readOnly(em -> em.createNativeQuery("""
                            SELECT p.object_id, o.attempt_id, o.provider_version, p.revision_ordinal, o.content_type
                            FROM document_revision_current c
                            JOIN document_revision_parts p USING(revision_id)
                            JOIN document_part_attempt_objects o ON o.physical_object_id=p.object_id
                            WHERE c.node_id=:node ORDER BY p.revision_ordinal
                            """).setParameter("node", DocumentIds.nodeId(member.getDestination().getAddress())).getResultList());
                    require(physical.size() == member.getPartsCount(), "complete physical origin bindings");
                    for (int i = 0; i < member.getPartsCount(); i++) {
                        var declaration = member.getParts(i); var entry = row.orElseThrow().readManifest().getParts(i);
                        var origin = (Object[]) physical.get(i);
                        require(((Number) origin[3]).intValue() == i, "physical bindings retain canonical ordinal");
                        if (declaration.hasUpload()) {
                            var observed = observations.stream().filter(o -> o.key().equals(entry.getObjectKey())).findFirst().orElseThrow();
                            require(origin[1].toString().equals(selected.attempt().toString()), "fresh object belongs to selected attempt");
                            require(Objects.equals(origin[2], observed.version()), "fresh object retains observed provider version");
                            require(entry.getWrittenBy().equals(producer), "fresh upload records new producer");
                            require(entry.getObjectKey().equals(observed.key()), "fresh member binds selected provider object");
                        } else {
                            var object = declaration.hasReuse() ? declaration.getReuse().getObject() : declaration.getHistoricalReuse().getObject();
                            require(origin[0].toString().equals(object.getObjectId())
                                            && Objects.equals(origin[2], object.getProviderVersion()),
                                    "reuse retains exact physical UUID and provider version");
                            require(entry.getObjectKey().equals(object.getObjectKey()) && entry.getSha256().equals(object.getSha256()),
                                    "reuse keeps original provider identity and bytes");
                            require(entry.getSizeBytes() == object.getSizeBytes() && Objects.equals(origin[4], object.getContentType()),
                                    "reuse retains declared byte size and persisted content type");
                            if (declaration.hasHistoricalReuse()) require(entry.equals(retainedManifest.get(
                                    declaration.getHistoricalReuse().getRevisionOrdinal())),
                                    "historical manifest including producer provenance is preserved at the selected source ordinal");
                        }
                    }
                    if (sameMember) {
                        var reread = new HashMap<Integer, ByteString>();
                        for (int i = 0; i < row.orElseThrow().readManifest().getPartsCount(); i++) {
                            var part = row.orElseThrow().readManifest().getParts(i);
                            var origin = (Object[]) physical.get(i);
                            reread.put(i, ByteString.copyFrom(provider.store().getBounded(source.placement().drive().namespace(),
                                    part.getObjectKey(), (String) origin[2], Math.toIntExact(part.getSizeBytes())).data()));
                        }
                        require(reread.equals(fragments), "actual retained and fresh provider bytes match the admitted candidate");
                        var published = result.getMembers(0);
                        var checked = new DocumentHistoricalSchemas(tx).check(caller, published.getAddress(),
                                UUID.fromString(published.getRevisionId()), reread, () -> {});
                        require(checked.roots().size() == 2, "retained CORE and fresh PARSED roots both archived");
                        require(checked.document().getParserResultsOrThrow("parsed").getDocument().getShape().equals(newShape),
                                "fresh parsed shape decodes using only retained definitions");
                        require(checked.document().getStructuredData().equals(Document.parseFrom(fragments.get(0)).getStructuredData()),
                                "original structured shape survives mixed publication");
                    }
                }
                long admissions = tx.readOnly(em -> ((Number) em.createNativeQuery(
                        "SELECT count(*) FROM document_revision_schema_admissions WHERE operation_id=:operation")
                        .setParameter("operation", command.operationId()).getSingleResult()).longValue());
                require(admissions == (verified ? command.intent().getMembersCount() : 0), "all schema admissions publish or none do");
                require(new DocumentLedger(tx).findByNodeId(source.node()).orElseThrow().mutationRevision == current.mutationRevision,
                        "mixed publication does not rewrite source");
            }
            require(budget.reservedBytes() == 0, "mixed operation releases byte ownership");
        } finally {
            history.close(); require(history.awaitDrained(Duration.ofSeconds(1)), "mixed historical pin drains");
            history.release(); reads.fence(); reads.attestLocalQuiescence();
        }
        System.out.println(sameMember ? (verified ? "HISTORICAL_MIXED_MEMBER_PROVIDER_OK" : "HISTORICAL_MIXED_MEMBER_UNVERIFIED_REFUSED_OK")
                : (verified ? "HISTORICAL_MIXED_UPLOAD_OK" : "HISTORICAL_UNVERIFIED_UPLOAD_REFUSED_OK"));
    }

    private static DocumentPublicationMember destination(DocumentPublicationMember.Builder member, String id, String prefix) {
        return member.setMemberId(id).setDestination(DocumentRevisionCondition.newBuilder().setIfAbsent(true)
                .setAddress(member.getDestination().getAddress().toBuilder().setGraphAddressId(prefix + "-" + id))).build();
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
