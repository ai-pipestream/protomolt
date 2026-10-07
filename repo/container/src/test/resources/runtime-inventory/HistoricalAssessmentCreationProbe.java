package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.ByteString;
import java.nio.file.Path;
import java.time.*;
import java.util.*;

/** Real provider reads, production-JAR observation, and SQL CREATE of historical evidence. */
public final class HistoricalAssessmentCreationProbe {
    private enum Scenario {
        ORDINARY, ORDINARY_LOST_ACK, CLAIMED, MIXED, MIXED_CONTENTION, REVOKED_BEFORE_STAGE,
        STAGE_WINS, CREATE_WINS, ROLLBACK, LOST_ACK, START_ROLLBACK, START_LOST_ACK, START_CONCURRENT, SUCCESSOR
    }
    static void run(Tx tx, AssessmentProviderProbe provider, AssessmentMixedReuseProbe.Source source,
            DocumentPublishedRevision revision, javax.sql.DataSource database) throws Exception {
        for (var scenario : Scenario.values()) {
            if (scenario == Scenario.ROLLBACK || scenario == Scenario.LOST_ACK
                    || scenario == Scenario.START_ROLLBACK || scenario == Scenario.START_LOST_ACK) {
                try (var fault = new HistoricalCreateCommitFault(database, scenario == Scenario.LOST_ACK || scenario == Scenario.START_LOST_ACK)) {
                    run(fault.tx(), provider, source, revision, database, scenario, fault, null, tx);
                }
            } else if (scenario == Scenario.STAGE_WINS || scenario == Scenario.CREATE_WINS) {
                var phase = scenario == Scenario.STAGE_WINS ? HistoricalAuthorizationCommitGate.Phase.SCHEMA_STAGE
                        : HistoricalAuthorizationCommitGate.Phase.ASSESSMENT_CREATE;
                try (var gate = new HistoricalAuthorizationCommitGate(database, phase)) {
                    run(gate.tx(), provider, source, revision, database, scenario, null, gate, tx);
                }
            } else {
                run(tx, provider, source, revision, database, scenario, null, null, tx);
            }
        }
    }

    private static void run(Tx tx, AssessmentProviderProbe provider, AssessmentMixedReuseProbe.Source source,
            DocumentPublishedRevision revision, javax.sql.DataSource database, Scenario scenario,
            HistoricalCreateCommitFault fault, HistoricalAuthorizationCommitGate gate, Tx independent) throws Exception {
        boolean lostAck = scenario == Scenario.ORDINARY_LOST_ACK;
        boolean claimed = scenario != Scenario.ORDINARY && !lostAck;
        boolean mixed = scenario == Scenario.MIXED || scenario == Scenario.MIXED_CONTENTION;
        boolean scoped = scenario == Scenario.REVOKED_BEFORE_STAGE || gate != null;
        var credential = new RepositoryCredentialBinding("historical-create", UUID.randomUUID(), 1);
        var caller = scoped ? new RepositoryCaller("scoped-create", false, java.util.Set.of("account"), java.util.Set.of(), Optional.of(credential))
                : new RepositoryCaller("principal", true);
        if (scoped) new RepositoryCredentialAuthorities(tx).register(new RepositoryCaller("operator", true), credential, caller.principalName());
        var ledger = new DocumentReadLedger(tx, UUID.randomUUID());
        var history = ledger.captureHistorical(caller, revision.getAddress(), UUID.fromString(revision.getRevisionId()));
        var budget = new PayloadBudget(128_000_000);
        try {
            var current = new DocumentLedger(tx).findByNodeId(source.node()).orElseThrow();
            var member = source.candidate().toBuilder().clearParts().setDestination(DocumentRevisionCondition.newBuilder()
                    .setAddress(revision.getAddress()).setExpectedMutationRevision(current.mutationRevision));
            var fragments = new HashMap<Integer, ByteString>();
            try (var use = history.use()) {
                for (var entry : use.plan().entries()) {
                    var bound = entry.part(); var part = bound.part(); var binding = bound.binding();
                    var slot = DocumentPublicationSlot.newBuilder().setPart(part.part()).setSubKey(part.subKey()).build();
                    var object = PublicationObjectIdentity.newBuilder().setObjectId(entry.objectId().toString())
                            .setBackendGeneration(binding.generation()).setStorageRealm(binding.profile().storageRealm())
                            .setNamespace(binding.namespace()).setObjectKey(part.key()).setProviderVersion(part.providerVersion())
                            .setSizeBytes(part.size()).setSha256(part.sha256()).setContentType(part.contentType());
                    fragments.put(member.getPartsCount(), ByteString.copyFrom(provider.store().getBounded(
                            binding.namespace(), part.key(), part.providerVersion(), Math.toIntExact(part.size())).data()));
                    member.addParts(DocumentPublicationPart.newBuilder().setSlot(slot).setHistoricalReuse(
                            PublicationHistoricalReuse.newBuilder().setSource(revision.getAddress()).setRevisionId(revision.getRevisionId())
                                    .setRevisionOrdinal(entry.revisionOrdinal()).setSourceSlot(slot).setObject(object)));
                }
            }
            if (mixed) {
                var parsed = Document.newBuilder().setDocId(revision.getAddress().getDocId()).putParserResults("fresh",
                        ParserResult.newBuilder().setDocument(ParserDocument.newBuilder().setShape(com.google.protobuf.Any.pack(
                                com.google.protobuf.StringValue.of("fresh parsed payload"), "type.test"))).build()).build().toByteString();
                fragments.put(member.getPartsCount(), parsed);
                member.addParts(DocumentPublicationPart.newBuilder().setSlot(DocumentPublicationSlot.newBuilder()
                        .setPart(DocumentPart.DOCUMENT_PART_PARSED)).setUpload(PublicationUpload.newBuilder()
                        .setSizeBytes(parsed.size()).setSha256(ai.protomolt.proto.repo.codec.DocumentPartCodec.sha256Hex(parsed.toByteArray()))
                        .setContentType("application/protobuf")));
            }
            var command = AssessmentMixedReuseProbe.command(member.build());
            var policy = new DocumentSchemaPolicies(tx).read("account", () -> {});
            var observation = DocumentAssessmentRuntimeObserver.observe(Path.of(System.getenv("PROTOMOLT_TEST_RUNTIME_BUNDLE")), () -> {});
            if (claimed) {
                if (gate != null) {
                    HistoricalCreateWinnerProbe.run(tx, independent, gate, scenario == Scenario.CREATE_WINS, caller,
                            command, policy, source.placement(), history, fragments, budget, observation);
                } else if (scoped) {
                    HistoricalCreateAuthorizationProbe.run(tx, database, caller, command, policy, source.placement(),
                            history, fragments, budget, observation);
                } else {
                    claimed(tx, provider, caller, command, policy, source.placement(), history, fragments, budget, observation,
                            fault, mixed, scenario == Scenario.MIXED_CONTENTION ? database : null, scenario);
                    System.out.println(scenario == Scenario.MIXED_CONTENTION ? "CLAIMED_HISTORICAL_MIXED_ORIGIN_CONTENTION_OK"
                            : scenario == Scenario.SUCCESSOR ? "CLAIMED_HISTORICAL_SUCCESSOR_CREATE_OK"
                            : scenario == Scenario.START_CONCURRENT ? "CLAIMED_HISTORICAL_START_CONCURRENT_CREATE_OK"
                            : scenario == Scenario.START_ROLLBACK ? "CLAIMED_HISTORICAL_START_ROLLBACK_CREATE_OK"
                            : scenario == Scenario.START_LOST_ACK ? "CLAIMED_HISTORICAL_START_LOST_ACK_REFUSED_OK"
                            : fault == null ? (mixed ? "CLAIMED_HISTORICAL_ASSESSMENT_MIXED_OK" : "CLAIMED_HISTORICAL_ASSESSMENT_CREATE_OK")
                            : fault.lostAcknowledgement() ? "CLAIMED_HISTORICAL_ASSESSMENT_LOST_ACK_OK" : "CLAIMED_HISTORICAL_ASSESSMENT_ROLLBACK_OK");
                }
                require(budget.reservedBytes() == 0, "claimed historical CREATE releases byte ownership");
                return;
            }
            DocumentOperationUploadAdmission.Prepared borrowedPlan;
            try (var assessment = DocumentPublicationAssessment.prepareHistorical(command, policy,
                    Map.of("a", DocumentPublicationCandidate.Mode.TYPED), Map.of("a", fragments), Optional.empty(),
                    (selected, occurrence) -> { throw new AssertionError("Historical CREATE must not use the registry"); }, budget,
                    new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000), Instant.now(), caller,
                    List.of(history), RepositoryReadControl.NONE)) {
                var prepared = assessment.preparePhysical(Map.of(source.placement().drive().id(), source.placement()),
                        Map.of(), Duration.ofMinutes(5), Map.of(), RepositoryReadControl.NONE);
                borrowedPlan = prepared;
                var owner = new RepositoryOperationLedger(tx).admitHistorical(caller,
                        new RepositoryOperationLedger.Key("account", "principal", command.operationId()), prepared,
                        UUID.randomUUID(), Duration.ofMinutes(5)).owner().orElseThrow();
                new DocumentOperationUploadAdmission(tx, new DriveLedger(tx)).admit(caller, owner, prepared);
                history.close(); require(!history.isDrained(), "assessment retains historical source");
                var id = UUID.randomUUID(); var deadline = Instant.now().plusSeconds(120).truncatedTo(java.time.temporal.ChronoUnit.MICROS);
                final DocumentAssessmentCreation.Created created;
                if (lostAck) {
                    AssessmentStageFaultProbe.loseAcknowledgement(database, faultTx -> {
                        try {
                            assessment.create(caller, owner, prepared, Map.of(), observation, new RepositorySchemaArtifacts(tx),
                                    new DocumentAssessmentCreation(faultTx, new DriveLedger(faultTx)), id, deadline, RepositoryReadControl.NONE);
                        } catch (com.google.protobuf.InvalidProtocolBufferException invalid) { throw new IllegalStateException(invalid); }
                    });
                    var discovered = new DocumentAssessmentDiscovery(tx).discover(caller, owner, command, () -> {}).orElseThrow();
                    require(discovered.stage().assessment().equals(id) && discovered.stage().retainUntil().equals(deadline),
                            "lost acknowledgement discovers original identity and deadline");
                    require(discovered.selections().isEmpty(), "historical-only assessment has no upload selections");
                    created = new DocumentAssessmentReconciliation(tx).observeRetained(caller, owner, command, discovered.selections(),
                            id, discovered.stage().manifestSha256(), deadline, budget, () -> {}).orElseThrow();
                    require(created.equals(discovered.stage()), "historical retained roots and slot snapshot reconcile exactly");
                } else created = assessment.create(caller, owner, prepared, Map.of(), observation,
                            new RepositorySchemaArtifacts(tx), new DocumentAssessmentCreation(tx, new DriveLedger(tx)),
                            id, deadline, RepositoryReadControl.NONE);
                require(created.assessment().equals(id), "exact historical assessment identity");
                var slots = tx.readOnly(em -> em.createNativeQuery("""
                        SELECT declaration,source_revision,source_node FROM document_assessment_slots WHERE assessment_id=:id
                        """).setParameter("id", id).getResultList());
                require(slots.size() == member.getPartsCount(), "all historical slots retained");
                for (var value : slots) {
                    var row = (Object[]) value;
                    require(row[0].equals("HISTORICAL_REUSE") && row[1].equals(UUID.fromString(revision.getRevisionId()))
                            && row[2].equals(source.node()), "exact historical source provenance");
                }
                long publications = tx.readOnly(em -> ((Number) em.createNativeQuery(
                        "SELECT count(*) FROM document_revision_commits WHERE operation_id=:op")
                        .setParameter("op", command.operationId()).getSingleResult()).longValue());
                require(publications == 0, "CREATE is not publication");
                require(new DocumentLedger(tx).findByNodeId(source.node()).orElseThrow().mutationRevision == current.mutationRevision,
                        "historical CREATE does not advance destination");
            }
            require(budget.reservedBytes() == 0, "historical CREATE releases byte ownership");
            try {
                new RepositoryOperationLedger(tx).admitHistorical(caller,
                        new RepositoryOperationLedger.Key("account", "principal", command.operationId()), borrowedPlan,
                        UUID.randomUUID(), Duration.ofMinutes(5));
                throw new AssertionError("Expired borrowed source plan was admitted");
            } catch (IllegalStateException expected) {
                require(expected.getMessage().equals("Read plan use has ended"), "expired plan fails on its closed source Use");
            }
        } finally {
            history.close(); require(history.awaitDrained(Duration.ofSeconds(1)), "historical source drains"); history.release();
            ledger.fence(); ledger.attestLocalQuiescence();
        }
        System.out.println(lostAck ? "HISTORICAL_ASSESSMENT_LOST_ACK_OK" : "HISTORICAL_ASSESSMENT_CREATE_OK");
    }
    private static void claimed(Tx tx, AssessmentProviderProbe provider, RepositoryCaller caller, DocumentPublicationCommand command,
            DocumentSchemaPolicies.Selection policy, DocumentUploadPlan.Placement placement,
            DocumentReadLedger.PinnedHistory history, Map<Integer, ByteString> fragments, PayloadBudget budget,
            DocumentAssessmentRuntimeObserver.Observation observation, HistoricalCreateCommitFault fault, boolean mixed,
            javax.sql.DataSource contentionDatabase, Scenario scenario) throws Exception {
        boolean startFault = scenario == Scenario.START_ROLLBACK || scenario == Scenario.START_LOST_ACK;
        boolean createFault = fault != null && !startFault;
        var key = new RepositoryOperationLedger.Key("account", caller.principalName(), command.operationId());
        var record = new DocumentPublicationPreparationRecord(key, command, DocumentPublicationSeeds.mint(key, command),
                Map.of(placement.drive().id(), placement), scenario == Scenario.SUCCESSOR ? Duration.ofSeconds(10) : Duration.ofMinutes(5), 0);
        var scopes = new DocumentPublicationScopeCalls();
        try (var sources = DocumentHistoricalAssessmentSources.open(command, caller, List.of(history), RepositoryReadControl.NONE)) {
            var registration = DocumentPublicationRegistration.historical(tx, budget, record, sources, UUID.randomUUID(),
                    scopes, new DriveLedger(tx), RepositoryReadControl.NONE);
            var modes = Map.of("a", DocumentPublicationCandidate.Mode.TYPED);
            var owner = registration.admitInitial(caller, modes, RepositoryReadControl.NONE).orElseThrow();
            try (var execution = registration.historicalExecution(caller, owner, modes, RepositoryReadControl.NONE)) {
                var admitted = execution.admitUploads(caller, RepositoryReadControl.NONE);
                if (scenario == Scenario.START_CONCURRENT) {
                    HistoricalConcurrentStartProbe.run(tx, registration, execution, caller, owner, modes, command,
                            policy, fragments, observation);
                    return;
                }
                if (startFault) {
                    fault.armStart(key);
                    try {
                        execution.start(caller, Duration.ofMinutes(2), RepositoryReadControl.NONE);
                        throw new AssertionError("Targeted START commit fault returned success");
                    } catch (RuntimeException failure) { fault.requireFailure(failure); }
                    long starts = tx.readOnly(em -> ((Number) em.createNativeQuery(
                            "SELECT count(*) FROM repository_publication_assessment_starts WHERE operation_id=:op")
                            .setParameter("op", command.operationId()).getSingleResult()).longValue());
                    require(starts == (fault.lostAcknowledgement() ? 1 : 0), "START fault has exact committed row count");
                }
                var started = execution.start(caller, Duration.ofMinutes(2), RepositoryReadControl.NONE);
                require(execution.start(caller, Duration.ofMinutes(2), RepositoryReadControl.NONE).equals(started),
                        "repeated start preserves coordinates and acknowledged permission");
                if (startFault) require(started.assessment().equals(fault.proposedStart()) == fault.lostAcknowledgement(),
                        "lost START acknowledgement recovers identity; rolled back START permits a new identity");
                if (scenario == Scenario.SUCCESSOR) {
                    HistoricalSuccessorCreateProbe.run(tx, provider, caller, record, owner, execution, started,
                            policy, fragments, budget, observation, registration.drainIdentity());
                    return;
                }
                var freshDefinition = ObservedAssessmentProbe.asset(com.google.protobuf.StringValue.getDescriptor());
                try (var assessment = execution.prepareAssessment(caller, policy, Map.of("a", fragments),
                        mixed ? Optional.of(ObservedAssessmentProbe.asset(Document.getDescriptor())) : Optional.empty(),
                        (selected, occurrence) -> { if (!mixed) throw new AssertionError("Claimed historical CREATE must use retained schemas"); return freshDefinition; },
                        new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000), Instant.now(), RepositoryReadControl.NONE)) {
                    Map<String, DocumentSelectedAttemptLedger.Selected> selections = Map.of();
                    if (mixed) {
                        require(admitted.attempts().size() == 1, "mixed CREATE admits one fresh attempt");
                        var attempt = admitted.attempts().getFirst();
                        var selected = new DocumentSelectedAttemptLedger.Selected("a", 1, attempt.id(), attempt.token());
                        var physical = assessment.preparePhysical(record.placements(), record.seeds().attempts(), record.lease(),
                                record.seeds().uploadTokens(), RepositoryReadControl.NONE);
                        var measured = physical.plan().members().getFirst().attempt().orElseThrow().uploads().stream().map(upload -> {
                            var object = upload.object();
                            var actual = DocumentPartTransfer.upload(provider.store(), placement.drive().namespace(), object,
                                    fragments.get(upload.revisionOrdinal()).toByteArray(), Map.of(), () -> {}, () -> {});
                            require(Arrays.equals(provider.store().getBounded(placement.drive().namespace(), object.objectKey(), actual.version(),
                                    Math.toIntExact(object.size())).data(), fragments.get(upload.revisionOrdinal()).toByteArray()),
                                    "fresh provider version contains exact supplied bytes");
                            return new DocumentSelectedAttemptLedger.Observation(object.objectKey(), object.size(), object.sha256(),
                                    object.contentType(), actual.version(), actual.etag());
                        }).toList();
                        new DocumentSelectedAttemptLedger(tx).verifyBatch(owner, selected, measured);
                        selections = Map.of("a", selected);
                    }
                    if (!createFault) {
                        try (var foreign = registration.historicalExecution(caller, owner, modes, RepositoryReadControl.NONE)) {
                            try {
                                foreign.createAssessment(caller, assessment, selections, observation, new RepositorySchemaArtifacts(tx),
                                        started, RepositoryReadControl.NONE);
                                throw new AssertionError("Foreign execution accepted assessment");
                            } catch (IllegalArgumentException expected) {
                                require(expected.getMessage().equals("Assessment belongs to another historical execution"), "exact handle identity refused");
                            }
                            require(foreign.start(caller, Duration.ofMinutes(2), RepositoryReadControl.NONE).equals(started),
                                    "other handle recovers the same durable start coordinates");
                            try (var ownAssessment = foreign.prepareAssessment(caller, policy, Map.of("a", fragments),
                                    mixed ? Optional.of(ObservedAssessmentProbe.asset(Document.getDescriptor())) : Optional.empty(),
                                    (member, occurrence) -> { if (!mixed) throw new AssertionError("Historical schemas are retained"); return freshDefinition; },
                                    new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000), Instant.now(), RepositoryReadControl.NONE)) {
                                try {
                                    foreign.createAssessment(caller, ownAssessment, selections, observation, new RepositorySchemaArtifacts(tx),
                                            started, RepositoryReadControl.NONE);
                                    throw new AssertionError("Loaded start granted CREATE authority to another handle");
                                } catch (RepositoryException expected) {
                                    require(expected.code() == RepositoryException.Code.FAILED_PRECONDITION
                                                    && expected.getMessage().contains("requires reconciliation"),
                                            "loading start coordinates confers reconciliation-only authority");
                                }
                            }
                        }
                        long claims = tx.readOnly(em -> ((Number) em.createNativeQuery(
                                "SELECT count(*) FROM repository_schema_artifact_claims WHERE operation_id=:op")
                                .setParameter("op", command.operationId()).getSingleResult()).longValue());
                        require(claims == 0, "foreign handle staged no schema claims");
                    }
                    if (scenario == Scenario.START_LOST_ACK) {
                        try {
                            execution.createAssessment(caller, assessment, selections, observation,
                                    new RepositorySchemaArtifacts(tx), started, RepositoryReadControl.NONE);
                            throw new AssertionError("Lost START acknowledgement granted CREATE permission");
                        } catch (RepositoryException expected) {
                            require(expected.code() == RepositoryException.Code.FAILED_PRECONDITION
                                    && expected.getMessage().contains("requires reconciliation"),
                                    "original handle without acknowledged START cannot CREATE");
                        }
                        long claims = tx.readOnly(em -> ((Number) em.createNativeQuery(
                                "SELECT count(*) FROM repository_schema_artifact_claims WHERE operation_id=:op")
                                .setParameter("op", command.operationId()).getSingleResult()).longValue());
                        require(claims == 0, "lost START acknowledgement stages no schema claims on either handle");
                        require(new DocumentAssessmentDiscovery(tx).discover(caller, owner, command, () -> {}).isEmpty(),
                                "START coordinates alone do not establish an assessment");
                        return;
                    }
                    var authorityBefore = authority(tx, owner.key());
                    DocumentAssessmentCreation.Created created = null;
                    if (contentionDatabase != null) {
                        created = HistoricalMixedOriginContentionProbe.create(tx, contentionDatabase, execution, caller,
                                command, assessment, selections, observation, started);
                    } else if (!createFault) {
                        created = execution.createAssessment(caller, assessment, selections, observation,
                                new RepositorySchemaArtifacts(tx), started, RepositoryReadControl.NONE);
                    } else {
                        fault.arm(started.assessment());
                        try {
                            execution.createAssessment(caller, assessment, selections, observation,
                                    new RepositorySchemaArtifacts(tx), started, RepositoryReadControl.NONE);
                            throw new AssertionError("Targeted commit fault returned success");
                        } catch (RuntimeException failure) { fault.requireFailure(failure); }
                    }
                    var discovered = new DocumentAssessmentDiscovery(tx).discover(caller, owner, command, () -> {});
                    if (createFault && !fault.lostAcknowledgement()) {
                        require(discovered.isEmpty(), "rollback has no discoverable assessment");
                        for (String table : List.of("document_assessment_owners", "document_assessment_objects", "document_assessment_slots",
                                "document_assessment_roots", "document_assessment_artifacts", "document_assessment_slot_snapshots")) {
                            long rows = tx.readOnly(em -> ((Number) em.createNativeQuery(
                                    "SELECT count(*) FROM " + table + " WHERE assessment_id=:id")
                                    .setParameter("id", started.assessment()).getSingleResult()).longValue());
                            require(rows == 0, "rollback leaves no rows in " + table);
                        }
                    } else {
                        require(discovered.orElseThrow().selections().equals(DocumentAssessmentRetainedSlots.uploadSelections(selections)),
                                "discovered selections match independently supplied selected attempts");
                        var expectedStage = discovered.orElseThrow().stage();
                        var retainedSlots = tx.readOnly(em -> em.createNativeQuery("""
                                SELECT revision_ordinal,declaration FROM document_assessment_slots
                                WHERE assessment_id=:id ORDER BY revision_ordinal
                                """).setParameter("id", expectedStage.assessment()).getResultList());
                        require(retainedSlots.size() == command.intent().getMembers(0).getPartsCount(), "all expected candidate slots retained");
                        for (Object value : retainedSlots) {
                            Object[] row = (Object[]) value;
                            int ordinal = ((Number) row[0]).intValue();
                            String expected = command.intent().getMembers(0).getParts(ordinal).hasUpload() ? "NEW_CONTENT" : "HISTORICAL_REUSE";
                            require(expected.equals(row[1]), "retained slot " + ordinal + " declaration: expected "
                                    + expected + ", found " + row[1]);
                        }
                        var stage = discovered.orElseThrow().stage();
                        require(stage.assessment().equals(started.assessment()) && stage.retainUntil().equals(started.retainUntil()),
                                "claimed CREATE retains committed start coordinates");
                        if (created != null) require(stage.equals(created), "successful CREATE matches discovered identity");
                        var retained = new DocumentAssessmentReconciliation(tx).observeRetained(caller, owner, command,
                                discovered.orElseThrow().selections(), stage.assessment(), stage.manifestSha256(), stage.retainUntil(), budget, () -> {});
                        require(retained.orElseThrow().equals(stage), "claimed CREATE retained evidence reconciles");
                    }
                    try {
                        execution.createAssessment(caller, assessment, selections, observation, new RepositorySchemaArtifacts(tx),
                                started, RepositoryReadControl.NONE);
                        throw new AssertionError("Second claimed CREATE was allowed");
                    } catch (RepositoryException expected) {
                        require(expected.code() == RepositoryException.Code.FAILED_PRECONDITION
                                && expected.getMessage().contains("requires reconciliation"), "second CREATE requires explicit reconciliation");
                    }
                    require(authority(tx, owner.key()).equals(authorityBefore), "CREATE preserves exact owner and claim leases");
                    long published = tx.readOnly(em -> ((Number) em.createNativeQuery(
                            "SELECT count(*) FROM document_revision_commits WHERE operation_id=:op")
                            .setParameter("op", command.operationId()).getSingleResult()).longValue());
                    require(published == 0, "claimed CREATE does not publish");
                }
            }
        } finally { scopes.close(); }
    }

    static List<Object> authority(Tx tx, RepositoryOperationLedger.Key key) {
        return tx.readOnly(em -> Arrays.asList((Object[]) em.createNativeQuery("""
                SELECT o.owner_generation,o.owner_token,o.lease_until,c.claim_epoch,c.claim_token,c.lease_until
                FROM repository_operation_owners o JOIN repository_execution_claims c USING(account_id,principal,operation_id)
                WHERE o.account_id=:a AND o.principal=:p AND o.operation_id=:op
                """).setParameter("a", key.account()).setParameter("p", key.principal())
                .setParameter("op", key.operationId()).getSingleResult()));
    }

    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
