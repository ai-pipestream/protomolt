package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.*;
import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.engine.DocumentPartReader;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.StringValue;
import java.time.*;
import java.util.*;

/** Real provider bytes and retained SQL evidence, with no live assessment or registry during replay. */
public final class AssessmentOperationReplayProbe {
    private static final DocumentRevisionAssembly.Limits LIMITS = new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000);
    static DocumentSchemaPolicies.Selection run(Tx tx, AssessmentProviderProbe provider, DocumentSchemaPolicies.Selection policy,
            DocumentAssessmentRuntimeObserver.Observation observation) throws Exception {
        var valid = ObservedAssessmentProbe.asset(StringValue.getDescriptor());
        var invalid = ObservedAssessmentProbe.invalidSchema();
        var current = policy;
        for (int scenario = 0; scenario < 3; scenario++) {
            var caller = new RepositoryCaller("principal", true);
            var a = ObservedAssessmentProbe.member("a"); var b = ObservedAssessmentProbe.member("b");
            var intent = DocumentPublicationIntent.newBuilder().setEncodingVersion(1).setAccountId("account")
                    .setOperationId(UUID.randomUUID().toString());
            for (var member : List.of(b.member(), a.member())) intent.addMembers(member.toBuilder().setDestination(
                    member.getDestination().toBuilder().setAddress(member.getDestination().getAddress().toBuilder()
                            .setGraphId("replay-" + UUID.randomUUID()))));
            var command = new DocumentPublicationCommand(intent.build());
            var owner = new RepositoryOperationLedger(tx).admit(new RepositoryOperationLedger.Key("account", "principal", command.operationId()),
                    command, UUID.randomUUID(), Duration.ofMinutes(5)).owner().orElseThrow();
            new ManagedBackendLedger(tx).bind("assessment-s3", provider.profile());
            var placements = new HashMap<UUID,DocumentUploadPlan.Placement>();
            var attempts = new HashMap<String,UUID>();
            var fragments = Map.of("a", a.fragments(), "b", b.fragments());
            var drives = new DriveLedger(tx);
            for (var member : command.intent().getMembersList()) {
                var drive = new DriveRecord(); drive.driveId = UUID.fromString(member.getDriveId()); drive.accountId = "account";
                drive.name = "replay-" + drive.driveId; drive.bucket = "namespace"; drive.prefix = "root";
                drive.provider = "s3"; drive.driveType = "CUSTOM"; drive.status = "ACTIVE"; drives.insert(drive);
                placements.put(drive.driveId, DocumentUploadPlan.Placement.sample(drive, "assessment-s3", provider.profile()));
                attempts.put(member.getMemberId(), UUID.randomUUID());
            }
            var prepared = DocumentOperationUploadAdmission.prepare(command, placements, attempts, Duration.ofMinutes(5));
            var admitted = new DocumentOperationUploadAdmission(tx, drives).admit(caller, owner, prepared);
            var selected = new HashMap<String,DocumentSelectedAttemptLedger.Selected>();
            for (var member : prepared.plan().members()) {
                var attempt = admitted.stream().filter(value -> value.id().equals(member.attempt().orElseThrow().id())).findFirst().orElseThrow();
                var selection = new DocumentSelectedAttemptLedger.Selected(member.intent().getMemberId(), 1, attempt.id(), attempt.token());
                selected.put(selection.member(), selection);
                new DocumentSelectedAttemptLedger(tx).verifyBatch(owner, selection, member.attempt().orElseThrow().uploads().stream().map(upload -> {
                    var object = upload.object();
                    var measured = DocumentPartTransfer.upload(provider.store(), "namespace", object,
                            fragments.get(selection.member()).get(upload.revisionOrdinal()).toByteArray(), Map.of(), () -> {}, () -> {});
                    return new DocumentSelectedAttemptLedger.Observation(object.objectKey(), object.size(), object.sha256(),
                            object.contentType(), measured.version(), measured.etag());
                }).toList());
            }
            var budget = new PayloadBudget(128_000_000);
            String expectedFirst = scenario == 0 ? null : scenario == 1 ? "a" : "b";
            int mode = scenario;
            DocumentAssessmentCreation.Created stage;
            var recordedRuntime = observation.identity(() -> {});
            try (var assessment = DocumentPublicationAssessment.prepare(command, current,
                    Map.of("a", DocumentPublicationCandidate.Mode.TYPED, "b", scenario == 0
                            ? DocumentPublicationCandidate.Mode.OPAQUE : DocumentPublicationCandidate.Mode.TYPED),
                    fragments, Optional.of(ObservedAssessmentProbe.asset(Document.getDescriptor())),
                    (member, occurrence) -> mode == 1 || (mode == 2 && member.getMemberId().equals("b")) ? invalid : valid,
                    budget, LIMITS, Instant.now(), () -> {})) {
                require(Objects.equals(assessment.failure().map(failure -> failure.member()).orElse(null), expectedFirst),
                        "fixture has expected first failure");
                if (mode == 1) require(assessment.typed().values().stream().allMatch(value -> value.failure().isPresent()),
                        "both typed members actually fail");
                stage = assessment.withRetentionEvidence(owner, observation, () -> {}, evidence -> {
                    new RepositorySchemaArtifacts(tx).stage(owner, command, List.copyOf(evidence.artifacts(() -> {}).values()), () -> {});
                    return new DocumentAssessmentCreation(tx, drives).create(caller, owner, prepared, selected, evidence, UUID.randomUUID(),
                            Instant.now().plusSeconds(120).truncatedTo(java.time.temporal.ChronoUnit.MICROS), budget, () -> {});
                });
            }
            require(budget.reservedBytes() == 0, "original assessment closed");
            if (scenario == 0) {
                var stricter = DocumentAdmissionPolicy.of(current.policy().definition().toBuilder()
                        .setMode(DocumentSchemaPolicyMode.DOCUMENT_SCHEMA_POLICY_MODE_TYPED_REQUIRED).build(), () -> {});
                current = new DocumentSchemaPolicies(tx).activate(stricter, current.revision(), () -> {});
                require(current.revision() > policy.revision() && !current.policy().sha256().equals(policy.policy().sha256()),
                        "current policy advances to a different definition");
                var selectedPolicy = new DocumentSchemaPolicies(tx).read("account", () -> {});
                require(selectedPolicy.revision() == current.revision()
                        && selectedPolicy.policy().bytes().equals(current.policy().bytes()), "changed policy is active in SQL");
                try (var refused = DocumentPublicationAssessment.prepare(command, current,
                        Map.of("a", DocumentPublicationCandidate.Mode.TYPED, "b", DocumentPublicationCandidate.Mode.OPAQUE),
                        fragments, Optional.of(ObservedAssessmentProbe.asset(Document.getDescriptor())),
                        (member, occurrence) -> valid, budget, LIMITS, Instant.now(), () -> {})) {
                    throw new AssertionError("Current policy accepted historical opaque mode");
                } catch (IllegalArgumentException expected) {
                    require(expected.getMessage().equals("Policy requires typed admission for member"), "current policy rejects opaque mode");
                }
                try {
                    tx.inTransaction(em -> { DocumentSchemaPolicies.lockCurrent(em, policy, () -> {}); });
                    throw new AssertionError("Stale policy passed publication fence");
                } catch (DocumentSchemaPolicies.StalePolicy expected) {
                    require(expected.getMessage().equals("Prepared schema policy is no longer active"), "publication policy fence remains current");
                }
                require(budget.reservedBytes() == 0, "refused current assessment leaks no bytes");
            }
            var reads = new DocumentReadLedger(tx, UUID.randomUUID(), 1);
            var payload = new PayloadBudget(16_000_000);
            var visited = new ArrayList<String>();
            var reader = new DocumentPartReader((generation, profile) -> {
                require(generation.equals("assessment-s3") && profile.equals(provider.profile()), "exact provider binding");
                return provider.store();
            }, 2, 16_000_000, payload);
            try (reader; var capture = reads.captureAssessment(caller, owner, command,
                    DocumentAssessmentRetainedSlots.uploadSelections(selected), stage.assessment(), stage.manifestSha256(),
                    stage.retainUntil(), budget, () -> {})) {
                var result = DocumentAssessmentReplay.replay(capture, (captured, member, control) -> {
                    visited.add(member); return reader.readAssessment(captured, member, control);
                }, budget, LIMITS, observation, RepositoryReadControl.NONE);
                require(visited.equals(List.of("a", "b")), "all members replayed in canonical order even after failure");
                require(Objects.equals(result.firstFailure().map(DocumentAssessmentFailure::getMemberId).orElse(null), expectedFirst),
                        "exact first failure reproduced");
                require(result.assessment().equals(stage.assessment()) && result.manifestSha256().equals(stage.manifestSha256())
                        && result.commandSha256().equals(command.sha256()) && result.ownerGeneration() == owner.generation(), "replay identity bound");
                require(result.replayRuntime().equals(observation.identity(() -> {})), "current runtime observed separately");
                require(result.recordedRuntime().equals(recordedRuntime), "stored runtime matches original assessment observation");
                require(budget.reservedBytes() == 0 && payload.reservedBytes() == 0, "replay releases inputs and fragment copies");
            } finally {
                require(reader.awaitIdle(Duration.ofSeconds(5)), "provider workers drained");
                require(reads.releaseDrained(1) == 1 && reads.outstandingReads() == 0, "replay releases exact session");
            }
            long outcomes = tx.readOnly(em -> ((Number) em.createNativeQuery("""
                    SELECT (SELECT count(*) FROM repository_operation_success WHERE operation_id=:op)
                         + (SELECT count(*) FROM repository_operation_rejection WHERE operation_id=:op)
                    """).setParameter("op", command.operationId()).getSingleResult()).longValue());
            require(outcomes == 0, "replay grants no terminal decision");
        }
        System.out.println("ASSESSMENT_OPERATION_REPLAY_OK");
        System.out.println("ASSESSMENT_POLICY_ADVANCEMENT_REPLAY_OK");
        // Later scenarios deliberately exercise opaque admission. Restore its definition
        // through normal administration, preserving the monotonic revision history.
        var restored = new DocumentSchemaPolicies(tx).activate(policy.policy(), current.revision(), () -> {});
        require(restored.revision() > current.revision(), "policy restoration advances revision");
        return restored;
    }
    static void verifyMixed(Tx tx, AssessmentProviderProbe provider, RepositoryOperationLedger.Owner owner,
            DocumentPublicationCommand command, Map<String,DocumentAssessmentRetainedSlots.UploadSelection> selected,
            DocumentAssessmentCreation.Created stage, UUID source, DocumentAssessmentRuntimeObserver.Observation observation) throws Exception {
        var caller = new RepositoryCaller("principal", false, Set.of("account"), Set.of());
        for (int scenario = 0; scenario < 3; scenario++) {
            int mode = scenario;
            var reads = new DocumentReadLedger(tx, UUID.randomUUID(), 1);
            var budget = new PayloadBudget(128_000_000); var payload = new PayloadBudget(16_000_000);
            var invoked = new java.util.concurrent.atomic.AtomicBoolean();
            var reader = new DocumentPartReader((generation, profile) -> {
                require(generation.equals("assessment-s3") && profile.equals(provider.profile()), "exact mixed provider binding");
                return provider.store();
            }, 2, 16_000_000, payload);
            try (AutoCloseable restore = () -> policy(tx, source, "ACCESS_READ")) {
                try (reader; var capture = reads.captureAssessment(caller, owner, command, selected, stage.assessment(),
                        stage.manifestSha256(), stage.retainUntil(), budget, () -> {})) {
                    DocumentAssessmentReader delegated = (captured, member, control) -> {
                        var batch = reader.readAssessment(captured, member, control);
                        boolean returned = false;
                        try {
                            invoked.set(true);
                            if (mode != 0) policy(tx, source, "ACCESS_DENY");
                            if (mode == 2) throw new IllegalStateException("private-replay-provider-detail");
                            returned = true; return batch;
                        } finally { if (!returned) batch.close(); }
                    };
                    if (mode == 0) {
                        var result = DocumentAssessmentReplay.replay(capture, delegated, budget, LIMITS, observation, RepositoryReadControl.NONE);
                        require(result.firstFailure().isEmpty() && result.manifestSha256().equals(stage.manifestSha256()),
                                "mixed historical fragments revalidate without registry");
                    } else {
                        try {
                            DocumentAssessmentReplay.replay(capture, delegated, budget, LIMITS, observation, RepositoryReadControl.NONE);
                            throw new AssertionError("Revoked replay result or private error delivered");
                        } catch (RepositoryException expected) {
                            require(expected.code() == RepositoryException.Code.NOT_FOUND && expected.getCause() == null
                                    && expected.getSuppressed().length == 0, "revoked replay suppresses result and private error");
                        }
                    }
                    require(invoked.get(), "real reader supplied provider batch before fault");
                    require(budget.reservedBytes() == 0 && payload.reservedBytes() == 0, "mixed replay releases all byte reservations");
                } finally {
                    require(reader.awaitIdle(Duration.ofSeconds(5)), "mixed provider workers drained");
                    require(reads.releaseDrained(1) == 1 && reads.outstandingReads() == 0, "mixed replay releases session");
                }
            }
        }
        System.out.println("ASSESSMENT_MIXED_REPLAY_OK");
    }
    private static void policy(Tx tx, UUID source, String access) {
        tx.inTransaction(em -> {
            int changed = em.createNativeQuery("UPDATE documents SET security=CAST(:policy AS jsonb) WHERE node_id=:id")
                    .setParameter("policy", "{\"permissions\":[{\"identityType\":\"public\",\"identity\":\"public\",\"access\":\"" + access + "\"}]}")
                    .setParameter("id", source).executeUpdate();
            require(changed == 1, "source policy updated");
        });
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
