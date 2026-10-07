package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.spi.*;
import com.google.protobuf.ByteString;
import java.time.*;
import java.util.*;

/** Real expiry, fresh source capture and provider reads, followed by successor CREATE. */
final class HistoricalSuccessorCreateProbe {
    static void run(Tx tx, AssessmentProviderProbe provider, RepositoryCaller caller,
            DocumentPublicationPreparationRecord original, RepositoryOperationLedger.Owner oldOwner,
            DocumentHistoricalExecution oldExecution, DocumentAssessmentStartJournal.Started oldStart,
            DocumentSchemaPolicies.Selection policy, Map<Integer, ByteString> fragments, PayloadBudget budget,
            DocumentAssessmentRuntimeObserver.Observation observation, RepositoryCoordinatorDrain.Identity oldIdentity,
            boolean installedOwner, HistoricalCreateCommitFault fault, javax.sql.DataSource database,
            HistoricalInstalledOwnerProbe.Check check) throws Exception {
        var command = original.command();
        boolean mixed = command.intent().getMembers(0).getPartsList().stream().anyMatch(part -> part.hasUpload());
        Optional<ai.protomolt.proto.repo.admission.DocumentSchemaAdmission.Definition> container = mixed
                ? Optional.of(ObservedAssessmentProbe.asset(ai.protomolt.proto.repo.v1.Document.getDescriptor())) : Optional.empty();
        var freshDefinition = ObservedAssessmentProbe.asset(com.google.protobuf.StringValue.getDescriptor());
        DocumentPublicationCandidate.Resolver resolver = (member, occurrence) -> {
            require(mixed && member.getParts(occurrence.ordinal()).hasUpload(), "only resubmitted uploads resolve fresh schemas");
            return freshDefinition;
        };
        var limits = new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000);
        try (var oldAssessment = oldExecution.prepareAssessment(caller, policy, Map.of("a", fragments), container, resolver,
                limits, Instant.now(), RepositoryReadControl.NONE)) {
            tx.readOnly(em -> em.createNativeQuery("""
                    SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM (GREATEST(c.lease_until,o.lease_until)-clock_timestamp())))+0.05)
                    FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                    WHERE c.operation_id=:op
                    """).setParameter("op", command.operationId()).getSingleResult());
            var reservation = new RepositoryCoordinatorReservation.ExpiredUnquiesced(oldIdentity, UUID.randomUUID(),
                    UUID.randomUUID(), Duration.ofMinutes(2), new RepositoryCoordinatorReservation.OwnerIdentity(
                            oldOwner.generation(), oldOwner.token()));
            // The embedding host supplies recovery authority separately from the client's scoped credential.
            var coordinator = new RepositoryCaller(caller.principalName(), true);
            if (!caller.processAuthority()) {
                try {
                    RepositoryCoordinatorExpiration.reserve(tx, caller, reservation, RepositoryReadControl.NONE);
                    throw new AssertionError("Scoped client reserved coordinator recovery");
                } catch (RepositoryException refused) {
                    require(refused.code() == RepositoryException.Code.PERMISSION_DENIED
                            && refused.getMessage().contains("private process authority"), "recovery requires host authority");
                }
            }
            var modes = Map.of("a", DocumentPublicationCandidate.Mode.TYPED);
            try (var preparedOwner = installedOwner ? HistoricalInstalledOwnerProbe.prepare(tx, caller, coordinator,
                    original, fragments, budget, check) : null) {
            final RepositorySuccessorInstall.Plan plan;
            if (preparedOwner != null) {
                plan = preparedOwner.plan();
            } else {
                RepositoryCoordinatorExpiration.reserve(tx, coordinator, reservation, RepositoryReadControl.NONE);
                plan = RepositorySuccessorInstall.prepare(reservation, original, Duration.ofMinutes(2), modes);
                RepositorySuccessorInstall.install(tx, budget, coordinator, plan, RepositoryReadControl.NONE);
            }
            var selector = command.intent().getMembers(0).getPartsList().stream().filter(p -> p.hasHistoricalReuse())
                    .findFirst().orElseThrow().getHistoricalReuse();
            var reads = new DocumentReadLedger(tx, UUID.randomUUID());
            var history = reads.captureHistorical(caller, selector.getSource(), UUID.fromString(selector.getRevisionId()));
            var scopes = new DocumentPublicationScopeCalls();
            try (var sources = DocumentHistoricalAssessmentSources.open(command, caller, List.of(history), RepositoryReadControl.NONE);
                 var accepted = sources.work()) {
                // Independently reread the exact immutable provider versions selected by the new capture.
                var fresh = new HashMap<Integer, ByteString>();
                try (var use = history.use()) {
                    for (var entry : use.plan().entries()) {
                        var part = entry.part();
                        var bytes = ByteString.copyFrom(provider.store().getBounded(part.binding().namespace(),
                                part.part().key(), part.part().providerVersion(), Math.toIntExact(part.part().size())).data());
                        for (int ordinal = 0; ordinal < command.intent().getMembers(0).getPartsCount(); ordinal++) {
                            var declaration = command.intent().getMembers(0).getParts(ordinal);
                            if (!declaration.hasHistoricalReuse()) continue;
                            var selected = declaration.getHistoricalReuse();
                            if (selected.getObject().getObjectId().equals(entry.objectId().toString())) {
                                require(bytes.equals(fragments.get(ordinal)), "fresh capture rereads original bytes");
                                fresh.put(ordinal, bytes);
                            }
                        }
                    }
                }
                for (int ordinal = 0; ordinal < command.intent().getMembers(0).getPartsCount(); ordinal++) {
                    var declaration = command.intent().getMembers(0).getParts(ordinal);
                    if (!declaration.hasUpload()) continue;
                    // These bytes are explicitly supplied again by the caller, not recovered from the journal.
                    var supplied = Objects.requireNonNull(fragments.get(ordinal));
                    require(supplied.size() == declaration.getUpload().getSizeBytes()
                            && ai.protomolt.proto.repo.codec.DocumentPartCodec.sha256Hex(supplied.toByteArray())
                                    .equals(declaration.getUpload().getSha256()), "resubmitted payload matches its declaration");
                    fresh.put(ordinal, supplied);
                }
                require(fresh.size() == fragments.size(), "every fragment is reread or explicitly resubmitted");
                if (installedOwner) {
                    HistoricalInstalledOwnerProbe.run(tx, provider, caller, coordinator, original, preparedOwner, sources, accepted,
                            policy, fresh, container, resolver, limits, budget, observation, fault, database, check);
                    return;
                }
                var activation = new RepositoryHistoricalSuccessorActivation(tx, budget, plan, original, sources, new DriveLedger(tx));
                if (mixed) {
                    var foreignScopes = new DocumentPublicationScopeCalls();
                    try (var foreign = foreignScopes.enter()) {
                        try {
                            activation.openAcceptedExecution(coordinator, caller, accepted, scopes, foreign, RepositoryReadControl.NONE);
                            throw new AssertionError("Attached through a foreign shutdown barrier");
                        } catch (IllegalArgumentException refused) {
                            require(refused.getMessage().contains("another publication scope"), "foreign permit rejected");
                        }
                        require(scopes.isIdle() && !foreignScopes.isIdle(), "foreign rejection preserves both barriers");
                    }
                    require(foreignScopes.isIdle(), "foreign caller still owns its permit");
                    var ended = scopes.enter();
                    ended.close();
                    try {
                        activation.openAcceptedExecution(coordinator, caller, accepted, scopes, ended, RepositoryReadControl.NONE);
                        throw new AssertionError("Attached through an ended host call");
                    } catch (IllegalStateException refused) {
                        require(refused.getMessage().contains("has ended"), "ended permit rejected");
                    }
                    try (var cancelled = scopes.enter()) {
                        var control = new RepositoryReadControl() {
                            public boolean isCancelled() { return true; }
                            public long remainingNanos() { return Long.MAX_VALUE; }
                        };
                        try {
                            activation.openAcceptedExecution(coordinator, caller, accepted, scopes, cancelled, control);
                            throw new AssertionError("Cancelled attachment succeeded");
                        } catch (RepositoryException refused) {
                            require(refused.code() == RepositoryException.Code.CANCELLED, "attachment cancellation propagates");
                        }
                    }
                    require(scopes.isIdle(), "failed attachment refunds its child permit");
                }
                try (var outerCall = scopes.enter()) {
                    if (mixed) {
                        scopes.close();
                        try {
                            scopes.enter();
                            throw new AssertionError("Closed publication admission accepted a new call");
                        } catch (RepositoryException refused) {
                            require(refused.code() == RepositoryException.Code.UNAVAILABLE, "new admission stays closed");
                        }
                    }
                    try (var execution = mixed ? activation.openAcceptedExecution(coordinator, caller, accepted, scopes, outerCall, RepositoryReadControl.NONE) : activation.openExecution(coordinator, caller, accepted, scopes, RepositoryReadControl.NONE)) {
                        outerCall.close();
                        require(!scopes.isIdle(), "execution retains its accepted call until it closes");
                        try {
                            oldExecution.createAssessment(caller, oldAssessment, Map.of(), observation,
                                    new RepositorySchemaArtifacts(tx), oldStart, RepositoryReadControl.NONE);
                            throw new AssertionError("Expired predecessor performed late CREATE");
                        } catch (RepositoryExecutionClaimLedger.Fenced expected) { /* Exact predecessor claim refused. */ }
                        var admitted = execution.admitUploads(caller, RepositoryReadControl.NONE);
                        require(admitted.attempts().size() == (mixed ? 1 : 0), "only mixed successor needs a fresh upload attempt");
                        var owner = tx.inTransaction(em -> {
                            var claim = RepositoryExecutionClaimLedger.lockLive(em, plan.next().key(), command.sha256(),
                                    reservation.predecessor().epoch()+1, reservation.successorToken());
                            return RepositoryOperationLedger.lockLiveOwner(em, plan.next().key(), plan.next().predecessorGeneration()+1,
                                    plan.next().seeds().ownerNonce(), Optional.of(claim));
                        });
                        var started = execution.start(caller, Duration.ofMinutes(1), RepositoryReadControl.NONE);
                        require(!started.assessment().equals(oldStart.assessment()), "successor owns a distinct start identity");
                        try (var assessment = execution.prepareAssessment(caller, policy, Map.of("a", fresh), container, resolver,
                                limits, Instant.now(), RepositoryReadControl.NONE)) {
                            Map<String, DocumentSelectedAttemptLedger.Selected> selections = Map.of();
                            if (mixed) {
                                var attempt = admitted.attempts().getFirst();
                                require(attempt.id().equals(plan.next().seeds().attempts().get("a"))
                                        && !attempt.id().equals(original.seeds().attempts().get("a")), "successor has a new attempt identity");
                                require(attempt.token().equals(plan.next().seeds().uploadTokens().get("a"))
                                        && !attempt.token().equals(original.seeds().uploadTokens().get("a")), "successor has a new upload token");
                                var selected = new DocumentSelectedAttemptLedger.Selected("a", 1, attempt.id(), attempt.token());
                                var physical = assessment.preparePhysical(plan.next().placements(), plan.next().seeds().attempts(),
                                        plan.next().lease(), plan.next().seeds().uploadTokens(), RepositoryReadControl.NONE);
                                var member = physical.plan().members().getFirst();
                                var measured = member.attempt().orElseThrow().uploads().stream().map(upload -> {
                                    var object = upload.object();
                                    var actual = DocumentPartTransfer.upload(provider.store(), member.placement().drive().namespace(), object,
                                            fresh.get(upload.revisionOrdinal()).toByteArray(), Map.of(), () -> {}, () -> {});
                                    return new DocumentSelectedAttemptLedger.Observation(object.objectKey(), object.size(), object.sha256(),
                                            object.contentType(), actual.version(), actual.etag());
                                }).toList();
                                new DocumentSelectedAttemptLedger(tx).verifyBatch(owner, selected, measured);
                                require(tx.readOnly(em -> ((Number) em.createNativeQuery("""
                                        SELECT count(*) FROM document_part_attempt_objects WHERE attempt_id=:id
                                        """).setParameter("id", original.seeds().attempts().get("a")).getSingleResult()).longValue()) == 1,
                                        "predecessor retains its one admitted upload object");
                                require(tx.readOnly(em -> ((Number) em.createNativeQuery("""
                                        SELECT count(*) FROM document_part_attempt_objects
                                        WHERE attempt_id=:id AND (verified OR provider_version IS NOT NULL OR etag IS NOT NULL)
                                        """).setParameter("id", original.seeds().attempts().get("a")).getSingleResult()).longValue()) == 0,
                                        "successor verification leaves predecessor upload objects unverified");
                                selections = Map.of("a", selected);
                            }
                            var created = execution.createAssessment(caller, assessment, selections, observation,
                                    new RepositorySchemaArtifacts(tx), started, RepositoryReadControl.NONE);
                            var found = new DocumentAssessmentDiscovery(tx).discover(caller, owner, command, () -> {}).orElseThrow();
                            require(found.stage().equals(created), "successor stage has exact discovered identity");
                            tx.inTransaction(em -> {
                                require(DocumentAssessmentReconciliation.verifyRetainedInTransaction(em, caller, owner,
                                        command, found.selections(), created, budget, () -> {}).orElseThrow().equals(created),
                                        "same-transaction retained verification matches CREATE");
                                try {
                                    tx.inTransaction(contender -> {
                                        contender.createNativeQuery("SELECT assessment_id FROM document_assessment_owners WHERE assessment_id=:id FOR UPDATE NOWAIT")
                                                .setParameter("id", created.assessment()).getSingleResult();
                                    });
                                    throw new AssertionError("Retained assessment lock escaped the caller transaction");
                                } catch (RuntimeException locked) {
                                    Throwable cause = locked;
                                    while (cause != null && !(cause instanceof java.sql.SQLException)) cause = cause.getCause();
                                    require(cause instanceof java.sql.SQLException sql && "55P03".equals(sql.getSQLState()),
                                            "independent transaction observes exact PostgreSQL lock refusal");
                                }
                            });
                            require(new DocumentAssessmentReconciliation(tx).observeRetained(caller, owner, command, found.selections(),
                                    created.assessment(), created.manifestSha256(), created.retainUntil(), budget, () -> {})
                                    .orElseThrow().equals(created), "successor retained evidence reconciles exactly");
                            if (mixed) {
                                HistoricalClaimedMixedPublicationProbe.run(tx, provider, caller, command, owner, execution,
                                        assessment, selections, observation, created, fresh);
                                System.out.println(caller.processAuthority() ? "HISTORICAL_MIXED_SUCCESSOR_PUBLICATION_OK"
                                        : "SCOPED_HISTORICAL_MIXED_SUCCESSOR_PUBLICATION_OK");
                            } else {
                                var publication = new DocumentPublicationCommit(tx, new DriveLedger(tx), true, false);
                                var result = execution.publishAssessment(caller, assessment, Map.of(), observation,
                                        new RepositorySchemaArtifacts(tx), publication, created, RepositoryReadControl.NONE);
                                require(result.getOwnerGeneration() == owner.generation() && result.getMembersCount() == 1,
                                        "successor publishes one member under its exact generation");
                                require(result.getCommandSha256().equals(command.sha256()), "publication binds canonical command");
                                require(new DocumentPublicationReplay(tx).observe(caller, command).result().orElseThrow().equals(result),
                                        "authorized durable replay returns exact successor receipt");
                                try {
                                    execution.publishAssessment(caller, assessment, Map.of(), observation,
                                            new RepositorySchemaArtifacts(tx), publication, created, RepositoryReadControl.NONE);
                                    throw new AssertionError("Publication attempt was reused");
                                } catch (RepositoryException refused) {
                                    require(refused.code() == RepositoryException.Code.FAILED_PRECONDITION
                                            && refused.getMessage().contains("reconciliation"), "repeat publication requires reconciliation");
                                }
                                System.out.println("CLAIMED_HISTORICAL_SUCCESSOR_PUBLICATION_OK");
                            }

                        }
                        long starts = tx.readOnly(em -> ((Number) em.createNativeQuery(
                                "SELECT count(*) FROM repository_publication_assessment_starts WHERE operation_id=:op")
                                .setParameter("op", command.operationId()).getSingleResult()).longValue());
                        long assessments = tx.readOnly(em -> ((Number) em.createNativeQuery(
                                "SELECT count(*) FROM document_assessment_owners WHERE operation_id=:op")
                                .setParameter("op", command.operationId()).getSingleResult()).longValue());
                        require(starts == 2 && assessments == 1, "exactly one original and one successor start, one assessment");
                        var originalId = tx.readOnly(em -> em.createNativeQuery("""
                                SELECT assessment_id FROM repository_publication_assessment_starts
                                WHERE operation_id=:op AND predecessor_generation=0
                                """).setParameter("op", command.operationId()).getSingleResult());
                        require(originalId.equals(oldStart.assessment()), "original start remains unchanged");
                        long published = tx.readOnly(em -> ((Number) em.createNativeQuery(
                                "SELECT count(*) FROM document_revision_commits WHERE operation_id=:op")
                                .setParameter("op", command.operationId()).getSingleResult()).longValue());
                        require(published == 1, "exactly one successor publication, no duplicate revision");
                    }
                }
                require(scopes.isIdle(), "execution close releases accepted call after outer call ends");
                accepted.close();
                require(activation.tentativeCapture().orElseThrow().complete(coordinator, Duration.ZERO, RepositoryReadControl.NONE).isPresent(),
                        "new capture drains after its own accepted work closes");
            } finally {
                scopes.close(); history.close(); require(history.awaitDrained(Duration.ofSeconds(1)), "new history drains");
                history.release(); reads.fence(); reads.attestLocalQuiescence();
            }
            }
        }
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
