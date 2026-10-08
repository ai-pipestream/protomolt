package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.ByteString;
import java.time.*;
import java.util.*;

/** Real provider publication through a retained initial owner, without a successor installation. */
final class HistoricalInitialOwnerProbe {
    static void run(Tx tx, AssessmentProviderProbe provider, RepositoryCaller caller, DocumentPublicationCommand command,
            DocumentSchemaPolicies.Selection policy, DocumentUploadPlan.Placement placement, DocumentPublishedRevision revision,
            Map<Integer, ByteString> fragments, PayloadBudget budget, DocumentAssessmentRuntimeObserver.Observation observation,
            HistoricalCreateCommitFault fault, javax.sql.DataSource database) throws Exception {
        run(tx, provider, caller, command, policy, placement, revision, fragments, budget, observation, fault, database, false);
        if (fault == null) {
            var current = new DocumentLedger(tx).findByNodeId(
                    ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(revision.getAddress())).orElseThrow();
            var member = command.intent().getMembers(0);
            var rejected = new DocumentPublicationCommand(command.intent().toBuilder().setOperationId(UUID.randomUUID().toString())
                    .setMembers(0, member.toBuilder().setDestination(member.getDestination().toBuilder()
                            .setExpectedMutationRevision(current.mutationRevision))).build());
            run(tx, provider, caller, rejected, policy, placement, revision, fragments, budget, observation, null, database, true);
            var lostReply = new DocumentPublicationCommand(rejected.intent().toBuilder()
                    .setOperationId(UUID.randomUUID().toString()).build());
            try (var rejectionFault = new HistoricalCreateCommitFault(database, true)) {
                run(rejectionFault.tx(), provider, caller, lostReply, policy, placement, revision, fragments, budget,
                        observation, null, database, true, rejectionFault, tx);
            }
        }
    }

    private static void run(Tx tx, AssessmentProviderProbe provider, RepositoryCaller caller, DocumentPublicationCommand command,
            DocumentSchemaPolicies.Selection policy, DocumentUploadPlan.Placement placement, DocumentPublishedRevision revision,
            Map<Integer, ByteString> fragments, PayloadBudget budget, DocumentAssessmentRuntimeObserver.Observation observation,
            HistoricalCreateCommitFault fault, javax.sql.DataSource database, boolean reject) throws Exception {
        run(tx, provider, caller, command, policy, placement, revision, fragments, budget, observation,
                fault, database, reject, null, tx);
    }

    private static void run(Tx tx, AssessmentProviderProbe provider, RepositoryCaller caller, DocumentPublicationCommand command,
            DocumentSchemaPolicies.Selection policy, DocumentUploadPlan.Placement placement, DocumentPublishedRevision revision,
            Map<Integer, ByteString> fragments, PayloadBudget budget, DocumentAssessmentRuntimeObserver.Observation observation,
            HistoricalCreateCommitFault fault, javax.sql.DataSource database, boolean reject,
            HistoricalCreateCommitFault rejectionFault, Tx observer) throws Exception {
        require(!caller.processAuthority(), "initial owner executes as a credential-bound scoped caller");
        var coordinator = new RepositoryCaller(caller.principalName(), true);
        var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), caller.principalName(), command.operationId());
        var record = new DocumentPublicationPreparationRecord(key, command, DocumentPublicationSeeds.mint(key, command),
                Map.of(placement.drive().id(), placement), reject ? Duration.ofSeconds(10) : Duration.ofMinutes(2), 0);
        var modes = Map.of("a", DocumentPublicationCandidate.Mode.TYPED);
        var freshDefinition = reject ? ObservedAssessmentProbe.invalidSchema()
                : ObservedAssessmentProbe.asset(com.google.protobuf.StringValue.getDescriptor());
        long before = budget.reservedBytes();
        var attempts = new RepositoryInstalledHistoricalAttempts(tx, budget, new DriveLedger(tx), 1);
        var reads = new DocumentReadLedger(tx, UUID.randomUUID());
        DocumentReadLedger.PinnedHistory history = null;
        DocumentHistoricalAssessmentSources sources = null;
        DocumentHistoricalAssessmentSources.Work accepted = null;
        boolean transferred = false;
        Throwable primary = null;
        try {
            DocumentAssessmentStartJournal.Started started;
            DocumentUploadCoordinator.Staged admitted;
            try (var request = attempts.beginInitial(caller, record, modes, UUID.randomUUID())) {
                require(count(tx, key, "repository_execution_claims") == 0, "slot reservation has no claim effects");
                history = reads.captureHistorical(caller, revision.getAddress(), UUID.fromString(revision.getRevisionId()));
                sources = DocumentHistoricalAssessmentSources.open(command, caller, List.of(history), RepositoryReadControl.NONE);
                accepted = sources.work();
                request.attachSources(sources, accepted, RepositoryReadControl.NONE); transferred = true;
                sources.close();
                request.openExecution(coordinator, RepositoryReadControl.NONE);
                var bodies = new HashMap<DocumentUploadPayloads.Key, ai.protomolt.proto.repo.codec.PartObject>();
                var parts = command.intent().getMembers(0).getPartsList();
                for (int ordinal = 0; ordinal < parts.size(); ordinal++) {
                    var part = parts.get(ordinal);
                    if (part.hasUpload()) bodies.put(new DocumentUploadPayloads.Key("a", ordinal),
                            new ai.protomolt.proto.repo.codec.PartObject(part.getSlot().getPart(), part.getSlot().getSubKey(),
                                    fragments.get(ordinal).toByteArray(), part.getUpload().getSha256()));
                }
                try (var opened = new ai.protomolt.proto.repo.blob.s3.S3BlobStoreProvider().open(Map.of(
                        "endpoint", System.getenv("PROTOMOLT_TEST_S3_ENDPOINT"), "region", System.getenv("PROTOMOLT_TEST_S3_REGION"),
                        "path-style", "true", "conditional-writes", "true", "access-key", System.getenv("PROTOMOLT_TEST_S3_ACCESS"),
                        "secret-key", System.getenv("PROTOMOLT_TEST_S3_SECRET")));
                     var uploads = new DocumentUploadCoordinator(tx, new DriveLedger(tx), budget, (generation, profile) -> {
                         require(generation.equals(placement.generation()) && profile.equals(provider.profile()), "exact historical upload backend");
                         return new DocumentUploadCoordinator.Backend(profile.identity(), opened);
                     }, 2, Duration.ofMillis(25), new SqlTimeouts(Duration.ofSeconds(5), Duration.ofSeconds(15)))) {
                    admitted = request.stageUploads(uploads, bodies, Map.of(), RepositoryReadControl.NONE);
                    var replay = request.stageUploads(uploads, bodies, Map.of(), RepositoryReadControl.NONE);
                    require(replay.members().stream().map(DocumentUploadCoordinator.StagedMember::selection).toList()
                            .equals(admitted.members().stream().map(DocumentUploadCoordinator.StagedMember::selection).toList()),
                            "historical upload replay retains verified selections");
                    require(uploads.providerActivity().active() == 0, "historical upload workers exited");
                }
                started = request.start(Duration.ofMinutes(1), RepositoryReadControl.NONE);
                var ordinary = new HashMap<Integer, ByteString>();
                var declared = command.intent().getMembers(0);
                for (int ordinal = 0; ordinal < declared.getPartsCount(); ordinal++)
                    if (!declared.getParts(ordinal).hasHistoricalReuse() && !declared.getParts(ordinal).hasEmpty())
                        ordinary.put(ordinal, fragments.get(ordinal));
                var providerReads = new java.util.concurrent.atomic.AtomicInteger();
                try (var reader = new ai.protomolt.proto.repo.engine.DocumentPartReader((generation, profile) -> {
                    require(generation.equals(placement.generation()), "historical reader uses original backend generation");
                    require(profile.equals(provider.profile()), "historical reader uses original provider profile");
                    return provider.store();
                }, 4, 4_000_000, budget)) {
                    request.prepareAssessmentFromReader(policy, Map.of("a", ordinary),
                            Optional.of(ObservedAssessmentProbe.asset(Document.getDescriptor())),
                            (member, occurrence) -> freshDefinition,
                            new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000), Instant.now(),
                            (captured, ordinal, control) -> {
                                providerReads.incrementAndGet();
                                return reader.readHistorical(captured, ordinal, control);
                            }, RepositoryReadControl.NONE);
                    require(providerReads.get() > 0, "initial owner prepares historical fragments from provider");
                    reader.close();
                    require(reader.awaitIdle(Duration.ofSeconds(1)), "historical provider work exits before reader closure");
                }
            }
            require(attempts.drain().equals(new RepositoryInstalledHistoricalAttempts.Drain(0, 1)), "initial entry survives first call");
            var owner = tx.inTransaction(em -> {
                var token = (UUID) em.createNativeQuery("SELECT claim_token FROM repository_execution_claims WHERE account_id=:a AND principal=:p AND operation_id=:o AND claim_epoch=1")
                        .setParameter("a", key.account()).setParameter("p", key.principal()).setParameter("o", key.operationId()).getSingleResult();
                var claim = RepositoryExecutionClaimLedger.lockLive(em, key, command.sha256(), 1, token);
                return RepositoryOperationLedger.lockLiveOwner(em, key, 1, record.seeds().ownerNonce(), Optional.of(claim));
            });
            require(admitted.attempts().size() == 1, "mixed initial entry admits one upload");
            var upload = admitted.attempts().getFirst();
            require(upload.id().equals(record.seeds().attempts().get("a")) && upload.token().equals(record.seeds().uploadTokens().get("a")),
                    "initial upload retains original identities");
            var selected = new DocumentSelectedAttemptLedger.Selected("a", 1, upload.id(), upload.token());
            var selections = Map.of("a", selected);
            try (var request = attempts.resume(caller, command).orElseThrow()) {
                request.openExecution(coordinator, RepositoryReadControl.NONE);
                require(request.start(Duration.ofMinutes(1), RepositoryReadControl.NONE).equals(started), "initial START survives separate calls");
                var progress = request.progress(RepositoryReadControl.NONE);
                require(progress.sourcesAttached() && progress.assessmentPrepared() && !progress.disposalOnly(),
                        "initial retry retains source and assessment phases");
                require(progress.execution().orElseThrow().acknowledgedStart().orElseThrow().equals(started)
                        && progress.acknowledgedCreation().isEmpty(), "START acknowledgement survives borrowing");
                if (fault == null) request.createAssessment(selections, observation, new RepositorySchemaArtifacts(tx), started, RepositoryReadControl.NONE);
                else {
                    fault.arm(started.assessment());
                    try {
                        request.createAssessment(selections, observation, new RepositorySchemaArtifacts(tx), started, RepositoryReadControl.NONE);
                        throw new AssertionError("Initial CREATE did not lose its reply");
                    } catch (RuntimeException failure) { fault.requireFailure(failure); }
                }
                require(request.progress(RepositoryReadControl.NONE).execution().orElseThrow().createAttempted(),
                        "CREATE attempt remains visible even after a lost reply");
                require(request.progress(RepositoryReadControl.NONE).acknowledgedCreation().isPresent() == (fault == null),
                        "only acknowledged CREATE is reported as available");
            }
            if (fault != null) {
                try (var request = attempts.resume(caller, command).orElseThrow()) {
                    try {
                        request.createAssessment(selections, observation, new RepositorySchemaArtifacts(tx), started, RepositoryReadControl.NONE);
                        throw new AssertionError("Initial uncertain CREATE repeated");
                    } catch (RepositoryException refused) {
                        require(refused.code() == RepositoryException.Code.FAILED_PRECONDITION, "uncertain CREATE stays sticky");
                    }
                    var reconciled = request.reconcileAssessment(selections, observation, RepositoryReadControl.NONE).orElseThrow();
                    require(reconciled.assessment().equals(started.assessment()) && reconciled.retainUntil().equals(started.retainUntil()),
                            "initial reconciliation keeps exact START coordinates");
                    require(request.progress(RepositoryReadControl.NONE).acknowledgedCreation().orElseThrow().equals(reconciled),
                            "reconciliation updates retained CREATE progress");
                }
            }
            if (reject) {
                int heldReads = reads.outstandingReads();
                var cancellation = new java.util.concurrent.atomic.AtomicReference<java.util.concurrent.Future<DocumentPublicationReplay.Observation>>();
                try (var workers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();
                     var reader = new ai.protomolt.proto.repo.engine.DocumentPartReader((generation, profile) -> {
                    require(generation.equals(placement.generation()) && profile.equals(provider.profile()), "exact rejection backend");
                    return provider.store();
                }, 4, 4_000_000, budget); var request = attempts.resume(caller, command).orElseThrow()) {
                    if (rejectionFault != null) {
                        rejectionFault.armRejection(owner, started.assessment());
                        rejectionFault.onRejectionCommit(pid -> {
                            cancellation.set(workers.submit(() -> new DocumentPublicationRejections(observer.withTimeouts(
                                    new SqlTimeouts(Duration.ofSeconds(10), Duration.ofSeconds(15))))
                                    .cancel(caller, owner, command, RepositoryReadControl.NONE)));
                            long until = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
                            boolean waiting = false;
                            while (System.nanoTime() < until) {
                                waiting = observer.readOnly(em -> ((Number) em.createNativeQuery("""
                                        SELECT count(*) FROM pg_stat_activity WHERE :pid=ANY(pg_blocking_pids(pid))
                                        """).setParameter("pid", pid).getSingleResult()).longValue() == 1);
                                if (waiting) break;
                                java.util.concurrent.locks.LockSupport.parkNanos(java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(10));
                            }
                            require(waiting && !cancellation.get().isDone(), "cancellation waits for uncommitted historical rejection");
                        });
                        try {
                            request.rejectAssessment(selections, reads, reader,
                                    new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000), observation,
                                    Duration.ofSeconds(5), RepositoryReadControl.NONE);
                            throw new AssertionError("Historical rejection did not lose its commit reply");
                        } catch (RuntimeException failure) { rejectionFault.requireFailure(failure); }
                        require(count(observer, key, "repository_operation_rejection") == 1,
                                "lost reply follows exactly one durable rejection");
                        reader.close(); // Terminal replay must not need another provider read.
                    }
                    var decided = request.rejectAssessment(selections, reads, reader,
                            new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000), observation,
                            Duration.ofSeconds(5), RepositoryReadControl.NONE);
                    require(decided.state() == DocumentPublicationReplay.State.TERMINATED
                            && decided.rejection().orElseThrow().getAssessment().getAssessmentId().equals(started.assessment().toString()),
                            "invalid historical candidate produces exact assessment-bound rejection");
                    require(new DocumentPublicationReplay(observer).observe(caller, command).rejection().equals(decided.rejection()),
                            "historical rejection replays durably");
                    if (cancellation.get() != null) {
                        require(cancellation.get().get(5, java.util.concurrent.TimeUnit.SECONDS).equals(decided),
                                "waiting cancellation returns committed assessment rejection unchanged");
                        require(count(observer, key, "repository_operation_rejection") == 1,
                                "competing cancellation creates no second decision");
                        System.out.println("HISTORICAL_REJECTION_BEATS_CANCELLATION_OK");
                    }
                    reads.releaseDrained(32);
                    require(reads.outstandingReads() == heldReads && !history.isReleased(),
                            "assessment replay releases its reads while historical source stays retained");
                    tx.withTimeouts(new SqlTimeouts(Duration.ofSeconds(5), Duration.ofSeconds(15))).inTransaction(em -> {
                        em.createNativeQuery("""
                                SELECT pg_sleep(GREATEST(0, EXTRACT(EPOCH FROM GREATEST(c.lease_until,o.lease_until)-clock_timestamp()))+0.05)
                                FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                                WHERE c.operation_id=:id
                                """).setParameter("id", command.operationId()).getSingleResult();
                        require(Boolean.TRUE.equals(em.createNativeQuery("""
                                SELECT c.lease_until<=clock_timestamp() AND o.lease_until<=clock_timestamp()
                                FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                                WHERE c.operation_id=:id
                                """).setParameter("id", command.operationId()).getSingleResult()), "rejection owner and claim naturally expire");
                    });
                    require(request.rejectAssessment(selections, reads, reader,
                            new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000), observation,
                            Duration.ofSeconds(5), RepositoryReadControl.NONE).rejection().equals(decided.rejection()),
                            "same historical entry returns its terminal rejection without mutation authority");
                    require(count(tx, key, "document_revision_commits") == 0, "invalid historical candidate never publishes");
                }
                System.out.println(rejectionFault == null ? "HISTORICAL_INITIAL_REJECTION_OK"
                        : "HISTORICAL_INITIAL_REJECTION_REPLY_LOST_OK");
                return;
            }
            DocumentPublicationResult result;
            try (var request = attempts.resume(caller, command).orElseThrow()) {
                result = request.publishAssessment(selections, observation, new RepositorySchemaArtifacts(tx),
                        new DocumentPublicationCommit(tx, new DriveLedger(tx), true, false), RepositoryReadControl.NONE);
                require(request.progress(RepositoryReadControl.NONE).execution().orElseThrow().publicationAttempted(),
                        "publication attempt remains visible for durable replay");
            }
            require(result.getOwnerGeneration() == 1, "receipt belongs to initial generation");
            HistoricalClaimedMixedPublicationProbe.verify(tx, provider, caller, command, owner, selections, fragments, result);
            try (var request = attempts.resume(caller, command).orElseThrow()) {
                try {
                    request.publishAssessment(selections, observation, new RepositorySchemaArtifacts(tx),
                            new DocumentPublicationCommit(tx, new DriveLedger(tx), true, false), RepositoryReadControl.NONE);
                    throw new AssertionError("Initial publication repeated across calls");
                } catch (RepositoryException refused) {
                    require(refused.code() == RepositoryException.Code.FAILED_PRECONDITION
                            && refused.getMessage().contains("reconciliation"), "initial publication remains sticky");
                }
            }
            require(count(tx, key, "repository_publication_assessment_starts") == 1
                    && count(tx, key, "document_assessment_owners") == 1 && count(tx, key, "document_revision_commits") == 1,
                    "initial command has one START, CREATE and publication");
            require(count(tx, key, "repository_coordinator_expirations") == 0 && count(tx, key, "repository_successor_installs") == 0,
                    "initial publication never fabricates a successor reservation or installation");
            try (var worker = accepted.fork()) {
                try (var request = attempts.resume(caller, command).orElseThrow()) {
                    require(request.retireTerminal(coordinator, Duration.ZERO, RepositoryReadControl.NONE)
                            == RepositoryInstalledHistoricalAttempts.Retirement.RETAINED, "terminal entry waits for actual worker");
                }
                require(!history.isReleased() && count(tx, key, "repository_preparation_capture_drains") == 0,
                        "held worker prevents capture drain evidence");
            }
            try (var request = attempts.resume(caller, command).orElseThrow()) {
                require(request.retireTerminal(coordinator, Duration.ofSeconds(1), RepositoryReadControl.NONE)
                        == RepositoryInstalledHistoricalAttempts.Retirement.RETIRED, "initial terminal generation retires after worker drainage");
            }
            require(history.isReleased() && count(tx, key, "repository_preparation_capture_drains") == 1,
                    "exact initial capture drains once");
            require(new DocumentPublicationReplay(tx).observe(caller, command).result().orElseThrow().equals(result),
                    "exact initial receipt replays after retirement");
            require(budget.reservedBytes() == before, "initial retirement returns all retained memory");
        } catch (Exception | Error failure) { primary = failure; throw failure; }
        finally {
            try {
                attempts.close();
                require(attempts.detachClosed(Duration.ofSeconds(1), ignored -> coordinator, RepositoryReadControl.NONE), "initial owner shutdown drains");
                if (!transferred) {
                    if (accepted != null) accepted.close();
                    if (sources != null) sources.close();
                    if (history != null) { history.close(); require(history.awaitDrained(Duration.ofSeconds(1)), "untransferred capture drains"); history.release(); }
                }
                require(budget.reservedBytes() == before, "initial probe returns memory on every exit path");
            } catch (Exception | Error cleanup) {
                if (primary == null) throw cleanup;
                if (cleanup != primary) primary.addSuppressed(cleanup);
            }
        }
        if (fault == null) {
            HistoricalUploadFaultProbe.run(tx, caller, command, placement, revision, fragments, budget, database);
            HistoricalRuntimeShutdownProbe.run(tx, provider, caller, command, placement, revision, budget);
        }
        System.out.println(fault == null ? "SCOPED_INITIAL_HISTORICAL_PUBLICATION_OK" : "SCOPED_INITIAL_HISTORICAL_CREATE_RECONCILED_OK");
    }
    private static long count(Tx tx, RepositoryOperationLedger.Key key, String table) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table + " WHERE account_id=:a AND principal=:p AND operation_id=:o")
                .setParameter("a", key.account()).setParameter("p", key.principal()).setParameter("o", key.operationId()).getSingleResult()).longValue());
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
