package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.codec.*;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.*;

/** Faults follow actual provider writes; no provider result is fabricated. */
final class HistoricalUploadFaultProbe {
    static void run(Tx independent, RepositoryCaller caller, DocumentPublicationCommand original,
            DocumentUploadPlan.Placement placement, DocumentPublishedRevision source,
            Map<Integer, ByteString> fragments, PayloadBudget budget, javax.sql.DataSource database) throws Exception {
        for (String mode : List.of("replay", "revoke", "cancel", "lost-provider-reply", "lost-verification-reply", "shutdown", "expire", "takeover")) {
            try (var fault = mode.equals("lost-verification-reply") ? new HistoricalCreateCommitFault(database, true) : null) {
            var tx = fault == null ? independent : fault.tx();
            var sourceId = ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(source.getAddress());
            var current = new DocumentLedger(tx).findByNodeId(sourceId).orElseThrow();
            var member = original.intent().getMembers(0);
            var command = new DocumentPublicationCommand(original.intent().toBuilder().setOperationId(UUID.randomUUID().toString())
                    .setMembers(0, member.toBuilder().setDestination(member.getDestination().toBuilder()
                            .setExpectedMutationRevision(current.mutationRevision))).build());
            var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), caller.principalName(), command.operationId());
            var record = new DocumentPublicationPreparationRecord(key, command, DocumentPublicationSeeds.mint(key, command),
                    Map.of(placement.drive().id(), placement), mode.equals("expire") || mode.equals("takeover")
                            ? Duration.ofSeconds(2) : Duration.ofMinutes(2), 0);
            if (fault != null) fault.armVerification(record.seeds().attempts().get("a"));
            var security = tx.readOnly(em -> (String) em.createNativeQuery("SELECT security::text FROM documents WHERE node_id=:id")
                    .setParameter("id", sourceId).getSingleResult());
            long baseline = budget.reservedBytes();
            var calls = new AtomicInteger();
            var cancelled = new AtomicBoolean();
            var revoked = new AtomicBoolean();
            var spec = new AtomicReference<BlobStore.PutSpec>();
            var receipt = new AtomicReference<BlobStore.PutResult>();
            var written = new AtomicReference<byte[]>();
            var providerEntered = new java.util.concurrent.CountDownLatch(1);
            var providerRelease = new java.util.concurrent.CountDownLatch(1);
            var control = new RepositoryReadControl() {
                public boolean isCancelled() { return cancelled.get(); }
                public long remainingNanos() { return Long.MAX_VALUE; }
            };
            var owner = new RepositoryInstalledHistoricalAttempts(tx, budget, new DriveLedger(tx), 1);
            var reads = new DocumentReadLedger(tx, UUID.randomUUID());
            var coordinator = new RepositoryCaller(caller.principalName(), true);
            var incarnation = UUID.randomUUID();
            var successor = new AtomicReference<RepositorySuccessorInstall.Plan>();
            try (var opened = new ai.protomolt.proto.repo.blob.s3.S3BlobStoreProvider().open(Map.of(
                    "endpoint", System.getenv("PROTOMOLT_TEST_S3_ENDPOINT"), "region", System.getenv("PROTOMOLT_TEST_S3_REGION"),
                    "path-style", "true", "conditional-writes", "true", "access-key", System.getenv("PROTOMOLT_TEST_S3_ACCESS"),
                    "secret-key", System.getenv("PROTOMOLT_TEST_S3_SECRET")))) {
                var observed = (BlobStore) java.lang.reflect.Proxy.newProxyInstance(BlobStore.class.getClassLoader(),
                        new Class<?>[]{BlobStore.class}, (proxy, method, args) -> {
                            final Object result;
                            try { result = method.invoke(opened.store(), args); }
                            catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                            if (method.getName().equals("put")) {
                                calls.incrementAndGet(); spec.set((BlobStore.PutSpec) args[0]);
                                receipt.set((BlobStore.PutResult) result); written.set(((byte[]) args[1]).clone());
                                if (mode.equals("shutdown")) {
                                    providerEntered.countDown();
                                    awaitProvider(providerRelease);
                                }
                                if (mode.equals("revoke")) {
                                    revoked.set(true);
                                    security(tx, sourceId,
                                            "{\"permissions\":[{\"identityType\":\"public\",\"identity\":\"public\",\"access\":\"ACCESS_DENY\"}]}");
                                }
                                if (mode.equals("cancel")) cancelled.set(true);
                                if (mode.equals("expire")) expire(tx, key);
                                if (mode.equals("takeover") && calls.get() == 1) {
                                    expire(tx, key);
                                    successor.set(HistoricalUploadTakeover.install(tx, budget, coordinator, record, incarnation));
                                }
                                if (mode.equals("lost-provider-reply")) throw new IllegalStateException("injected lost historical PUT reply");
                            }
                            return result;
                        });
                try (var borrowed = new OpenedBlobStore(observed, () -> {}, opened.capabilities(), opened::ensureNamespace, opened.reclaimer());
                     var uploads = new DocumentUploadCoordinator(tx, new DriveLedger(tx), budget, (generation, profile) -> {
                         require(generation.equals(placement.generation()) && profile.equals(placement.profile()), "exact historical backend");
                         return new DocumentUploadCoordinator.Backend(profile.identity(), borrowed);
                     }, 2, Duration.ofMillis(25), new SqlTimeouts(
                             mode.equals("expire") || mode.equals("takeover") ? Duration.ofSeconds(5) : Duration.ofSeconds(2), Duration.ofSeconds(10)));
                     var request = owner.beginInitial(caller, record, Map.of("a", DocumentPublicationCandidate.Mode.TYPED), incarnation)) {
                    request.captureSources(reads, RepositoryReadControl.NONE);
                    request.openExecution(coordinator, RepositoryReadControl.NONE);
                    var bodies = new HashMap<DocumentUploadPayloads.Key, PartObject>();
                    var parts = command.intent().getMembers(0).getPartsList();
                    for (int ordinal = 0; ordinal < parts.size(); ordinal++) {
                        var part = parts.get(ordinal);
                        if (part.hasUpload()) bodies.put(new DocumentUploadPayloads.Key("a", ordinal),
                                new PartObject(part.getSlot().getPart(), part.getSlot().getSubKey(),
                                        fragments.get(ordinal).toByteArray(), part.getUpload().getSha256()));
                    }
                    require(bodies.size() == 1, "fault fixture requires one real upload");
                    long retained = budget.reservedBytes();
                    if (mode.equals("replay")) {
                        var first = request.stageUploads(uploads, bodies, Map.of(), control);
                        var again = request.stageUploads(uploads, bodies, Map.of(), control);
                        require(first.members().getFirst().selection().equals(again.members().getFirst().selection()), "exact selection replay");
                    } else if (mode.equals("shutdown")) {
                        var finished = new java.util.concurrent.CountDownLatch(1);
                        var failure = new AtomicReference<RuntimeException>();
                        try (var workers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
                            var transfer = workers.submit(() -> {
                                try { request.stageUploads(uploads, bodies, Map.of(), control); }
                                catch (RuntimeException rejected) { failure.set(rejected); }
                                finally { finished.countDown(); }
                            });
                            try {
                                require(providerEntered.await(10, java.util.concurrent.TimeUnit.SECONDS), "provider reaches post-PUT gate");
                                cancelled.set(true);
                                require(transfer.cancel(true), "transfer cancellation submitted");
                                owner.close(); uploads.close();
                                require(transfer.isCancelled() && finished.getCount() == 1, "cancelled future is not completed work");
                                require(!uploads.awaitIdle(Duration.ofMillis(20)), "provider worker keeps coordinator busy");
                                require(!owner.detachClosed(Duration.ofMillis(20), ignored -> coordinator, RepositoryReadControl.NONE),
                                        "shutdown timeout retains active request");
                                require(uploads.providerActivity().active() == 1 && budget.reservedBytes() > retained,
                                        "provider worker retains payload and child metadata");
                                require(reads.outstandingReads() == 1 && reads.releaseDrained(16) == 0,
                                        "active provider work protects historical capture");
                            } finally { providerRelease.countDown(); }
                            require(finished.await(10, java.util.concurrent.TimeUnit.SECONDS), "actual transfer exits after provider release");
                            require(failure.get() instanceof java.util.concurrent.CancellationException
                                    || hasCode(failure.get(), RepositoryException.Code.CANCELLED), "cancelled transfer cannot succeed");
                            require(uploads.awaitIdle(Duration.ofSeconds(1)), "coordinator becomes idle after actual completion");
                        }
                    } else if (fault != null) {
                        try {
                            request.stageUploads(uploads, bodies, Map.of(), control);
                            throw new AssertionError("Lost SQL verification reply returned success");
                        } catch (RuntimeException failure) { fault.requireFailure(failure); }
                        require(independent.readOnly(em -> ((Number) em.createNativeQuery("""
                                SELECT count(*) FROM document_part_attempt_objects WHERE attempt_id=:id AND verified
                                """).setParameter("id", record.seeds().attempts().get("a")).getSingleResult()).longValue()) == 1,
                                "independent transaction confirms verification committed");
                        var again = request.stageUploads(uploads, bodies, Map.of(), control);
                        require(again.members().size() == 1 && again.members().getFirst().selection().equals(
                                new DocumentSelectedAttemptLedger.Selected("a", 1, record.seeds().attempts().get("a"),
                                        record.seeds().uploadTokens().get("a"))), "lost acknowledgement replays exact attempt and token");
                    } else {
                        RuntimeException rejected = null;
                        try { request.stageUploads(uploads, bodies, Map.of(), control); }
                        catch (RuntimeException failure) { rejected = failure; }
                        require(rejected != null, "faulted transfer cannot succeed");
                        if (mode.equals("revoke") || mode.equals("cancel")) {
                            var expected = mode.equals("revoke") ? RepositoryException.Code.NOT_FOUND : RepositoryException.Code.CANCELLED;
                            require(hasCode(rejected, expected), "transfer reports exact authorization or cancellation failure");
                        } else if (mode.equals("expire") || mode.equals("takeover")) require(fenced(rejected), "old claim rejects late provider result");
                        else require(hasMessage(rejected, "injected lost historical PUT reply"), "provider reply failure survives");
                        if (revoked.get()) { security(tx, sourceId, security); revoked.set(false); }
                        cancelled.set(false);
                        try {
                            request.stageUploads(uploads, bodies, Map.of(), control);
                            throw new AssertionError("Unverified historical attempt implicitly retried");
                        } catch (RuntimeException expected) {
                            // Restoring an ACL advances the document mutation revision; that candidate is stale.
                            if (mode.equals("revoke")) require(expected instanceof DocumentLedger.RevisionConflictException,
                                    "restored policy does not revive a stale candidate");
                            else if (mode.equals("expire") || mode.equals("takeover")) require(fenced(expected), "old claim cannot retry");
                            else require(expected instanceof DocumentPartAttemptLedger.FenceException,
                                    "uncertain attempt requires reconciliation");
                        }
                    }
                    require(calls.get() == 1, "replay or rejected retry performs no additional PUT");
                    require(uploads.providerActivity().active() == 0 && budget.reservedBytes() == retained, "transfer workers and bytes drain");
                    var actual = opened.store().getBounded(spec.get().bucket(), spec.get().key(), receipt.get().versionId(), written.get().length);
                    require(Arrays.equals(actual.data(), written.get()) && Objects.equals(actual.versionId(), receipt.get().versionId()),
                            "real stored bytes and provider version remain accounted for");
                    if (mode.equals("takeover")) {
                        require(successor.get() != null, "successor installation completed before predecessor reply");
                        HistoricalUploadTakeover.stage(tx, budget, caller, coordinator, record, successor.get(), source, uploads, bodies);
                        require(calls.get() == 2, "successor writes once and replays without another PUT");
                    }
                    long verified = tx.readOnly(em -> ((Number) em.createNativeQuery("""
                            SELECT count(*) FROM document_part_attempt_objects WHERE attempt_id=:id AND verified
                            """).setParameter("id", record.seeds().attempts().get("a")).getSingleResult()).longValue());
                    require(verified == (mode.equals("replay") || fault != null ? 1 : 0), "verification agrees with actual commit outcome");
                    require(tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM document_assessment_owners WHERE operation_id=:o")
                            .setParameter("o", key.operationId()).getSingleResult()).longValue()) == 0, "staging does not create an assessment");
                }
            } finally {
                try {
                    if (revoked.get()) security(tx, sourceId, security);
                } finally {
                    owner.close();
                    try {
                        require(owner.detachClosed(Duration.ofSeconds(2), ignored -> coordinator, RepositoryReadControl.NONE), "fault owner detaches");
                    } finally {
                        reads.releaseDrained(16);
                        require(reads.outstandingReads() == 0 && budget.reservedBytes() == baseline, "fault capture and memory drain");
                        reads.fence(); reads.attestLocalQuiescence();
                    }
                }
            }
            System.out.println("HISTORICAL_UPLOAD_" + mode.toUpperCase(Locale.ROOT).replace('-', '_') + "_OK");
            }
        }
    }
    private static boolean hasCode(Throwable failure, RepositoryException.Code code) {
        for (var next = failure; next != null; next = next.getCause())
            if (next instanceof RepositoryException repository && repository.code() == code) return true;
        return false;
    }
    private static boolean fenced(Throwable failure) {
        for (var next = failure; next != null; next = next.getCause())
            if (next instanceof RepositoryExecutionClaimLedger.Fenced) return true;
        return false;
    }
    private static void expire(Tx tx, RepositoryOperationLedger.Key key) {
        tx.withTimeouts(new SqlTimeouts(Duration.ofSeconds(5), Duration.ofSeconds(5))).inTransaction(em -> {
            em.createNativeQuery("SELECT claim_token FROM repository_execution_claims WHERE operation_id=:o FOR UPDATE")
                    .setParameter("o", key.operationId()).getSingleResult();
            em.createNativeQuery("SELECT owner_token FROM repository_operation_owners WHERE operation_id=:o FOR UPDATE")
                    .setParameter("o", key.operationId()).getSingleResult();
            // Keep renewal behind the same claim/owner locks while database time crosses both deadlines.
            em.createNativeQuery("""
                    SELECT pg_sleep(GREATEST(0,extract(epoch FROM (GREATEST(c.lease_until,o.lease_until)-clock_timestamp())))+0.05)
                    FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                    WHERE c.operation_id=:id
                    """).setParameter("id", key.operationId()).getSingleResult();
            require(Boolean.TRUE.equals(em.createNativeQuery("""
                    SELECT c.lease_until<=clock_timestamp() AND o.lease_until<=clock_timestamp()
                    FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                    WHERE c.operation_id=:id
                    """).setParameter("id", key.operationId()).getSingleResult()), "database confirms both leases expired");
        });
    }
    private static void awaitProvider(java.util.concurrent.CountDownLatch release) {
        boolean interrupted = false;
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(20);
        try {
            while (true) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) throw new AssertionError("Provider release gate timed out");
                try {
                    if (!release.await(remaining, java.util.concurrent.TimeUnit.NANOSECONDS))
                        throw new AssertionError("Provider release gate timed out");
                    return;
                } catch (InterruptedException cancellation) { interrupted = true; }
            }
        } finally { if (interrupted) Thread.currentThread().interrupt(); }
    }
    private static boolean hasMessage(Throwable failure, String message) {
        for (var next = failure; next != null; next = next.getCause()) if (message.equals(next.getMessage())) return true;
        return false;
    }
    private static void security(Tx tx, UUID node, String value) {
        tx.inTransaction(em -> {
            int changed = em.createNativeQuery("UPDATE documents SET security=CAST(:security AS jsonb) WHERE node_id=:id")
                    .setParameter("security", value).setParameter("id", node).executeUpdate();
            require(changed == 1, "source policy fixture selects exactly one row");
        });
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
