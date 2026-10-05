package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.blob.s3.*;
import ai.protomolt.proto.repo.engine.DocumentPartReader;
import ai.protomolt.proto.repo.spi.*;
import com.google.protobuf.ByteString;
import java.util.Map;
import java.util.UUID;

/** Real versioned LocalStack S3 transfers and reader execution in the production-JAR host. */
public final class AssessmentProviderProbe implements AutoCloseable {
    private final software.amazon.awssdk.services.s3.S3Client client;
    private final BlobStore store;
    private final ManagedBackendLedger.Profile profile;

    AssessmentProviderProbe() {
        String endpoint = System.getenv("PROTOMOLT_TEST_S3_ENDPOINT"), region = System.getenv("PROTOMOLT_TEST_S3_REGION");
        client = software.amazon.awssdk.services.s3.S3Client.builder().endpointOverride(java.net.URI.create(endpoint))
                .region(software.amazon.awssdk.regions.Region.of(region)).forcePathStyle(true)
                .httpClientBuilder(software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient.builder())
                .credentialsProvider(software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                        software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(
                                System.getenv("PROTOMOLT_TEST_S3_ACCESS"), System.getenv("PROTOMOLT_TEST_S3_SECRET")))).build();
        try {
            client.createBucket(request -> request.bucket("namespace"));
            client.putBucketVersioning(request -> request.bucket("namespace").versioningConfiguration(
                    configuration -> configuration.status(software.amazon.awssdk.services.s3.model.BucketVersioningStatus.ENABLED)));
            store = new S3BlobStore(client);
            profile = new ManagedBackendLedger.Profile(S3BackendIdentity.of(endpoint, region, true), "assessment-provider");
        } catch (RuntimeException | Error failure) { client.close(); throw failure; }
    }
    BlobStore store() { return store; }
    ManagedBackendLedger.Profile profile() { return profile; }
    @Override public void close() { client.close(); }

    void verifyReads(Tx tx, RepositoryOperationLedger.Owner owner, DocumentPublicationCommand command,
            Map<String,DocumentAssessmentRetainedSlots.UploadSelection> selections, DocumentAssessmentCreation.Created retained,
            PayloadBudget captureBudget, Map<String,Map<Integer,ByteString>> fragments) throws Exception {
        var caller = new RepositoryCaller("principal", false, java.util.Set.of("account"), java.util.Set.of());
        UUID destination = ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(command.intent().getMembers(1).getDestination().getAddress());
        for (int mode = 0; mode < 4; mode++) {
            var reads = new DocumentReadLedger(tx, UUID.randomUUID(), 1);
            var captured = reads.captureAssessment(caller, owner, command, selections, retained.assessment(),
                    retained.manifestSha256(), retained.retainUntil(), captureBudget, () -> {});
            var payload = new PayloadBudget(16_000_000);
            var invoked = new java.util.concurrent.atomic.AtomicBoolean();
            var entered = new java.util.concurrent.CountDownLatch(1);
            var releaseProvider = new java.util.concurrent.CountDownLatch(1);
            var cancelled = new java.util.concurrent.atomic.AtomicBoolean();
            final int scenario = mode;
            BlobStore selected = mode == 0 ? store : (BlobStore) java.lang.reflect.Proxy.newProxyInstance(
                    BlobStore.class.getClassLoader(), new Class<?>[]{BlobStore.class}, (proxy, method, args) -> {
                        final Object result;
                        try { result = method.invoke(store, args); }
                        catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                        if (method.getName().equals("getBounded") && invoked.compareAndSet(false, true)) {
                            if (scenario == 3) {
                                entered.countDown();
                                awaitProviderRelease(releaseProvider);
                            } else policy(tx, destination, "ACCESS_DENY");
                            if (scenario == 2) throw new IllegalStateException("private-provider-detail-after-real-GET");
                        }
                        return result;
                    });
            var reader = new DocumentPartReader((generation, original) -> {
                require(generation.equals("assessment-s3") && original.equals(profile), "exact original backend lookup");
                return selected;
            }, 2, 16_000_000, payload);
            try (reader) {
                if (mode == 0) {
                    // A new latest version must not replace the version bound into the assessment.
                    try (var use = captured.use()) {
                        var first = use.plan().entries().getFirst().part().part();
                        byte[] other = "different-latest-version".getBytes(java.nio.charset.StandardCharsets.UTF_8);
                        var latest = store.put(new BlobStore.PutSpec("namespace", first.key(), first.contentType(), Map.of(),
                                ai.protomolt.proto.repo.codec.DocumentPartCodec.sha256Hex(other)), other);
                        require(!latest.versionId().equals(first.providerVersion()), "distinct latest provider version");
                    }
                    for (String member : java.util.List.of("a", "b")) {
                        try (var batch = reader.readAssessment(captured, member, RepositoryReadControl.NONE);
                                var use = captured.use()) {
                            var entries = use.plan().entries(member);
                            require(batch.parts().size() == entries.size(), "member fragment count");
                            for (int i = 0; i < entries.size(); i++)
                                require(ByteString.copyFrom(batch.parts().get(i).bytes()).equals(
                                        fragments.get(member).get(entries.get(i).revisionOrdinal())), "exact retained fragment bytes");
                        }
                    }
                    try (var batch = reader.readAssessment(captured, "a", RepositoryReadControl.NONE)) {
                        captured.close();
                        require(reads.releaseDrained(1) == 0, "returned batch keeps assessment retained");
                    }
                } else if (mode < 3) {
                    try (var unexpected = reader.readAssessment(captured, "a", RepositoryReadControl.NONE)) {
                        throw new AssertionError("Revoked provider result was delivered");
                    } catch (RepositoryException expected) {
                        require(expected.code() == RepositoryException.Code.NOT_FOUND, "provider result/error reauthorized");
                        require(!expected.toString().contains("private-provider-detail"), "provider error redacted");
                        require(expected.getCause() == null && expected.getSuppressed().length == 0, "provider error not attached indirectly");
                    }
                    require(invoked.get(), "revocation occurred after a genuine GET");
                } else {
                    var control = new RepositoryReadControl() {
                        @Override public boolean isCancelled() { return cancelled.get(); }
                        @Override public long remainingNanos() { return Long.MAX_VALUE; }
                    };
                    try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
                        var pending = executor.submit(() -> reader.readAssessment(captured, "a", control));
                        require(entered.await(5, java.util.concurrent.TimeUnit.SECONDS), "real GET reached controlled completion gate");
                        cancelled.set(true);
                        try (var unexpected = pending.get(5, java.util.concurrent.TimeUnit.SECONDS)) {
                            throw new AssertionError("Cancelled provider read was delivered");
                        } catch (java.util.concurrent.ExecutionException failure) {
                            require(failure.getCause() instanceof RepositoryException rejected
                                    && rejected.code() == RepositoryException.Code.CANCELLED, "prompt cancellation result");
                        }
                        captured.close();
                        require(reads.releaseDrained(1) == 0 && payload.reservedBytes() > 0,
                                "cancelled worker retains session and payload until actual completion");
                    } finally { releaseProvider.countDown(); }
                }
            } finally {
                releaseProvider.countDown();
                captured.close();
                require(reader.awaitIdle(java.time.Duration.ofSeconds(5)), "reader idle before borrowed client closure");
                require(captured.awaitDrained(java.time.Duration.ofSeconds(5)), "provider uses drained");
                require(reads.releaseDrained(1) == 1 && reads.outstandingReads() == 0, "exact session cleanup");
                require(payload.reservedBytes() == 0, "payload reservations released");
                policy(tx, destination, "ACCESS_READ");
            }
        }
        System.out.println("ASSESSMENT_PROVIDER_READS_OK");
    }
    private static void awaitProviderRelease(java.util.concurrent.CountDownLatch release) {
        boolean interrupted = false;
        long deadline = System.nanoTime() + java.time.Duration.ofSeconds(10).toNanos();
        try {
            while (true) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) throw new AssertionError("Controlled provider completion timed out");
                try {
                    if (!release.await(remaining, java.util.concurrent.TimeUnit.NANOSECONDS))
                        throw new AssertionError("Controlled provider completion timed out");
                    return;
                } catch (InterruptedException cancelled) { interrupted = true; }
            }
        } finally { if (interrupted) Thread.currentThread().interrupt(); }
    }
    private static void policy(Tx tx, UUID node, String access) {
        tx.inTransaction(em -> {
            int changed = em.createNativeQuery("UPDATE documents SET security=CAST(:policy AS jsonb) WHERE node_id=:node")
                    .setParameter("policy", "{\"permissions\":[{\"identityType\":\"public\",\"identity\":\"public\",\"access\":\"" + access + "\"}]}")
                    .setParameter("node", node).executeUpdate();
            require(changed == 1, "policy fixture selected destination");
        });
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
