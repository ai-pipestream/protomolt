package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.engine.DocumentHistoricalOperations;
import ai.protomolt.proto.repo.engine.DocumentPartReader;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.StringValue;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** Real provider transfers; failures are injected only after the real GET returns. */
public final class NativeHistoricalMaterializationProbe {
    static void run(Tx tx, AssessmentProviderProbe provider, DocumentPublishedRevision revision) throws Exception {
        var caller = new RepositoryCaller("materialization-reader", false, Set.of("account"), Set.of());
        var id = UUID.fromString(revision.getRevisionId()); var address = revision.getAddress();
        var node = ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(address);
        String originalSecurity = tx.readOnly(em -> (String) em.createNativeQuery("SELECT security::text FROM documents WHERE node_id=:node")
                .setParameter("node", node).getSingleResult());
        var row = tx.readOnly(em -> (Object[]) em.createNativeQuery("""
                SELECT revision_ordinal,encode(root_locator_sha256,'hex'),evidence_bytes
                FROM document_revision_schema_evidence WHERE revision_id=:revision ORDER BY revision_ordinal LIMIT 1
                """).setParameter("revision", id).getSingleResult());
        int ordinal = ((Number) row[0]).intValue();
        var evidence = DocumentRootSchemaEvidence.parseFrom((byte[]) row[2]);
        var path = evidence.getOccurrencesList().stream().filter(value -> value.getStepsCount() == 1).findFirst().orElseThrow();
        var selection = new HistoricalMaterializationRepository.Selection(ordinal, (String) row[1], DocumentPartCodec.sha256Hex(path.toByteArray()));
        var limits = new HistoricalMaterializationRepository.Limits(4_000_000, 4_000_000, 16_000_000, 64, 8_000_000, 64);
        try {
            for (int mode = 0; mode < 5; mode++) {
                security(tx, node, originalSecurity);
                var ledger = new DocumentReadLedger(tx, UUID.randomUUID(), 1);
                final DocumentPublicationLedger.BoundPart expected;
                try (var initial = ledger.captureHistorical(caller, address, id); var use = initial.use()) {
                    require(use.plan().entries().size() > 1, "fixture has unselected provider parts");
                    expected = use.plan().entries().stream().filter(entry -> entry.revisionOrdinal() == ordinal).findFirst().orElseThrow().part();
                }
                require(ledger.releaseDrained(1) == 1, "initial capture released");
                if (mode == 0) {
                    byte[] newer = "newer provider bytes must not replace archived version".getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    var written = provider.store().put(new BlobStore.PutSpec(expected.binding().namespace(), expected.part().key(),
                            expected.part().contentType(), Map.of(), DocumentPartCodec.sha256Hex(newer)), newer);
                    require(!written.versionId().equals(expected.part().providerVersion()), "distinct latest version created");
                }
                var calls = new AtomicInteger(); var cancelled = new AtomicBoolean(); final int scenario = mode;
                BlobStore observed = (BlobStore) java.lang.reflect.Proxy.newProxyInstance(BlobStore.class.getClassLoader(),
                        new Class<?>[] {BlobStore.class}, (proxy, method, args) -> {
                            final Object value;
                            try { value = method.invoke(provider.store(), args); }
                            catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                            if (method.getName().equals("getBounded")) {
                                calls.incrementAndGet();
                                require(args[0].equals(expected.binding().namespace()) && args[1].equals(expected.part().key())
                                        && args[2].equals(expected.part().providerVersion()), "exact selected original provider identity");
                                if (scenario == 1 || scenario == 2) security(tx, node, "{}");
                                if (scenario == 2) throw new IllegalStateException("private-provider-detail-after-real-GET");
                                if (scenario == 3) cancelled.set(true);
                                if (scenario == 4) {
                                    var received = (BlobStore.GetResult) value;
                                    var corrupt = received.data().clone(); corrupt[corrupt.length - 1] ^= 1;
                                    return new BlobStore.GetResult(corrupt, received.contentType(), received.eTag(), received.versionId());
                                }
                            }
                            return value;
                        });
                var budget = new PayloadBudget(64_000_000);
                var control = new RepositoryReadControl() {
                    public boolean isCancelled() { return cancelled.get(); }
                    public long remainingNanos() { return Long.MAX_VALUE; }
                };
                try (var reader = new DocumentPartReader((generation, profile) -> {
                    require(generation.equals(expected.binding().generation()) && profile.equals(expected.binding().profile()),
                            "original backend identity lookup");
                    return observed;
                }, 2, 16_000_000, budget)) {
                    HistoricalMaterializationRepository operations = new DocumentHistoricalOperations(ledger, reader, budget);
                    if (mode == 0) {
                        var result = operations.readMaterialized(caller, address, id, selection, limits, control);
                        try (result) {
                            var view = result.view(control);
                            require(view.selection().equals(selection) && view.occurrence().revisionOrdinal() == ordinal,
                                    "SPI preserves requested occurrence identity");
                            require(view.path().equals(path), "SPI retains the complete selected path");
                            require(view.original().unpack(StringValue.class).getValue().equals("retained payload"), "archived version decoded");
                            require(view.schema().getArtifactSha256().equals(path.getSteps(0).getAnyBoundary().getResolved().getArtifactSha256()),
                                    "recorded schema selected");
                            var definition = view.definition();
                            require(DocumentPartCodec.sha256Hex(definition.descriptorArtifact().toByteArray()).equals(view.schema().getArtifactSha256())
                                    && definition.reference().getDescriptorSha256().equals(view.schema().getArtifactSha256()), "exact retained descriptor delivered");
                            require(DocumentPartCodec.sha256Hex(definition.metadataArtifact().toByteArray()).equals(definition.reference().getMetadataSha256())
                                    && definition.metadata().getSchema().equals(view.schema().getSchema()), "retained metadata identity delivered");
                            var offlineFiles = ai.protomolt.proto.descriptors.ClosedDescriptorSet.load(definition.descriptorArtifact(),
                                    new ai.protomolt.proto.descriptors.ClosedDescriptorSet.Limits(16_000_000, 256, 4096, 64));
                            var offlineType = offlineFiles.stream().flatMap(file -> file.getMessageTypes().stream())
                                    .filter(type -> type.getFullName().equals(view.schema().getSchema().getTypeName())).findFirst().orElseThrow();
                            var offline = com.google.protobuf.DynamicMessage.parseFrom(offlineType, view.original().getValue());
                            require(offline.getField(offlineType.findFieldByNumber(1)).equals("retained payload"), "offline decoding uses delivered definition");
                            require(ledger.releaseDrained(1) == 0 && budget.reservedBytes() > 0, "result owns independent pin and bytes");
                            security(tx, node, "{}");
                            try { result.view(control); throw new AssertionError("revoked SPI view exposed content"); }
                            catch (RepositoryException denied) { require(denied.code() == RepositoryException.Code.NOT_FOUND, "SPI view reauthorizes"); }
                            finally { security(tx, node, originalSecurity); }
                        }
                        try { result.view(control); throw new AssertionError("closed SPI view exposed content"); }
                        catch (IllegalStateException closed) { /* Owned content is no longer available. */ }
                        result.close();
                        require(calls.get() == 1, "only selected fragment was fetched");
                        var missingSelection = new HistoricalMaterializationRepository.Selection(9999, selection.rootSha256(), selection.pathSha256());
                        try (var unexpected = operations.readMaterialized(caller, address, id, missingSelection, limits, control)) {
                            throw new AssertionError("unknown ordinal returned content");
                        } catch (RepositoryException missing) { require(missing.code() == RepositoryException.Code.NOT_FOUND, "unknown ordinal unavailable"); }
                        require(calls.get() == 1, "unknown ordinal does not fetch provider content");
                        NativeHistoricalMaterializationTransportProbe.run(operations, address, id, selection, limits,
                                () -> security(tx, node, "{}"), () -> security(tx, node, originalSecurity));
                    } else {
                        try (var unexpected = operations.readMaterialized(caller, address, id, selection, limits, control)) {
                            throw new AssertionError("faulted materialization returned content");
                        } catch (RepositoryException failure) {
                            var code = scenario < 3 ? RepositoryException.Code.NOT_FOUND
                                    : scenario == 3 ? RepositoryException.Code.CANCELLED : RepositoryException.Code.DATA_LOSS;
                            require(failure.code() == code, "provider failure classification");
                            if (scenario < 3) require(failure.getCause() == null && failure.getSuppressed().length == 0
                                    && !failure.toString().contains("private-provider-detail"), "revocation redacts provider details");
                        }
                        require(calls.get() == 1, "fault injected after one real selected GET");
                    }
                    reader.close();
                    require(reader.awaitIdle(Duration.ofSeconds(5)), "selected provider work drains");
                    require(budget.reservedBytes() == 0, "provider and decoding reservations drained");
                } finally {
                    ledger.releaseDrained(1);
                    ledger.fence(); ledger.attestLocalQuiescence();
                    require(ledger.outstandingReads() == 0, "selected history pin drained");
                }
            }
        } finally { security(tx, node, originalSecurity); }
        System.out.println("NATIVE_HISTORICAL_MATERIALIZATION_OK");
    }
    private static void security(Tx tx, UUID node, String value) {
        tx.inTransaction(em -> { em.createNativeQuery("UPDATE documents SET security=CAST(:policy AS jsonb) WHERE node_id=:node")
                .setParameter("node", node).setParameter("policy", value).executeUpdate(); });
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
