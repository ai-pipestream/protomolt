package ai.pipestream.proto.metric.lucene;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.proto.composer.Composer;
import ai.pipestream.proto.metric.MemberRef;
import ai.pipestream.proto.metric.MetricServiceGrpc;
import ai.pipestream.proto.metric.QueryMetricsRequest;
import ai.pipestream.proto.metric.QueryMetricsResponse;
import ai.pipestream.proto.repo.v1.NodeAddress;
import ai.pipestream.proto.search.service.IndexSnapshots;
import ai.pipestream.proto.search.service.RepoDocumentMapping;
import ai.pipestream.proto.search.service.SearchServiceModule;
import ai.pipestream.proto.search.service.SnapshotStore;
import ai.pipestream.proto.search.v1.IndexDocumentRequest;
import ai.pipestream.proto.search.v1.SearchIndexServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The remote metrics node: a writer node indexes and snapshots; a second
 * node mounting only the read-only search role and the metrics role, with
 * no repo anywhere, restores the snapshot and answers aggregates over it.
 * The composition the snapshots were built to make cheap.
 */
class RemoteMetricsNodeTest {

    @TempDir
    Path work;

    /** An in-memory blob store standing in for the bucket. */
    static final class FakeSnapshotStore implements SnapshotStore {

        final Map<String, byte[]> blobs = new LinkedHashMap<>();

        @Override
        public synchronized List<String> list(String prefix) {
            return blobs.keySet().stream().filter(key -> key.startsWith(prefix)).toList();
        }

        @Override
        public synchronized void put(String key, Path file) {
            try {
                blobs.put(key, Files.readAllBytes(file));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public synchronized void download(String key, Path target) {
            byte[] bytes = blobs.get(key);
            if (bytes == null) {
                throw new IllegalStateException("no blob under " + key);
            }
            try {
                Files.write(target, bytes);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public synchronized void delete(String key) {
            blobs.remove(key);
        }
    }

    @Test
    void aMetricsNodeWithNoRepoAnswersOverTheRestoredSnapshot() throws Exception {
        FakeSnapshotStore blobs = new FakeSnapshotStore();

        // The writer: indexes three typed documents, snapshots on close.
        Map<String, ai.pipestream.proto.repo.v1.Document> corpus = Map.of(
                "doc-1", MetricServiceModuleTest.document(
                        "doc-1", "First", "PDF", "2026-07-10T10:00:00Z"),
                "doc-2", MetricServiceModuleTest.document(
                        "doc-2", "Second", "HTML", "2026-08-05T10:00:00Z"),
                "doc-3", MetricServiceModuleTest.document(
                        "doc-3", "Third", "PDF", "2026-08-20T10:00:00Z"));
        SearchServiceModule writer = new SearchServiceModule(new SearchServiceModule.Config(
                0, work.resolve("writer-index"),
                Map.of(RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served()),
                new IndexSnapshots(blobs)));
        try (Composer.Node node = Composer.emptyBuilder()
                .module(new MetricServiceModuleTest.FakeRepoModule(corpus))
                .module(writer)
                .environment(Map.of())
                .build()
                .boot(List.of("repo", "search"))) {
            ManagedChannel channel = NettyChannelBuilder
                    .forAddress("127.0.0.1", writer.grpcPort()).usePlaintext().build();
            try {
                for (String docId : corpus.keySet()) {
                    SearchIndexServiceGrpc.newBlockingStub(channel)
                            .indexDocument(IndexDocumentRequest.newBuilder()
                                    .setAddress(NodeAddress.newBuilder().setDocId(docId)
                                            .setGraphAddressId("ds").setAccountId("acct")
                                            .setGraphId("intake:acct"))
                                    .setMappingSubject(RepoDocumentMapping.SUBJECT)
                                    .build());
                }
            } finally {
                channel.shutdownNow();
            }
        }

        // The remote metrics node: read-only search + metrics, NO repo.
        SearchServiceModule readerSearch = new SearchServiceModule(new SearchServiceModule.Config(
                0, work.resolve("reader-index"),
                Map.of(RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served()),
                new IndexSnapshots(blobs, true),
                true));
        MetricServiceModule metrics = MetricServiceModuleTest.metricsModule(0);
        try (Composer.Node node = Composer.emptyBuilder()
                .module(readerSearch)
                .module(metrics)
                .environment(Map.of())
                .build()
                .boot(List.of("search", "metric"))) {
            ManagedChannel channel = NettyChannelBuilder
                    .forAddress("127.0.0.1", metrics.grpcPort()).usePlaintext().build();
            try {
                MetricServiceGrpc.MetricServiceBlockingStub stub =
                        MetricServiceGrpc.newBlockingStub(channel);
                QueryMetricsResponse counted = stub.queryMetrics(
                        QueryMetricsRequest.newBuilder()
                                .setMappingSubject(RepoDocumentMapping.SUBJECT)
                                .addMeasures("documents")
                                .setLimit(10)
                                .build());
                assertThat(counted.getRows(0).getMeasuresMap())
                        .containsEntry("documents", 3.0);

                QueryMetricsResponse byType = stub.queryMetrics(
                        QueryMetricsRequest.newBuilder()
                                .setMappingSubject(RepoDocumentMapping.SUBJECT)
                                .addMeasures("documents")
                                .addDimensions(MemberRef.newBuilder()
                                        .setName("document_type"))
                                .setLimit(10)
                                .build());
                assertThat(byType.getRowsList()).hasSize(2);
                assertThat(byType.getRows(1).getDimensionsMap())
                        .containsEntry("document_type", "PDF");
                assertThat(byType.getRows(1).getMeasuresMap())
                        .containsEntry("documents", 2.0);
            } finally {
                channel.shutdownNow();
            }
        }
    }
}
