package ai.protomolt.proto.search.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.search.chunk.SentencePackedChunker;
import ai.protomolt.proto.search.index.spi.CatalogIndexingHintSource;
import ai.protomolt.proto.search.index.spi.ChunkingPolicy;
import ai.protomolt.proto.search.index.spi.IndexFieldKind;
import ai.protomolt.proto.search.index.spi.IndexMapping;
import ai.protomolt.proto.search.index.spi.IndexMappingFactory;
import ai.protomolt.proto.search.index.spi.ResolvedFieldHint;
import ai.protomolt.proto.search.index.spi.VectorSimilarity;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.search.v1.SearchHit;
import ai.protomolt.proto.search.v1.SearchLane;
import ai.protomolt.proto.search.v1.SearchRequest;
import ai.protomolt.proto.search.v1.StoredValue;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import com.google.protobuf.TimestampProto;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The store's own guarantees: a failed mount releases every subject it
 * already opened, and concurrent replaces of one document converge to a
 * single block — the "replays never duplicate" contract under parallelism.
 */
class LuceneSearchStoreTest {

    @TempDir
    Path work;

    static ChunkingPolicy policyNaming(String providerId) {
        return new ChunkingPolicy(
                new ChunkingPolicy.ChunkingSpec(
                        SentencePackedChunker.STRATEGY, SentencePackedChunker.STRATEGY_VERSION,
                        12, 0, 2, 30, SentencePackedChunker.BOUNDARY),
                new ChunkingPolicy.EmbeddingSpec(
                        providerId, SearchTestProvider.DIMENSION,
                        VectorSimilarity.COSINE, true),
                "", true);
    }

    static Document document(String docId, String body) {
        return Document.newBuilder()
                .setDocId(docId)
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle("Title of " + docId)
                        .setBody(body))
                .build();
    }

    @Test
    void storedValuesCarryTheMappingsTypes() throws Exception {
        // A deliberately non-uniform fixture: every stored kind the mapper
        // writes, in one subject, so type coercion cannot hide behind a
        // strings-only mapping. Numeric and date cells were silently
        // dropped before stored values were typed.
        Descriptor order = orderDescriptor();
        CatalogIndexingHintSource catalog = new CatalogIndexingHintSource();
        catalog.put("test.Order", "id",
                ResolvedFieldHint.builder(IndexFieldKind.KEYWORD).stored(true).build());
        catalog.put("test.Order", "title",
                ResolvedFieldHint.builder(IndexFieldKind.TEXT).stored(true).build());
        catalog.put("test.Order", "amount",
                ResolvedFieldHint.builder(IndexFieldKind.INT64).stored(true).build());
        catalog.put("test.Order", "ratio",
                ResolvedFieldHint.builder(IndexFieldKind.DOUBLE).stored(true).build());
        catalog.put("test.Order", "paying",
                ResolvedFieldHint.builder(IndexFieldKind.BOOLEAN).stored(true).build());
        catalog.put("test.Order", "created_at",
                ResolvedFieldHint.builder(IndexFieldKind.DATE).stored(true).build());
        IndexMapping mapping = IndexMappingFactory.defaults(catalog).create(order);
        ServedMapping served = new ServedMapping(mapping, "id",
                message -> (String) message.getField(order.findFieldByName("id")), null);
        try (LuceneSearchStore store = new LuceneSearchStore(work, Map.of("orders", served))) {
            store.index("orders", DynamicMessage.newBuilder(order)
                    .setField(order.findFieldByName("id"), "order-1")
                    .setField(order.findFieldByName("title"), "Autumn ledger of gears")
                    .setField(order.findFieldByName("amount"), 4200L)
                    .setField(order.findFieldByName("ratio"), 0.5d)
                    .setField(order.findFieldByName("paying"), true)
                    .setField(order.findFieldByName("created_at"),
                            Timestamp.newBuilder().setSeconds(1_700_000_000L).build())
                    .build());
            List<SearchHit> hits = store.search("orders", SearchRequest.newBuilder()
                    .setMappingSubject("orders")
                    .setQuery("autumn ledger")
                    .setK(3)
                    .setLane(SearchLane.SEARCH_LANE_LEXICAL)
                    .build());
            assertThat(hits).hasSize(1);
            Map<String, StoredValue> stored = hits.get(0).getStoredMap();
            assertThat(stored.get("id").getStringValue()).isEqualTo("order-1");
            assertThat(stored.get("title").getStringValue())
                    .isEqualTo("Autumn ledger of gears");
            assertThat(stored.get("amount").getInt64Value()).isEqualTo(4200L);
            assertThat(stored.get("ratio").getDoubleValue()).isEqualTo(0.5d);
            assertThat(stored.get("paying").getBoolValue()).isTrue();
            assertThat(stored.get("created_at").getTimestampValue())
                    .isEqualTo(Timestamp.newBuilder().setSeconds(1_700_000_000L).build());
        }
    }

    /** {@code test.Order}: one field per stored kind, built dynamically. */
    static Descriptor orderDescriptor() throws Exception {
        DescriptorProto.Builder message = DescriptorProto.newBuilder().setName("Order");
        message.addField(scalar("id", 1, FieldDescriptorProto.Type.TYPE_STRING));
        message.addField(scalar("title", 2, FieldDescriptorProto.Type.TYPE_STRING));
        message.addField(scalar("amount", 3, FieldDescriptorProto.Type.TYPE_INT64));
        message.addField(scalar("ratio", 4, FieldDescriptorProto.Type.TYPE_DOUBLE));
        message.addField(scalar("paying", 5, FieldDescriptorProto.Type.TYPE_BOOL));
        message.addField(FieldDescriptorProto.newBuilder()
                .setName("created_at").setNumber(6)
                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setTypeName(".google.protobuf.Timestamp")
                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL));
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("test/order.proto")
                .setPackage("test")
                .setSyntax("proto3")
                .addDependency("google/protobuf/timestamp.proto")
                .addMessageType(message)
                .build();
        return FileDescriptor
                .buildFrom(file, new FileDescriptor[] {TimestampProto.getDescriptor()})
                .findMessageTypeByName("Order");
    }

    static FieldDescriptorProto.Builder scalar(
            String name, int number, FieldDescriptorProto.Type type) {
        return FieldDescriptorProto.newBuilder()
                .setName(name).setNumber(number).setType(type)
                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL);
    }

    @Test
    void aFailedMountReleasesTheSubjectsItAlreadyOpened() {
        // Mount order matters: the sound subject opens first, the subject
        // naming an absent embedding provider fails the mount second.
        Map<String, ServedMapping> subjects = new LinkedHashMap<>();
        subjects.put("sound", RepoDocumentMapping.served());
        subjects.put("broken", RepoDocumentMapping.served(policyNaming("no-such-provider")));
        assertThatThrownBy(() -> new LuceneSearchStore(work, subjects, null, false,
                RepoDocumentMapping.laneVectorization()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no-such-provider");

        // The sound subject's writer did not leak its write lock: the same
        // directory mounts cleanly on the next attempt.
        try (LuceneSearchStore store =
                new LuceneSearchStore(work, Map.of("sound", RepoDocumentMapping.served()))) {
            assertThat(store.subjectNames()).containsExactly("sound");
        }
    }

    @Test
    void deletingADocumentRemovesItsWholeBlockAndLeavesOthers() {
        try (LuceneSearchStore store = new LuceneSearchStore(work, Map.of(
                RepoDocumentMapping.SUBJECT,
                RepoDocumentMapping.served(policyNaming(SearchTestProvider.PROVIDER_ID))),
                null, false, RepoDocumentMapping.laneVectorization())) {
            store.index(RepoDocumentMapping.SUBJECT,
                    document("doc-keep", "The evergreen anchor stays behind."));
            LuceneSearchStore.IndexResult landed = store.index(RepoDocumentMapping.SUBJECT,
                    document("doc-gone", "The evergreen anchor leaves town."));
            assertThat(store.indexedDocIds(RepoDocumentMapping.SUBJECT))
                    .containsExactlyInAnyOrder("doc-keep", "doc-gone");

            // The delete reports exactly the chunk children the index call
            // landed, so a caller can tell a real removal from a no-op.
            assertThat(store.delete(RepoDocumentMapping.SUBJECT, "doc-gone"))
                    .isEqualTo(landed.chunksIndexed());

            // The enumeration respects live docs: the deleted block is out
            // even though no merge has reclaimed its terms yet.
            assertThat(store.indexedDocIds(RepoDocumentMapping.SUBJECT))
                    .containsExactly("doc-keep");

            List<SearchHit> lexical = store.search(RepoDocumentMapping.SUBJECT,
                    SearchRequest.newBuilder()
                            .setMappingSubject(RepoDocumentMapping.SUBJECT)
                            .setQuery("evergreen anchor")
                            .setK(10)
                            .setLane(SearchLane.SEARCH_LANE_LEXICAL)
                            .build());
            assertThat(lexical).extracting(SearchHit::getDocId).containsExactly("doc-keep");

            // The chunk children went with the parent: the deleted
            // document's own sentence no longer answers the vector lane.
            List<SearchHit> vector = store.search(RepoDocumentMapping.SUBJECT,
                    SearchRequest.newBuilder()
                            .setMappingSubject(RepoDocumentMapping.SUBJECT)
                            .setQuery("The evergreen anchor leaves town.")
                            .setK(10)
                            .setLane(SearchLane.SEARCH_LANE_VECTOR)
                            .build());
            assertThat(vector).extracting(SearchHit::getDocId)
                    .doesNotContain("doc-gone");
        }
    }

    @Test
    void writesAreVisibleImmediatelyAndDurableAcrossReopen() {
        // Fewer writes than a commit batch: visibility rides the
        // near-real-time searcher, durability rides the close commit.
        SearchRequest query = SearchRequest.newBuilder()
                .setMappingSubject(RepoDocumentMapping.SUBJECT)
                .setQuery("durable phrase")
                .setK(3)
                .setLane(SearchLane.SEARCH_LANE_LEXICAL)
                .build();
        try (LuceneSearchStore store = new LuceneSearchStore(work,
                Map.of(RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served()))) {
            store.index(RepoDocumentMapping.SUBJECT,
                    document("doc-d", "the durable phrase survives"));
            assertThat(store.search(RepoDocumentMapping.SUBJECT, query)).hasSize(1);
        }
        try (LuceneSearchStore reopened = new LuceneSearchStore(work,
                Map.of(RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served()))) {
            assertThat(reopened.search(RepoDocumentMapping.SUBJECT, query)).hasSize(1);
        }
    }

    @Test
    void deletingAnAbsentIdSucceedsAndRefusalsNameTheProblem() {
        try (LuceneSearchStore store = new LuceneSearchStore(work,
                Map.of(RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served()))) {
            // Idempotent: an id the index does not hold is already gone,
            // and the count says nothing was removed.
            assertThat(store.delete(RepoDocumentMapping.SUBJECT, "never-indexed")).isZero();

            assertThatThrownBy(() -> store.delete("no-such-subject", "doc-x"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("no-such-subject")
                    .hasMessageContaining(RepoDocumentMapping.SUBJECT);
            assertThatThrownBy(() -> store.delete(RepoDocumentMapping.SUBJECT, "  "))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("doc_id");
        }
    }

    @Test
    void concurrentReplacesOfOneDocumentNeverDuplicate() throws Exception {
        try (LuceneSearchStore store = new LuceneSearchStore(work, Map.of(
                RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served()))) {
            int workers = 8;
            int rounds = 20;
            List<Future<?>> writes = new ArrayList<>();
            try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
                for (int w = 0; w < workers; w++) {
                    int worker = w;
                    writes.add(pool.submit(() -> {
                        for (int round = 0; round < rounds; round++) {
                            store.index(RepoDocumentMapping.SUBJECT,
                                    document("doc-c", "the shared anchor phrase, revision "
                                            + worker + "." + round));
                        }
                    }));
                }
                for (Future<?> write : writes) {
                    write.get();
                }
            }

            List<SearchHit> hits = store.search(RepoDocumentMapping.SUBJECT,
                    SearchRequest.newBuilder()
                            .setMappingSubject(RepoDocumentMapping.SUBJECT)
                            .setQuery("shared anchor")
                            .setK(workers * rounds)
                            .setLane(SearchLane.SEARCH_LANE_LEXICAL)
                            .build());
            // However the replaces interleaved, exactly one block survives.
            assertThat(hits).hasSize(1);
            assertThat(hits.getFirst().getDocId()).isEqualTo("doc-c");
        }
    }
}
