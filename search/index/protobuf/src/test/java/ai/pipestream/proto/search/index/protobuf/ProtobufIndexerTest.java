package ai.pipestream.proto.search.index.protobuf;

import ai.pipestream.proto.search.index.spi.IndexFieldKind;
import ai.pipestream.proto.search.index.spi.IndexMapping;
import ai.pipestream.proto.search.index.spi.ResolvedFieldHint;
import ai.pipestream.proto.search.index.spi.VectorSimilarity;
import ai.pipestream.proto.indexing.testdata.IndexableDoc;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProtobufIndexerTest {

    @Test
    void buildsMappingFromIndexingHints() {
        ProtobufIndexer indexer = ProtobufIndexer.defaults(null);
        IndexMapping mapping = indexer.mapping(IndexableDoc.getDescriptor());

        assertThat(mapping.find("doc_id")).get().extracting(f -> f.type())
                .isEqualTo(IndexFieldKind.KEYWORD);
        assertThat(mapping.find("title")).get().extracting(f -> f.type())
                .isEqualTo(IndexFieldKind.TEXT);
        assertThat(mapping.find("page_count")).get().extracting(f -> f.type())
                .isEqualTo(IndexFieldKind.INT32);
    }

    @Test
    void mappingCarriesRichHintsFromCompiledOptions() {
        ProtobufIndexer indexer = ProtobufIndexer.defaults(null);
        IndexMapping mapping = indexer.mapping(IndexableDoc.getDescriptor());

        ResolvedFieldHint embedding = mapping.find("embedding").orElseThrow().hint();
        assertThat(embedding.type()).isEqualTo(IndexFieldKind.VECTOR);
        assertThat(embedding.vectorDims()).isEqualTo(3);
        assertThat(embedding.vectorSimilarity()).isEqualTo(VectorSimilarity.DOT_PRODUCT);
        assertThat(embedding.hnswParams()).isEqualTo(new ResolvedFieldHint.HnswParams(16, 100));
        assertThat(embedding.engineParams("opensearch")).containsEntry("mode", "on_disk");

        ResolvedFieldHint category = mapping.find("category").orElseThrow().hint();
        assertThat(category.type()).isEqualTo(IndexFieldKind.TEXT);
        assertThat(category.analyzerOverride()).contains("english");
        assertThat(category.sortable()).isTrue();
        assertThat(category.nullValue()).isEqualTo("uncategorized");
        assertThat(category.subFields()).containsExactly(
                new ResolvedFieldHint.SubField(IndexFieldKind.KEYWORD, "raw", ""));
    }

    @Test
    void rangeHintResolvesBoundsFromCompiledMessage() {
        ProtobufIndexer indexer = ProtobufIndexer.defaults(null);
        IndexMapping mapping = indexer.mapping(IndexableDoc.getDescriptor());

        // the range field stays one entry (never expanded into page_span.gte / page_span.lte)
        assertThat(mapping.find("page_span")).get().extracting(IndexMapping.IndexedField::type)
                .isEqualTo(IndexFieldKind.INT_RANGE);
        assertThat(mapping.find("page_span.gte")).isEmpty();
    }

    @Test
    void writesNdjsonWithoutValidation() throws Exception {
        ProtobufIndexer indexer = ProtobufIndexer.create();
        IndexableDoc doc = IndexableDoc.newBuilder()
                .setDocId("d1")
                .setTitle("Hello")
                .setPageCount(3)
                .build();

        String line = indexer.toNdjsonLine(doc);
        assertThat(line).contains("\"doc_id\":\"d1\"").contains("\"title\":\"Hello\"");
    }

    @Test
    void chainsValidationBeforeNdjson() throws Exception {
        ProtoValidator validator = ProtoValidator.forMessageType(IndexableDoc.getDescriptor());
        ProtobufIndexer indexer = ProtobufIndexer.defaults(validator);

        IndexableDoc bad = IndexableDoc.newBuilder().setTitle("x").setPageCount(-1).build();
        assertThatThrownBy(() -> indexer.toNdjsonLine(bad))
                .isInstanceOf(ValidationResult.ValidationException.class);

        IndexableDoc ok = IndexableDoc.newBuilder()
                .setDocId("d1")
                .setTitle("Hello")
                .setPageCount(2)
                .build();
        assertThat(indexer.validate(ok).valid()).isTrue();
        assertThat(indexer.toNdjsonLine(ok)).contains("\"doc_id\":\"d1\"");
    }

    @Test
    void writeBulkIndexProducesTwoLines() throws Exception {
        ProtoValidator validator = ProtoValidator.forMessageType(IndexableDoc.getDescriptor());
        ProtobufIndexer indexer = ProtobufIndexer.defaults(validator);
        IndexableDoc doc = IndexableDoc.newBuilder()
                .setDocId("d1")
                .setTitle("Hello")
                .setPageCount(1)
                .build();

        StringBuilder out = new StringBuilder();
        indexer.writeBulkIndex(out, "docs", "d1", doc);
        String[] lines = out.toString().split("\n");
        assertThat(lines).hasSize(2);
        assertThat(lines[0]).contains("\"_index\":\"docs\"").contains("\"_id\":\"d1\"");
        assertThat(lines[1]).contains("\"title\":\"Hello\"");
    }
}
