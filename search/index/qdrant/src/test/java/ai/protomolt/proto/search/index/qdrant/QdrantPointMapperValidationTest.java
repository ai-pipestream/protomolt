package ai.protomolt.proto.search.index.qdrant;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mapper.MappingException;
import ai.protomolt.proto.mapper.ProtoFieldMapperImpl;
import ai.protomolt.proto.repo.semantic.v1.EmbeddingSet;
import ai.protomolt.proto.repo.semantic.v1.OffsetUnit;
import ai.protomolt.proto.repo.semantic.v1.Segment;
import ai.protomolt.proto.repo.semantic.v1.SegmentVector;
import ai.protomolt.proto.repo.semantic.v1.Segmentation;
import ai.protomolt.proto.repo.semantic.v1.SegmentationKind;
import ai.protomolt.proto.repo.semantic.v1.Similarity;
import ai.protomolt.proto.repo.semantic.v1.SourceTextBinding;
import ai.protomolt.proto.repo.semantic.v1.TextSource;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.validate.FieldRules;
import ai.protomolt.proto.validate.StringRules;
import ai.protomolt.proto.validate.ValidateProto;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import ai.protomolt.proto.search.index.spi.IndexFieldKind;
import ai.protomolt.proto.search.index.spi.IndexMapping;
import ai.protomolt.proto.search.index.spi.ResolvedFieldHint;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validation on write: {@link QdrantPointMapper#map} gates the source message on its
 * declared {@code ai.protomolt.proto.validate.v1} rules before producing points.
 */
class QdrantPointMapperValidationTest {

    private static final String DIGEST =
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    private final QdrantPointMapper mapper =
            new QdrantPointMapper(new ProtoFieldMapperImpl(new DescriptorRegistry()));

    private static final IndexMapping EMPTY_MAPPING =
            new IndexMapping("it.validate.Ruled", List.of());

    private static final IndexMapping DOC_MAPPING =
            new IndexMapping("ai.protomolt.proto.repo.v1.Document", List.of(
                    new IndexMapping.IndexedField("doc_id", "doc_id",
                            ResolvedFieldHint.of(IndexFieldKind.KEYWORD))));

    private static Descriptor ruledDescriptor() {
        try {
            FieldOptions options = FieldOptions.newBuilder()
                    .setExtension(ValidateProto.field, FieldRules.newBuilder()
                            .setString(StringRules.newBuilder().setMinLen(3))
                            .build())
                    .build();
            FileDescriptorProto file = FileDescriptorProto.newBuilder()
                    .setName("it/validate/ruled.proto")
                    .setPackage("it.validate")
                    .setSyntax("proto3")
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Ruled")
                            .addField(FieldDescriptorProto.newBuilder()
                                    .setName("name").setNumber(1)
                                    .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                    .setOptions(options)))
                    .build();
            return FileDescriptor.buildFrom(file, new FileDescriptor[]{
                    ValidateProto.getDescriptor(),
            }).findMessageTypeByName("Ruled");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    void violatingMessagesAreRejectedBeforeMapping() {
        Descriptor descriptor = ruledDescriptor();
        DynamicMessage invalid = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("name"), "ab")
                .build();

        assertThatThrownBy(() -> mapper.map(invalid, EMPTY_MAPPING))
                .isInstanceOf(ValidationResult.ValidationException.class)
                .hasMessageContaining("[name]");
    }

    @Test
    void validMessagesPassTheGate() throws Exception {
        Descriptor descriptor = ruledDescriptor();
        DynamicMessage valid = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("name"), "abc")
                .build();

        // Validation passed; only the Document-type check remains to fail.
        assertThatThrownBy(() -> mapper.map(valid, EMPTY_MAPPING))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("ai.protomolt.proto.repo.v1.Document");
    }

    @Test
    void ruleFreeDocumentsMapUnchanged() throws Exception {
        Document document = Document.newBuilder()
                .setDocId("doc-1")
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .addSegmentations(segmentation("rs-1", "c-1")))
                .build();

        assertThat(mapper.map(document, DOC_MAPPING)).isEmpty(); // no embeddings, but no rejection either
    }

    @Test
    void conflictingModelSimilaritiesAcrossSegmentationsAreRejected() {
        Document document = Document.newBuilder()
                .setDocId("doc-1")
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .addSegmentations(segmentation("rs-1", "c-1"))
                        .addSegmentations(segmentation("rs-2", "c-2"))
                        .addEmbeddingSets(EmbeddingSet.newBuilder()
                                .setEmbeddingSetId("es-1")
                                .setSegmentationId("rs-1")
                                .setSegmentationDigest(DIGEST)
                                .setModel("shared-model")
                                .setDimensions(2)
                                .setSimilarity(Similarity.SIMILARITY_COSINE)
                                .addVectors(SegmentVector.newBuilder()
                                        .setSegmentId("c-1")
                                        .addValues(0.1f).addValues(0.2f)))
                        .addEmbeddingSets(EmbeddingSet.newBuilder()
                                .setEmbeddingSetId("es-2")
                                .setSegmentationId("rs-2")
                                .setSegmentationDigest(DIGEST)
                                .setModel("shared-model")
                                .setDimensions(2)
                                .setSimilarity(Similarity.SIMILARITY_DOT_PRODUCT)
                                .addVectors(SegmentVector.newBuilder()
                                        .setSegmentId("c-2")
                                        .addValues(0.3f).addValues(0.4f))))
                .build();

        assertThatThrownBy(() -> mapper.map(document, DOC_MAPPING))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("conflicting similarities");
    }

    private static Segmentation segmentation(String segmentationId, String segmentId) {
        return Segmentation.newBuilder()
                .setSegmentationId(segmentationId)
                .setContentDigest(DIGEST)
                .setKind(SegmentationKind.SEGMENTATION_KIND_PARAGRAPH)
                .setOffsetUnit(OffsetUnit.OFFSET_UNIT_UTF8_BYTE)
                .setSource(TextSource.newBuilder()
                        .setFieldPath("search_metadata.body")
                        .setBinding(SourceTextBinding.newBuilder()
                                .setResolvedTextSha256(DIGEST)
                                .setUtf8Length(10)))
                .addSegments(Segment.newBuilder()
                        .setSegmentId(segmentId)
                        .setOrdinal(0)
                        .setText("chunk text")
                        .setStart(0)
                        .setEnd(10))
                .build();
    }
}
