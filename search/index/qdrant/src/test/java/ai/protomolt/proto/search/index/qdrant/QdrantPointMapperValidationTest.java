package ai.protomolt.proto.search.index.qdrant;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mapper.MappingException;
import ai.protomolt.proto.mapper.ProtoFieldMapperImpl;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v1.SemanticChunk;
import ai.protomolt.proto.repo.v1.SemanticProcessingResult;
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
 * declared {@code ai.pipestream.proto.validate.v1} rules before producing points. The repo
 * Document declares no rules today, so the gate is exercised with a ruled dynamic message:
 * a violating message is rejected by the validator, a valid one sails past validation and
 * fails only at the Document-type check — proving the gate ran first.
 */
class QdrantPointMapperValidationTest {

    private final QdrantPointMapper mapper =
            new QdrantPointMapper(new ProtoFieldMapperImpl(new DescriptorRegistry()));

    private static final IndexMapping EMPTY_MAPPING =
            new IndexMapping("it.validate.Ruled", List.of());

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
                .hasMessageContaining("ai.pipestream.proto.repo.v1.Document");
    }

    @Test
    void ruleFreeDocumentsMapUnchanged() throws Exception {
        // The repo Document declares no validation rules: the gate is a pass-through.
        Document document = Document.newBuilder()
                .setDocId("doc-1")
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .addSemanticResults(SemanticProcessingResult.newBuilder()
                                .setResultId("rs-1")
                                .addChunks(SemanticChunk.newBuilder()
                                        .setChunkId("c-1")
                                        .setChunkNumber(0)
                                        .setText("chunk text"))))
                .build();
        IndexMapping mapping = new IndexMapping("ai.pipestream.proto.repo.v1.Document", List.of(
                new IndexMapping.IndexedField("doc_id", "doc_id",
                        ResolvedFieldHint.of(IndexFieldKind.KEYWORD))));

        assertThat(mapper.map(document, mapping)).isEmpty(); // no embeddings, but no rejection either
    }
}
