package ai.protomolt.proto.actions;

import ai.protomolt.proto.search.index.hints.FieldIndexHint;
import ai.protomolt.proto.search.index.hints.IndexFieldType;
import ai.protomolt.proto.search.index.hints.IndexingHintsProto;
import ai.protomolt.proto.meta.FieldMeta;
import ai.protomolt.proto.meta.MessageMeta;
import ai.protomolt.proto.meta.MetadataProto;
import ai.protomolt.proto.validate.FieldRules;
import ai.protomolt.proto.validate.Int32Rules;
import ai.protomolt.proto.validate.StringRules;
import ai.protomolt.proto.validate.ValidateProto;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.MessageOptions;
import com.google.protobuf.Descriptors.FileDescriptor;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/** Shared fixtures: programmatic option-annotated descriptors and inline proto sources. */
final class TestFixtures {

    static final ObjectMapper MAPPER = new ObjectMapper();

    /** A two-field document type for inline-source tests. */
    static final String DOC_PROTO = """
            syntax = "proto3";
            package t;
            message Doc {
              string title = 1;
              string alt = 2;
            }
            """;

    private TestFixtures() {
    }

    static ObjectNode obj(String json) {
        try {
            return (ObjectNode) MAPPER.readTree(json);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** A context whose registry knows {@code actions.test.Person} (and the well-known types). */
    static ActionContext personContext() {
        ActionContext context = ActionContext.create();
        context.registry().registerFile(personFile());
        return context;
    }

    /**
     * {@code actions.test.Person}: name (string, min_len 3, pii metadata), age (int32, gte 0),
     * nickname (string, unconstrained); message-level metadata owner/description.
     */
    static FileDescriptor personFile() {
        FieldOptions nameOptions = FieldOptions.newBuilder()
                .setExtension(ValidateProto.field, FieldRules.newBuilder()
                        .setString(StringRules.newBuilder().setMinLen(3))
                        .build())
                .setExtension(MetadataProto.field, FieldMeta.newBuilder()
                        .setDescription("Full name")
                        .setSensitivity("pii")
                        .build())
                .build();
        FieldOptions ageOptions = FieldOptions.newBuilder()
                .setExtension(ValidateProto.field, FieldRules.newBuilder()
                        .setInt32(Int32Rules.newBuilder().setGte(0))
                        .build())
                .build();
        MessageOptions personOptions = MessageOptions.newBuilder()
                .setExtension(MetadataProto.message, MessageMeta.newBuilder()
                        .setDescription("A person")
                        .setOwner("identity-team")
                        .build())
                .build();
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("actions/person.proto")
                .setPackage("actions.test")
                .setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Person")
                        .setOptions(personOptions)
                        .addField(field("name", 1, FieldDescriptorProto.Type.TYPE_STRING, nameOptions))
                        .addField(field("age", 2, FieldDescriptorProto.Type.TYPE_INT32, ageOptions))
                        .addField(field("nickname", 3, FieldDescriptorProto.Type.TYPE_STRING, null)))
                .build();
        return build(file);
    }

    /**
     * {@code actions.test.HintedDoc}: title (TEXT, english analyzer, keyword subfield 'raw'),
     * id (KEYWORD, sortable), count (int32, no hint — inference applies), embedding (VECTOR,
     * 4 dims, L2).
     */
    static FileDescriptor hintedFile() {
        FieldOptions titleOptions = FieldOptions.newBuilder()
                .setExtension(IndexingHintsProto.index, FieldIndexHint.newBuilder()
                        .setType(IndexFieldType.INDEX_FIELD_TYPE_TEXT)
                        .setAnalyzer("english")
                        .addSubFields(ai.protomolt.proto.search.index.hints.SubFieldHint.newBuilder()
                                .setType(IndexFieldType.INDEX_FIELD_TYPE_KEYWORD)
                                .setName("raw"))
                        .build())
                .setExtension(ai.protomolt.proto.meta.MetadataProto.field,
                        ai.protomolt.proto.meta.FieldMeta.newBuilder()
                                .setSensitivity("pii").build())
                .build();
        FieldOptions idOptions = FieldOptions.newBuilder()
                .setExtension(IndexingHintsProto.index, FieldIndexHint.newBuilder()
                        .setType(IndexFieldType.INDEX_FIELD_TYPE_KEYWORD)
                        .setSortable(true)
                        .build())
                .build();
        FieldOptions embeddingOptions = FieldOptions.newBuilder()
                .setExtension(IndexingHintsProto.index, FieldIndexHint.newBuilder()
                        .setType(IndexFieldType.INDEX_FIELD_TYPE_VECTOR)
                        .setVectorDims(4)
                        .setVectorSimilarity(
                                ai.protomolt.proto.search.index.hints.VectorSimilarity.VECTOR_SIMILARITY_L2)
                        .build())
                .build();
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("actions/hinted.proto")
                .setPackage("actions.test")
                .setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("HintedDoc")
                        .addField(field("title", 1, FieldDescriptorProto.Type.TYPE_STRING, titleOptions))
                        .addField(field("id", 2, FieldDescriptorProto.Type.TYPE_STRING, idOptions))
                        .addField(field("count", 3, FieldDescriptorProto.Type.TYPE_INT32, null))
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("embedding").setNumber(4)
                                .setType(FieldDescriptorProto.Type.TYPE_FLOAT)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)
                                .setOptions(embeddingOptions)))
                .build();
        return build(file);
    }

    /**
     * {@code actions.test.DimensionlessVectorDoc}: embedding hinted VECTOR with no
     * vector_dims — the shape the Qdrant schema renderer rejects by field name.
     */
    static FileDescriptor dimensionlessVectorFile() {
        FieldOptions embeddingOptions = FieldOptions.newBuilder()
                .setExtension(IndexingHintsProto.index, FieldIndexHint.newBuilder()
                        .setType(IndexFieldType.INDEX_FIELD_TYPE_VECTOR)
                        .build())
                .build();
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("actions/dimensionless_vector.proto")
                .setPackage("actions.test")
                .setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("DimensionlessVectorDoc")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("embedding").setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_FLOAT)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)
                                .setOptions(embeddingOptions)))
                .build();
        return build(file);
    }

    /** The real validation option file, read from the validation module's classpath resource. */
    static String validateProtoSource() {
        try (InputStream in = TestFixtures.class.getResourceAsStream(
                "/ai/pipestream/proto/validate/v1/validate.proto")) {
            if (in == null) {
                throw new IllegalStateException("validate.proto not found on the test classpath");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static FieldDescriptorProto field(
            String name, int number, FieldDescriptorProto.Type type, FieldOptions options) {
        FieldDescriptorProto.Builder builder = FieldDescriptorProto.newBuilder()
                .setName(name)
                .setNumber(number)
                .setType(type)
                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL);
        if (options != null) {
            builder.setOptions(options);
        }
        return builder.build();
    }

    private static FileDescriptor build(FileDescriptorProto file) {
        try {
            return FileDescriptor.buildFrom(file, new FileDescriptor[0]);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
