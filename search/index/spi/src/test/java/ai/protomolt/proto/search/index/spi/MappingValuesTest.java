package ai.protomolt.proto.search.index.spi;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mapper.MappingException;
import ai.protomolt.proto.mapper.ProtoFieldMapper;
import ai.protomolt.proto.mapper.ProtoFieldMapperImpl;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.MessageOptions;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MappingValuesTest {

    @Test
    void singularPathsKeepTheMapperSemantics() throws Exception {
        Fixtures f = Fixtures.create();
        DynamicMessage doc = DynamicMessage.newBuilder(f.doc)
                .setField(f.doc.findFieldByName("doc_id"), "d1")
                .build();

        assertThat(MappingValues.read(f.mapper, doc, "doc_id", false)).isEqualTo("d1");
        // unset singular message parent: missing, not a mapping error
        assertThat(MappingValues.read(f.mapper, doc, "solo.label", false)).isNull();
    }

    @Test
    void repeatedIntermediateFansOutAcrossElements() throws Exception {
        Fixtures f = Fixtures.create();
        DynamicMessage doc = f.docWithChunks(f.chunk("alpha"), f.chunk("beta"));

        Object value = MappingValues.read(f.mapper, doc, "chunks.text", false);

        assertThat(value).isEqualTo(List.of("alpha", "beta"));
    }

    @Test
    void nestedFanOutFlattensDepthFirst() throws Exception {
        Fixtures f = Fixtures.create();
        DynamicMessage groupA = DynamicMessage.newBuilder(f.group)
                .addRepeatedField(f.group.findFieldByName("chunks"), f.chunk("a1"))
                .addRepeatedField(f.group.findFieldByName("chunks"), f.chunk("a2"))
                .build();
        DynamicMessage groupB = DynamicMessage.newBuilder(f.group)
                .addRepeatedField(f.group.findFieldByName("chunks"), f.chunk("b1"))
                .build();
        DynamicMessage doc = DynamicMessage.newBuilder(f.doc)
                .addRepeatedField(f.doc.findFieldByName("groups"), groupA)
                .addRepeatedField(f.doc.findFieldByName("groups"), groupB)
                .build();

        assertThat(MappingValues.read(f.mapper, doc, "groups.chunks.text", false))
                .isEqualTo(List.of("a1", "a2", "b1"));
    }

    @Test
    void repeatedLeafUnderRepeatedAncestorFlattens() throws Exception {
        Fixtures f = Fixtures.create();
        DynamicMessage first = DynamicMessage.newBuilder(f.chunk)
                .addRepeatedField(f.chunk.findFieldByName("tags"), "t1")
                .addRepeatedField(f.chunk.findFieldByName("tags"), "t2")
                .build();
        DynamicMessage second = DynamicMessage.newBuilder(f.chunk)
                .addRepeatedField(f.chunk.findFieldByName("tags"), "t3")
                .build();
        DynamicMessage doc = f.docWithChunks(first, second);

        assertThat(MappingValues.read(f.mapper, doc, "chunks.tags", false))
                .isEqualTo(List.of("t1", "t2", "t3"));
    }

    @Test
    void emptyRepeatedIntermediateReadsAsMissing() throws Exception {
        Fixtures f = Fixtures.create();
        DynamicMessage doc = DynamicMessage.newBuilder(f.doc)
                .setField(f.doc.findFieldByName("doc_id"), "d1")
                .build();

        assertThat(MappingValues.read(f.mapper, doc, "chunks.text", false)).isNull();
    }

    @Test
    void elementsWithUnsetSingularParentsContributeNothing() throws Exception {
        Fixtures f = Fixtures.create();
        DynamicMessage withMeta = DynamicMessage.newBuilder(f.chunk)
                .setField(f.chunk.findFieldByName("meta"), DynamicMessage.newBuilder(f.inner)
                        .setField(f.inner.findFieldByName("label"), "L1")
                        .build())
                .build();
        DynamicMessage doc = f.docWithChunks(withMeta, f.chunk("no meta"));

        assertThat(MappingValues.read(f.mapper, doc, "chunks.meta.label", false))
                .isEqualTo(List.of("L1"));
    }

    @Test
    void includeDefaultsAppliesPerElement() throws Exception {
        Fixtures f = Fixtures.create();
        DynamicMessage doc = f.docWithChunks(
                f.chunk("alpha"), DynamicMessage.newBuilder(f.chunk).build());

        assertThat(MappingValues.read(f.mapper, doc, "chunks.text", false))
                .isEqualTo(List.of("alpha"));
        assertThat(MappingValues.read(f.mapper, doc, "chunks.text", true))
                .isEqualTo(List.of("alpha", ""));
    }

    @Test
    void unknownFieldBelowAFanOutStillFailsLoudly() throws Exception {
        Fixtures f = Fixtures.create();
        DynamicMessage doc = f.docWithChunks(f.chunk("alpha"));

        assertThatThrownBy(() -> MappingValues.read(f.mapper, doc, "chunks.missing", false))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("missing");
    }

    @Test
    void readWholeKeepsTheMapperSemanticsOnSingularPaths() throws Exception {
        Fixtures f = Fixtures.create();
        DynamicMessage doc = DynamicMessage.newBuilder(f.doc)
                .setField(f.doc.findFieldByName("doc_id"), "d1")
                .build();

        assertThat(MappingValues.readWhole(f.mapper, doc, "doc_id", false)).isEqualTo("d1");
        assertThat(MappingValues.readWhole(f.mapper, doc, "solo.label", false)).isNull();
    }

    @Test
    void readWholeRefusesFanOutPathsRegardlessOfElementCount() throws Exception {
        Fixtures f = Fixtures.create();
        DynamicMessage populated = f.docWithChunks(f.chunk("alpha"), f.chunk("beta"));
        DynamicMessage empty = DynamicMessage.newBuilder(f.doc)
                .setField(f.doc.findFieldByName("doc_id"), "d1")
                .build();

        // Validity is a property of the path, not of this document's element count.
        assertThatThrownBy(() -> MappingValues.readWhole(f.mapper, populated, "chunks.text", false))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("whole value");
        assertThatThrownBy(() -> MappingValues.readWhole(f.mapper, empty, "chunks.text", false))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("whole value");
    }

    @Test
    void mapIntermediatesStayWithTheMapperError() throws Exception {
        Fixtures f = Fixtures.create();
        Descriptor entry = f.doc.findNestedTypeByName("AttrsEntry");
        DynamicMessage doc = DynamicMessage.newBuilder(f.doc)
                .addRepeatedField(f.doc.findFieldByName("attrs"), DynamicMessage.newBuilder(entry)
                        .setField(entry.findFieldByName("key"), "k")
                        .setField(entry.findFieldByName("value"), "v")
                        .build())
                .build();

        assertThatThrownBy(() -> MappingValues.read(f.mapper, doc, "attrs.k", false))
                .isInstanceOf(MappingException.class);
    }

    private record Fixtures(
            Descriptor doc,
            Descriptor chunk,
            Descriptor inner,
            Descriptor group,
            ProtoFieldMapper mapper) {

        static Fixtures create() throws Exception {
            FileDescriptor file = docFile();
            return new Fixtures(
                    file.findMessageTypeByName("Doc"),
                    file.findMessageTypeByName("Chunk"),
                    file.findMessageTypeByName("Inner"),
                    file.findMessageTypeByName("Group"),
                    new ProtoFieldMapperImpl(new DescriptorRegistry()));
        }

        DynamicMessage chunk(String text) {
            return DynamicMessage.newBuilder(chunk)
                    .setField(chunk.findFieldByName("text"), text)
                    .build();
        }

        DynamicMessage docWithChunks(DynamicMessage... chunks) {
            DynamicMessage.Builder builder = DynamicMessage.newBuilder(doc)
                    .setField(doc.findFieldByName("doc_id"), "d1");
            for (DynamicMessage element : chunks) {
                builder.addRepeatedField(doc.findFieldByName("chunks"), element);
            }
            return builder.build();
        }

        private static FileDescriptor docFile() throws Exception {
            FileDescriptorProto proto = FileDescriptorProto.newBuilder()
                    .setName("mapping_values_doc.proto")
                    .setPackage("ai.protomolt.test.mappingvalues")
                    .setSyntax("proto3")
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Inner")
                            .addField(stringField("label", 1, FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Chunk")
                            .addField(stringField("text", 1, FieldDescriptorProto.Label.LABEL_OPTIONAL))
                            .addField(stringField("tags", 2, FieldDescriptorProto.Label.LABEL_REPEATED))
                            .addField(messageField("meta", 3, ".ai.protomolt.test.mappingvalues.Inner",
                                    FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Group")
                            .addField(messageField("chunks", 1, ".ai.protomolt.test.mappingvalues.Chunk",
                                    FieldDescriptorProto.Label.LABEL_REPEATED)))
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Doc")
                            .addField(stringField("doc_id", 1, FieldDescriptorProto.Label.LABEL_OPTIONAL))
                            .addField(messageField("chunks", 2, ".ai.protomolt.test.mappingvalues.Chunk",
                                    FieldDescriptorProto.Label.LABEL_REPEATED))
                            .addField(messageField("solo", 3, ".ai.protomolt.test.mappingvalues.Inner",
                                    FieldDescriptorProto.Label.LABEL_OPTIONAL))
                            .addField(messageField("attrs", 4, ".ai.protomolt.test.mappingvalues.Doc.AttrsEntry",
                                    FieldDescriptorProto.Label.LABEL_REPEATED))
                            .addField(messageField("groups", 5, ".ai.protomolt.test.mappingvalues.Group",
                                    FieldDescriptorProto.Label.LABEL_REPEATED))
                            .addNestedType(DescriptorProto.newBuilder()
                                    .setName("AttrsEntry")
                                    .setOptions(MessageOptions.newBuilder().setMapEntry(true))
                                    .addField(stringField("key", 1, FieldDescriptorProto.Label.LABEL_OPTIONAL))
                                    .addField(stringField("value", 2, FieldDescriptorProto.Label.LABEL_OPTIONAL))))
                    .build();
            return FileDescriptor.buildFrom(proto, new FileDescriptor[]{});
        }

        private static FieldDescriptorProto.Builder stringField(
                String name, int number, FieldDescriptorProto.Label label) {
            return FieldDescriptorProto.newBuilder()
                    .setName(name)
                    .setNumber(number)
                    .setType(FieldDescriptorProto.Type.TYPE_STRING)
                    .setLabel(label);
        }

        private static FieldDescriptorProto.Builder messageField(
                String name, int number, String typeName, FieldDescriptorProto.Label label) {
            return FieldDescriptorProto.newBuilder()
                    .setName(name)
                    .setNumber(number)
                    .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                    .setTypeName(typeName)
                    .setLabel(label);
        }
    }
}
