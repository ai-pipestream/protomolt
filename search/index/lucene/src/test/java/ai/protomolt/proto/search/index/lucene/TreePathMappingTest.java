package ai.protomolt.proto.search.index.lucene;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.search.index.spi.IndexFieldKind;
import ai.protomolt.proto.search.index.spi.IndexMapping;
import ai.protomolt.proto.search.index.spi.ResolvedFieldHint;
import ai.protomolt.proto.mapper.ProtoFieldMapperImpl;
import ai.protomolt.proto.types.TreePath;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * TreePath through the Lucene mapper: each path value emits its ancestor
 * chain as exact terms, docValues always take the SORTED_SET form (the
 * chain is multi-valued by construction), and stored keeps the complete
 * path only — ancestors are derivable.
 */
class TreePathMappingTest {

    private final ProtoLuceneMapper mapper =
            new ProtoLuceneMapper(new ProtoFieldMapperImpl(new DescriptorRegistry()));

    private static final TreePath AUDIO = TreePath.newBuilder()
            .addSegments("electronics").addSegments("audio").addSegments("headphones").build();

    @Test
    void aPathEmitsItsAncestorChainAsTerms() throws Exception {
        Document doc = map(ResolvedFieldHint.builder(IndexFieldKind.TREE_PATH)
                .facetable(true).build(), AUDIO);
        assertThat(indexedValues(doc))
                .containsExactly("electronics", "electronics/audio",
                        "electronics/audio/headphones");
    }

    @Test
    void docValuesAlwaysTakeTheSortedSetForm() throws Exception {
        Document facetable = map(ResolvedFieldHint.builder(IndexFieldKind.TREE_PATH)
                .facetable(true).build(), AUDIO);
        assertThat(docValues(facetable)).hasSize(3)
                .allMatch(f -> f.fieldType().docValuesType() == DocValuesType.SORTED_SET);

        // Sortable-only would be a single-valued form elsewhere, but the ancestor
        // chain is multi-valued by construction.
        Document sortable = map(ResolvedFieldHint.builder(IndexFieldKind.TREE_PATH)
                .sortable(true).build(), AUDIO);
        assertThat(docValues(sortable)).hasSize(3)
                .allMatch(f -> f.fieldType().docValuesType() == DocValuesType.SORTED_SET);
    }

    @Test
    void storedKeepsTheCompletePathOnly() throws Exception {
        Document doc = map(ResolvedFieldHint.builder(IndexFieldKind.TREE_PATH)
                .facetable(true).build(), AUDIO);
        assertThat(doc.getFields("category").length).isGreaterThan(0);
        assertThat(storedValues(doc)).containsExactly("electronics/audio/headphones");
    }

    @Test
    void repeatedPathsMergeTheirChains() throws Exception {
        TreePath second = TreePath.newBuilder()
                .addSegments("clearance").addSegments("audio").build();
        Descriptor docType = docDescriptor(true);
        var field = docType.findFieldByName("category");
        DynamicMessage message = DynamicMessage.newBuilder(docType)
                .addRepeatedField(field, AUDIO)
                .addRepeatedField(field, second)
                .build();
        Document doc = mapper.map(message, mapping(
                ResolvedFieldHint.builder(IndexFieldKind.TREE_PATH).facetable(true).build(),
                true));
        assertThat(indexedValues(doc)).containsExactly(
                "electronics", "electronics/audio", "electronics/audio/headphones",
                "clearance", "clearance/audio");
    }

    @Test
    void fieldSpecsReportSortedSetForTreePaths() throws Exception {
        IndexMapping mapping = mapping(ResolvedFieldHint.builder(IndexFieldKind.TREE_PATH)
                .sortable(true).build(), false);
        LuceneFieldSpecs specs = LuceneFieldSpecs.from(mapping);
        assertThat(specs.find("category").orElseThrow().docValuesType())
                .isEqualTo(DocValuesType.SORTED_SET);
    }

    private Document map(ResolvedFieldHint hint, TreePath path) throws Exception {
        Descriptor docType = docDescriptor(false);
        DynamicMessage message = DynamicMessage.newBuilder(docType)
                .setField(docType.findFieldByName("category"), path)
                .build();
        return mapper.map(message, mapping(hint, false));
    }

    private static IndexMapping mapping(ResolvedFieldHint hint, boolean repeated) {
        return new IndexMapping("ai.protomolt.test.Doc", List.of(
                new IndexMapping.IndexedField("category", "category", hint, repeated)));
    }

    private static List<String> indexedValues(Document doc) {
        return fieldStream(doc)
                .filter(f -> f.fieldType().indexOptions() != IndexOptions.NONE)
                .map(IndexableField::stringValue)
                .toList();
    }

    private static List<IndexableField> docValues(Document doc) {
        return fieldStream(doc)
                .filter(f -> f.fieldType().docValuesType() != DocValuesType.NONE)
                .toList();
    }

    private static List<String> storedValues(Document doc) {
        return fieldStream(doc)
                .filter(f -> f.fieldType().stored())
                .map(IndexableField::stringValue)
                .toList();
    }

    private static java.util.stream.Stream<IndexableField> fieldStream(Document doc) {
        return java.util.Arrays.stream(doc.getFields("category"));
    }

    private static Descriptor docDescriptor(boolean repeated) throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("tree_path_doc_" + (repeated ? "repeated" : "singular") + ".proto")
                .setPackage("ai.protomolt.test")
                .setSyntax("proto3")
                .addDependency(TreePath.getDescriptor().getFile().getName())
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Doc")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("category")
                                .setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName("." + TreePath.getDescriptor().getFullName())
                                .setLabel(repeated
                                        ? FieldDescriptorProto.Label.LABEL_REPEATED
                                        : FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .build();
        return FileDescriptor.buildFrom(
                        file, new FileDescriptor[]{TreePath.getDescriptor().getFile()})
                .findMessageTypeByName("Doc");
    }
}
