package ai.pipestream.proto.index.spi;

import ai.pipestream.proto.index.hints.FieldIndexHint;
import ai.pipestream.proto.index.hints.IndexingHintsProto;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Struct;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IndexingPlanFactoryTest {

    @Test
    void infersKeywordForIdAndTextForBody() throws Exception {
        Descriptor descriptor = sampleDescriptor();
        IndexingPlan plan = IndexingPlanFactory.inferringOnly().create(descriptor);

        assertThat(plan.find("doc_id")).get().extracting(f -> f.type())
                .isEqualTo(IndexFieldKind.KEYWORD);
        assertThat(plan.find("title")).get().extracting(f -> f.type())
                .isEqualTo(IndexFieldKind.TEXT);
        assertThat(plan.find("page_count")).get().extracting(f -> f.type())
                .isEqualTo(IndexFieldKind.INT32);
    }

    @Test
    void catalogOverridesInference() throws Exception {
        Descriptor descriptor = sampleDescriptor();
        CatalogIndexingHintSource catalog = new CatalogIndexingHintSource()
                .put(descriptor.getFullName(), "title", ResolvedFieldHint.of(IndexFieldKind.KEYWORD));
        IndexingPlan plan = IndexingPlanFactory.defaults(catalog).create(descriptor);

        assertThat(plan.find("title")).get().extracting(IndexingPlan.IndexedField::type)
                .isEqualTo(IndexFieldKind.KEYWORD);
    }

    @Test
    void protoOptionHintWithoutTypeKeepsExplicitAttributes() throws Exception {
        Descriptor descriptor = hintedDescriptor();
        IndexingPlanFactory factory = new IndexingPlanFactory(
                new ProtoOptionsIndexingHintSource().orElse(new InferringIndexingHintSource()));
        IndexingPlan plan = factory.create(descriptor);

        IndexingPlan.IndexedField title = plan.find("title").orElseThrow();
        // explicit name/stored from the option survive even though type was left unset
        assertThat(title.fieldName()).isEqualTo("custom_title");
        assertThat(title.stored()).isFalse();
        // type comes from inference; indexed was not explicitly set, so inferred default applies
        assertThat(title.type()).isEqualTo(IndexFieldKind.TEXT);
        assertThat(title.indexed()).isTrue();
    }

    @Test
    void jsonNameModeUsesJsonNamesForEveryFieldNameSegment() throws Exception {
        Descriptor descriptor = nestedSampleDescriptor();
        IndexingPlanFactory factory = new IndexingPlanFactory(expandingHints(), false, 8);
        IndexingPlan plan = factory.create(descriptor);

        // paths stay in proto names (the field-mapper vocabulary)
        IndexingPlan.IndexedField leaf = plan.find("user_address.display_name").orElseThrow();
        // prefix and leaf use the same naming mode: no mixed user_address_displayName
        assertThat(leaf.fieldName()).isEqualTo("userAddress_displayName");
    }

    @Test
    void protoNameModeUsesProtoNamesForEveryFieldNameSegment() throws Exception {
        Descriptor descriptor = nestedSampleDescriptor();
        IndexingPlanFactory factory = new IndexingPlanFactory(expandingHints(), true, 8);
        IndexingPlan plan = factory.create(descriptor);

        IndexingPlan.IndexedField leaf = plan.find("user_address.display_name").orElseThrow();
        assertThat(leaf.fieldName()).isEqualTo("user_address_display_name");
    }

    /**
     * A name override renames the leaf it sits on; on an expanded message it has to rename the
     * prefix instead, or the children silently keep the un-overridden name.
     */
    @Test
    void nameOverrideOnAnExpandedMessagePrefixesItsChildren() throws Exception {
        Descriptor descriptor = nestedSampleDescriptor();
        IndexingHintSource hints = field ->
                field.getJavaType() == com.google.protobuf.Descriptors.FieldDescriptor.JavaType.MESSAGE
                        ? java.util.Optional.of(
                                ResolvedFieldHint.builder(IndexFieldKind.TEXT).name("addr").build())
                        : java.util.Optional.empty();
        IndexingPlan plan = new IndexingPlanFactory(hints, true, 8).create(descriptor);

        assertThat(plan.find("user_address.display_name")).get()
                .extracting(IndexingPlan.IndexedField::fieldName)
                .isEqualTo("addr_display_name");
    }

    @Test
    void repeatedProtoFieldsAreMarkedRepeated() throws Exception {
        Descriptor descriptor = inferenceDescriptor();
        IndexingPlan plan = IndexingPlanFactory.inferringOnly().create(descriptor);

        assertThat(plan.find("embedding")).get()
                .extracting(IndexingPlan.IndexedField::repeated).isEqualTo(true);
        assertThat(plan.find("digest")).get()
                .extracting(IndexingPlan.IndexedField::repeated).isEqualTo(false);
    }

    /** Hints message fields with an expandable kind so nested fields become dotted paths. */
    private static IndexingHintSource expandingHints() {
        return field -> field.getJavaType() == com.google.protobuf.Descriptors.FieldDescriptor.JavaType.MESSAGE
                ? java.util.Optional.of(ResolvedFieldHint.of(IndexFieldKind.TEXT))
                : java.util.Optional.empty();
    }

    @Test
    void structInfersObjectFields() {
        IndexingPlan plan = IndexingPlanFactory.inferringOnly().create(Struct.getDescriptor());
        assertThat(plan.fields()).isNotEmpty();
        assertThat(plan.find("fields")).isPresent();
    }

    @Test
    void infersDateForTimestampBinaryForBytesAndNeverVector() throws Exception {
        Descriptor descriptor = inferenceDescriptor();
        IndexingPlan plan = IndexingPlanFactory.inferringOnly().create(descriptor);

        assertThat(plan.find("created")).get().extracting(IndexingPlan.IndexedField::type)
                .isEqualTo(IndexFieldKind.DATE);
        assertThat(plan.find("digest")).get().extracting(IndexingPlan.IndexedField::type)
                .isEqualTo(IndexFieldKind.BINARY);
        // VECTOR is explicit-only: a repeated float field infers FLOAT
        assertThat(plan.find("embedding")).get().extracting(IndexingPlan.IndexedField::type)
                .isEqualTo(IndexFieldKind.FLOAT);
    }

    @Test
    void richProtoOptionHintResolvesEveryAttribute() throws Exception {
        Descriptor descriptor = richHintedDescriptor();
        IndexingPlanFactory factory = new IndexingPlanFactory(
                new ProtoOptionsIndexingHintSource().orElse(new InferringIndexingHintSource()));
        IndexingPlan plan = factory.create(descriptor);

        ResolvedFieldHint hint = plan.find("embedding").orElseThrow().hint();
        assertThat(hint.type()).isEqualTo(IndexFieldKind.VECTOR);
        assertThat(hint.vectorDims()).isEqualTo(3);
        assertThat(hint.vectorSimilarity()).isEqualTo(VectorSimilarity.L2);
        assertThat(hint.vectorElementType()).isEqualTo(VectorElementType.BYTE);
        assertThat(hint.hnswParams()).isEqualTo(new ResolvedFieldHint.HnswParams(16, 200));
        assertThat(hint.subFields()).containsExactly(
                new ResolvedFieldHint.SubField(IndexFieldKind.KEYWORD, "raw", "keyword_analyzer"));
        assertThat(hint.analyzerOverride()).contains("english");
        assertThat(hint.searchAnalyzerOverride()).contains("english_search");
        assertThat(hint.nullValue()).isEqualTo("none");
        assertThat(hint.skipIfMissing()).isFalse();
        assertThat(hint.sortable()).isTrue();
        assertThat(hint.facetable()).isTrue();
        assertThat(hint.mapMode()).isEqualTo(MapMode.JSON);
        assertThat(hint.dateFormatOverride()).contains("epoch_millis");
        assertThat(hint.dateResolution()).isEqualTo(DateResolution.SECONDS);
        assertThat(hint.engineParams("opensearch")).containsOnly(Map.entry("engine", "faiss"));
    }

    @Test
    void catalogRichHintOverridesProtoOptions() throws Exception {
        Descriptor descriptor = hintedDescriptor(); // proto option: name "custom_title", stored=false
        CatalogIndexingHintSource catalog = new CatalogIndexingHintSource()
                .put(descriptor.getFullName(), "title", ResolvedFieldHint.builder(IndexFieldKind.KEYWORD)
                        .sortable(true)
                        .engineParams(Map.of("solr.omitNorms", "true"))
                        .build());
        IndexingPlan plan = IndexingPlanFactory.defaults(catalog).create(descriptor);

        IndexingPlan.IndexedField title = plan.find("title").orElseThrow();
        // catalog wins wholesale: proto-option name/stored do not leak through
        assertThat(title.fieldName()).isEqualTo("title");
        assertThat(title.type()).isEqualTo(IndexFieldKind.KEYWORD);
        assertThat(title.hint().sortable()).isTrue();
        assertThat(title.hint().engineParams("solr")).containsOnly(Map.entry("omitNorms", "true"));
    }

    @Test
    void unspecifiedTypeMergeKeepsRichAttributes() throws Exception {
        Descriptor descriptor = sampleDescriptor();
        CatalogIndexingHintSource catalog = new CatalogIndexingHintSource()
                .put(descriptor.getFullName(), "title",
                        new ResolvedFieldHint(IndexFieldKind.UNSPECIFIED, true, true, "", 0).toBuilder()
                                .sortable(true)
                                .analyzer("english")
                                .nullValue("n/a")
                                .build());
        IndexingPlan plan = IndexingPlanFactory.defaults(catalog).create(descriptor);

        ResolvedFieldHint hint = plan.find("title").orElseThrow().hint();
        // type comes from inference; the rich attributes survive the merge
        assertThat(hint.type()).isEqualTo(IndexFieldKind.TEXT);
        assertThat(hint.sortable()).isTrue();
        assertThat(hint.analyzerOverride()).contains("english");
        assertThat(hint.nullValue()).isEqualTo("n/a");
    }

    @Test
    void rangeHintResolvesGteLteBounds() throws Exception {
        Descriptor descriptor = rangeDescriptor("gte", "lte", FieldDescriptorProto.Type.TYPE_INT32);
        CatalogIndexingHintSource catalog = new CatalogIndexingHintSource()
                .put("pages", ResolvedFieldHint.of(IndexFieldKind.INT_RANGE));
        IndexingPlan plan = IndexingPlanFactory.defaults(catalog).create(descriptor);

        // the range field stays a single plan entry: bounds are not expanded into dotted paths
        assertThat(plan.find("pages")).get().extracting(IndexingPlan.IndexedField::type)
                .isEqualTo(IndexFieldKind.INT_RANGE);
        assertThat(plan.find("pages.gte")).isEmpty();
    }

    @Test
    void rangeHintResolvesMinMaxBounds() throws Exception {
        Descriptor descriptor = rangeDescriptor("min", "max", FieldDescriptorProto.Type.TYPE_INT64);
        CatalogIndexingHintSource catalog = new CatalogIndexingHintSource()
                .put("pages", ResolvedFieldHint.of(IndexFieldKind.LONG_RANGE));
        IndexingPlan plan = IndexingPlanFactory.defaults(catalog).create(descriptor);

        assertThat(plan.find("pages")).get().extracting(IndexingPlan.IndexedField::type)
                .isEqualTo(IndexFieldKind.LONG_RANGE);
    }

    @Test
    void rangeHintWithMismatchedBoundTypesThrowsWithPath() throws Exception {
        // gte/lte exist but are int32 while the hint demands DOUBLE_RANGE
        Descriptor descriptor = rangeDescriptor("gte", "lte", FieldDescriptorProto.Type.TYPE_INT32);
        CatalogIndexingHintSource catalog = new CatalogIndexingHintSource()
                .put("pages", ResolvedFieldHint.of(IndexFieldKind.DOUBLE_RANGE));

        assertThatThrownBy(() -> IndexingPlanFactory.defaults(catalog).create(descriptor))
                .isInstanceOf(IndexingPlanException.class)
                .hasMessageContaining("DOUBLE_RANGE")
                .hasMessageContaining("pages");
    }

    @Test
    void rangeHintWithoutBoundPairThrowsWithPath() throws Exception {
        Descriptor descriptor = rangeDescriptor("low", "high", FieldDescriptorProto.Type.TYPE_INT32);
        CatalogIndexingHintSource catalog = new CatalogIndexingHintSource()
                .put("pages", ResolvedFieldHint.of(IndexFieldKind.INT_RANGE));

        assertThatThrownBy(() -> IndexingPlanFactory.defaults(catalog).create(descriptor))
                .isInstanceOf(IndexingPlanException.class)
                .hasMessageContaining("(gte,lte) or (min,max)")
                .hasMessageContaining("pages");
    }

    @Test
    void rangeHintOnScalarFieldThrowsWithPath() throws Exception {
        Descriptor descriptor = sampleDescriptor();
        CatalogIndexingHintSource catalog = new CatalogIndexingHintSource()
                .put("page_count", ResolvedFieldHint.of(IndexFieldKind.INT_RANGE));

        assertThatThrownBy(() -> IndexingPlanFactory.defaults(catalog).create(descriptor))
                .isInstanceOf(IndexingPlanException.class)
                .hasMessageContaining("singular message field")
                .hasMessageContaining("page_count");
    }

    @Test
    void nestedRangeErrorCarriesDottedPath() throws Exception {
        Descriptor descriptor = nestedRangeDescriptor();
        CatalogIndexingHintSource catalog = new CatalogIndexingHintSource()
                // TEXT on the intermediate message expands it into dotted paths
                .put("inner", ResolvedFieldHint.of(IndexFieldKind.TEXT))
                .put("pages", ResolvedFieldHint.of(IndexFieldKind.INT_RANGE));

        assertThatThrownBy(() -> IndexingPlanFactory.defaults(catalog).create(descriptor))
                .isInstanceOf(IndexingPlanException.class)
                .hasMessageContaining("inner.pages");
    }

    @Test
    void unparsableNullValueThrowsWithPath() throws Exception {
        Descriptor descriptor = sampleDescriptor();
        CatalogIndexingHintSource catalog = new CatalogIndexingHintSource()
                .put("page_count", ResolvedFieldHint.builder(IndexFieldKind.INT32)
                        .nullValue("not-a-number")
                        .build());

        assertThatThrownBy(() -> IndexingPlanFactory.defaults(catalog).create(descriptor))
                .isInstanceOf(IndexingPlanException.class)
                .hasMessageContaining("null_value")
                .hasMessageContaining("page_count");
    }

    /**
     * The visiting set is the only thing standing between a self-referential message and a
     * StackOverflowError; without it {@code create} never returns.
     */
    @Test
    void selfReferentialMessageStopsAtTheCycleInsteadOfRecursing() throws Exception {
        Descriptor descriptor = selfReferentialDescriptor();
        IndexingPlan plan = new IndexingPlanFactory(expandingHints(), true, 8).create(descriptor);

        // The back-reference expands into a message already on the stack, so it contributes
        // nothing: the plan is the scalar fields reachable before the cycle closes.
        assertThat(plan.fields()).extracting(IndexingPlan.IndexedField::path)
                .containsExactly("id");
    }

    /**
     * A sibling cycle (A -> B -> A) is only caught if the guard tracks the whole path, not just
     * the immediately preceding message.
     */
    @Test
    void mutuallyRecursiveMessagesStopAtTheCycle() throws Exception {
        Descriptor descriptor = mutuallyRecursiveDescriptor();
        IndexingPlan plan = new IndexingPlanFactory(expandingHints(), true, 8).create(descriptor);

        assertThat(plan.fields()).extracting(IndexingPlan.IndexedField::path)
                .containsExactly("a_label", "b.b_label");
    }

    @Test
    void nestingBeyondMaxDepthStopsExpandingAndKeepsTheMessageAsALeaf() throws Exception {
        Descriptor descriptor = chainDescriptor(5);
        IndexingPlan plan = new IndexingPlanFactory(expandingHints(), true, 2).create(descriptor);

        // Three expansions (depths 0, 1, 2) then the cap: the fourth message stays a leaf
        // entry rather than expanding into its own children.
        IndexingPlan.IndexedField capped = plan.find("next.next.next").orElseThrow();
        assertThat(capped.fieldName()).isEqualTo("next_next_next");
        assertThat(capped.type()).isEqualTo(IndexFieldKind.TEXT);
        assertThat(plan.find("next.next.next.next")).isEmpty();
        assertThat(plan.fields()).hasSize(1);
    }

    @Test
    void maxDepthOfZeroKeepsEveryTopLevelMessageAsALeaf() throws Exception {
        Descriptor descriptor = chainDescriptor(5);
        IndexingPlan plan = new IndexingPlanFactory(expandingHints(), true, 0).create(descriptor);

        assertThat(plan.fields()).extracting(IndexingPlan.IndexedField::path)
                .containsExactly("next");
    }

    private static Descriptor selfReferentialDescriptor() throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("self_referential.proto")
                .setPackage("ai.pipestream.test")
                .setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Node")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("id")
                                .setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("next")
                                .setNumber(2)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(".ai.pipestream.test.Node")
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("Node");
    }

    private static Descriptor mutuallyRecursiveDescriptor() throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("mutually_recursive.proto")
                .setPackage("ai.pipestream.test")
                .setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("A")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("a_label")
                                .setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("b")
                                .setNumber(2)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(".ai.pipestream.test.B")
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("B")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("b_label")
                                .setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("a")
                                .setNumber(2)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(".ai.pipestream.test.A")
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("A");
    }

    /** A straight chain {@code L0.next -> L1.next -> ... -> L(levels).leaf}, rooted at L0. */
    private static Descriptor chainDescriptor(int levels) throws Exception {
        FileDescriptorProto.Builder file = FileDescriptorProto.newBuilder()
                .setName("chain.proto")
                .setPackage("ai.pipestream.test")
                .setSyntax("proto3");
        for (int level = 0; level <= levels; level++) {
            DescriptorProto.Builder message = DescriptorProto.newBuilder().setName("L" + level);
            if (level < levels) {
                message.addField(FieldDescriptorProto.newBuilder()
                        .setName("next")
                        .setNumber(1)
                        .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName(".ai.pipestream.test.L" + (level + 1))
                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL));
            } else {
                message.addField(FieldDescriptorProto.newBuilder()
                        .setName("leaf")
                        .setNumber(1)
                        .setType(FieldDescriptorProto.Type.TYPE_STRING)
                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL));
            }
            file.addMessageType(message);
        }
        return FileDescriptor.buildFrom(file.build(), new FileDescriptor[0]).findMessageTypeByName("L0");
    }

    private static Descriptor inferenceDescriptor() throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("inference_doc.proto")
                .setPackage("ai.pipestream.test")
                .setSyntax("proto3")
                .addDependency("google/protobuf/timestamp.proto")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("InferenceDoc")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("created")
                                .setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(".google.protobuf.Timestamp")
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("digest")
                                .setNumber(2)
                                .setType(FieldDescriptorProto.Type.TYPE_BYTES)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("embedding")
                                .setNumber(3)
                                .setType(FieldDescriptorProto.Type.TYPE_FLOAT)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)))
                .build();
        return FileDescriptor.buildFrom(
                        file, new FileDescriptor[]{com.google.protobuf.TimestampProto.getDescriptor()})
                .findMessageTypeByName("InferenceDoc");
    }

    private static Descriptor richHintedDescriptor() throws Exception {
        DescriptorProtos.FieldOptions embeddingOptions = DescriptorProtos.FieldOptions.newBuilder()
                .setExtension(IndexingHintsProto.index, FieldIndexHint.newBuilder()
                        .setType(ai.pipestream.proto.index.hints.IndexFieldType.INDEX_FIELD_TYPE_VECTOR)
                        .setVectorDims(3)
                        .setVectorSimilarity(ai.pipestream.proto.index.hints.VectorSimilarity.VECTOR_SIMILARITY_L2)
                        .setVectorElementType(
                                ai.pipestream.proto.index.hints.VectorElementType.VECTOR_ELEMENT_TYPE_BYTE)
                        .setHnsw(ai.pipestream.proto.index.hints.HnswParams.newBuilder()
                                .setM(16)
                                .setEfConstruction(200))
                        .addSubFields(ai.pipestream.proto.index.hints.SubFieldHint.newBuilder()
                                .setType(ai.pipestream.proto.index.hints.IndexFieldType.INDEX_FIELD_TYPE_KEYWORD)
                                .setName("raw")
                                .setAnalyzer("keyword_analyzer"))
                        .setAnalyzer("english")
                        .setSearchAnalyzer("english_search")
                        .setNullValue("none")
                        .setSkipIfMissing(false)
                        .setSortable(true)
                        .setFacetable(true)
                        .setMapMode(ai.pipestream.proto.index.hints.MapMode.MAP_MODE_JSON)
                        .setDateFormat("epoch_millis")
                        .setDateResolution(ai.pipestream.proto.index.hints.DateResolution.DATE_RESOLUTION_SECONDS)
                        .putEngineParams("opensearch.engine", "faiss")
                        .build())
                .build();
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("rich_hinted_doc.proto")
                .setPackage("ai.pipestream.test")
                .setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("RichHintedDoc")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("embedding")
                                .setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_FLOAT)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)
                                .setOptions(embeddingOptions)))
                .build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("RichHintedDoc");
    }

    private static Descriptor rangeDescriptor(
            String lowerName, String upperName, FieldDescriptorProto.Type boundType) throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("range_" + lowerName + "_doc.proto")
                .setPackage("ai.pipestream.test")
                .setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Bounds")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName(lowerName)
                                .setNumber(1)
                                .setType(boundType)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName(upperName)
                                .setNumber(2)
                                .setType(boundType)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("RangeDoc")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("pages")
                                .setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(".ai.pipestream.test.Bounds")
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("RangeDoc");
    }

    private static Descriptor nestedRangeDescriptor() throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("nested_range_doc.proto")
                .setPackage("ai.pipestream.test")
                .setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Bounds")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("low")
                                .setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_INT32)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Inner")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("pages")
                                .setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(".ai.pipestream.test.Bounds")
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Outer")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("inner")
                                .setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(".ai.pipestream.test.Inner")
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("Outer");
    }

    private static Descriptor hintedDescriptor() throws Exception {
        DescriptorProtos.FieldOptions titleOptions = DescriptorProtos.FieldOptions.newBuilder()
                .setExtension(IndexingHintsProto.index, FieldIndexHint.newBuilder()
                        .setName("custom_title")
                        .setStored(false)
                        .build())
                .build();
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("hinted_doc.proto")
                .setPackage("ai.pipestream.test")
                .setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("HintedDoc")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("title")
                                .setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .setOptions(titleOptions)))
                .build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("HintedDoc");
    }

    private static Descriptor nestedSampleDescriptor() throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("nested_names.proto")
                .setPackage("ai.pipestream.test")
                .setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Address")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("display_name")
                                .setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Profile")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("user_address")
                                .setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(".ai.pipestream.test.Address")
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("Profile");
    }

    private static Descriptor sampleDescriptor() throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("doc.proto")
                .setPackage("ai.pipestream.test")
                .setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Doc")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("doc_id")
                                .setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("title")
                                .setNumber(2)
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("page_count")
                                .setNumber(3)
                                .setType(FieldDescriptorProto.Type.TYPE_INT32)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("Doc");
    }
}
