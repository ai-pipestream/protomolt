package ai.pipestream.proto.mapper.metadata;

import ai.pipestream.proto.cel.CelEvaluator;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link MetadataExtractor} contract: argument validation, empty input, result shape,
 * typed value extraction, parallel failure reporting, and validation-cache eviction.
 */
class MetadataExtractorContractTest {

    private final CelEvaluator evaluator = new CelEvaluator();
    private final MetadataExtractor extractor = new MetadataExtractor(evaluator);

    @Test
    void nullArgumentsAreRejected() {
        Descriptor descriptor = richDocumentDescriptor("nullargs");
        DynamicMessage input = DynamicMessage.getDefaultInstance(descriptor);
        Map<String, String> selectors = Map.of("title", "input.title");
        assertThrows(NullPointerException.class, () -> extractor.extract(null, input, selectors));
        assertThrows(NullPointerException.class, () -> extractor.extract(descriptor, null, selectors));
        assertThrows(NullPointerException.class, () -> new MetadataExtractor(null));
    }

    @Test
    void nullOrEmptySelectorsExtractNothing() {
        Descriptor descriptor = richDocumentDescriptor("empty");
        DynamicMessage input = DynamicMessage.getDefaultInstance(descriptor);
        assertEquals(Map.of(), extractor.extract(descriptor, input, null));
        assertEquals(Map.of(), extractor.extract(descriptor, input, Map.of()));
    }

    @Test
    void resultMapIsImmutable() {
        Descriptor descriptor = richDocumentDescriptor("immutable");
        DynamicMessage input = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("title"), "t").build();
        Map<String, Object> single = extractor.extract(descriptor, input, Map.of("title", "input.title"));
        assertThrows(UnsupportedOperationException.class, () -> single.put("x", "y"));

        Map<String, Object> several = extractor.extract(descriptor, input, Map.of(
                "title", "input.title", "pages", "input.pages"));
        assertThrows(UnsupportedOperationException.class, () -> several.put("x", "y"));
    }

    @Test
    void extractsTypedValuesAcrossFieldKinds() {
        Descriptor descriptor = richDocumentDescriptor("types");
        DynamicMessage input = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("title"), "t")
                .setField(descriptor.findFieldByName("pages"), 12)
                .setField(descriptor.findFieldByName("score"), 0.5d)
                .setField(descriptor.findFieldByName("published"), true)
                .addRepeatedField(descriptor.findFieldByName("tags"), "a")
                .addRepeatedField(descriptor.findFieldByName("tags"), "b")
                .build();
        Map<String, String> selectors = new LinkedHashMap<>();
        selectors.put("title", "input.title");
        selectors.put("pages", "input.pages");
        selectors.put("score", "input.score");
        selectors.put("published", "input.published");
        selectors.put("tags", "input.tags");
        Map<String, Object> result = extractor.extract(descriptor, input, selectors);
        assertThat(result)
                .containsEntry("title", "t")
                .containsEntry("pages", 12L)
                .containsEntry("score", 0.5d)
                .containsEntry("published", true)
                .containsEntry("tags", List.of("a", "b"));
    }

    @Test
    void computedSelectorEvaluatesAgainstTheMessage() {
        Descriptor descriptor = richDocumentDescriptor("computed");
        DynamicMessage input = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("title"), "doc").build();
        Map<String, Object> result = extractor.extract(descriptor, input,
                Map.of("shouted", "input.title + '!'"));
        assertEquals("doc!", result.get("shouted"));
    }

    @Test
    void unsetScalarFieldYieldsItsProtoDefault() {
        Descriptor descriptor = richDocumentDescriptor("defaults");
        DynamicMessage input = DynamicMessage.getDefaultInstance(descriptor);
        Map<String, Object> result = extractor.extract(descriptor, input, Map.of(
                "title", "input.title", "pages", "input.pages"));
        assertEquals("", result.get("title"));
        assertEquals(0L, result.get("pages"));
    }

    @Test
    void parallelExtractionFailureNamesTheFailingSelector() {
        Descriptor descriptor = richDocumentDescriptor("parallelfail");
        DynamicMessage input = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("title"), "t").build();
        Map<String, String> selectors = new LinkedHashMap<>();
        selectors.put("ok", "input.title");
        selectors.put("boom", "1 / 0");
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> extractor.extract(descriptor, input, selectors));
        assertThat(e.getMessage()).contains("boom").contains("1 / 0");
    }

    @Test
    void sameExpressionUnderTwoNamesCompilesOnce() {
        Descriptor descriptor = richDocumentDescriptor("dedup");
        DynamicMessage input = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("title"), "t").build();
        Map<String, String> selectors = new LinkedHashMap<>();
        selectors.put("a", "input.title");
        selectors.put("b", "input.title");
        Map<String, Object> result = extractor.extract(descriptor, input, selectors);
        assertEquals("t", result.get("a"));
        assertEquals("t", result.get("b"));
        assertEquals(1, evaluator.cacheSize());
    }

    @Test
    void environmentEvictionKeepsExtractionWorking() {
        // MAX_ENVIRONMENTS is 64; crossing it clears the per-descriptor cache. Extraction
        // must keep working for both new and previously-seen descriptors.
        for (int i = 0; i < 70; i++) {
            Descriptor descriptor = richDocumentDescriptor("evict" + i);
            DynamicMessage input = DynamicMessage.newBuilder(descriptor)
                    .setField(descriptor.findFieldByName("title"), "t" + i).build();
            assertEquals("t" + i, extractor.extract(descriptor, input,
                    Map.of("title", "input.title")).get("title"));
        }
        Descriptor first = richDocumentDescriptor("evict0");
        DynamicMessage input = DynamicMessage.newBuilder(first)
                .setField(first.findFieldByName("title"), "again").build();
        assertEquals("again", extractor.extract(first, input,
                Map.of("title", "input.title")).get("title"));
    }

    private static Descriptor richDocumentDescriptor(String suffix) {
        try {
            var string = DescriptorProtos.FieldDescriptorProto.newBuilder()
                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING);
            var document = DescriptorProtos.DescriptorProto.newBuilder().setName("Document")
                    .addField(string.clone().setName("title").setNumber(1))
                    .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                            .setName("pages").setNumber(2)
                            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32))
                    .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                            .setName("score").setNumber(3)
                            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE))
                    .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                            .setName("published").setNumber(4)
                            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL))
                    .addField(string.clone().setName("tags").setNumber(5)
                            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED))
                    .build();
            return FileDescriptor.buildFrom(DescriptorProtos.FileDescriptorProto.newBuilder()
                            .setName("contract_" + suffix + ".proto")
                            .setPackage("contracttest." + suffix)
                            .addMessageType(document).build(),
                    new FileDescriptor[]{}).findMessageTypeByName("Document");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    void repeatedExtractionAcrossCallsReusesValidation() {
        Descriptor descriptor = richDocumentDescriptor("repeat");
        DynamicMessage input = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("title"), "t").build();
        Map<String, String> selectors = Map.of("title", "input.title");
        extractor.extract(descriptor, input, selectors);
        extractor.extract(descriptor, input, selectors);
        assertTrue(evaluator.cacheSize() <= 1);
    }
}
