package ai.pipestream.proto.metadata;

import ai.pipestream.proto.cel.CelEnvironmentFactory;
import ai.pipestream.proto.cel.CelEvaluator;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MetadataExtractorTest {

    @Test
    void extractsSingleSelectorWithoutExecutor() {
        CelEvaluator evaluator = new CelEvaluator(CelEnvironmentFactory.builder()
                .addMessageType(Struct.getDescriptor()).addVar("input").build());
        Struct input = Struct.newBuilder().putFields("title", Value.newBuilder().setStringValue("Hello").build()).build();
        assertEquals("Hello", new MetadataExtractor(evaluator).extract(Struct.getDescriptor(), input,
                Map.of("title", "input.title")).get("title"));
    }

    @Test
    void wrapsMissingSelectorFailure() {
        CelEvaluator evaluator = new CelEvaluator(CelEnvironmentFactory.builder()
                .addMessageType(Struct.getDescriptor()).addVar("input").build());
        Struct input = Struct.getDefaultInstance();
        assertThrows(RuntimeException.class, () -> new MetadataExtractor(evaluator).extract(
                Struct.getDescriptor(), input, Map.of("bad", "input.")));
    }

    @Test
    void extractsNamedSelectorsInParallel() {
        CelEvaluator evaluator = new CelEvaluator(CelEnvironmentFactory.builder()
                .addMessageType(Struct.getDescriptor())
                .addVar("input")
                .build());
        MetadataExtractor extractor = new MetadataExtractor(evaluator);

        Struct input = Struct.newBuilder()
                .putFields("title", Value.newBuilder().setStringValue("Hello").build())
                .putFields("score", Value.newBuilder().setNumberValue(42).build())
                .build();

        Map<String, String> selectors = new LinkedHashMap<>();
        selectors.put("title", "input.title");
        selectors.put("score", "input.score");

        Map<String, Object> result = extractor.extract(Struct.getDescriptor(), input, selectors);

        assertThat(result).containsEntry("title", "Hello");
        assertThat(result.get("score")).isEqualTo(42.0);
    }

    @Test
    void wrapsSingleSelectorFailureInIllegalStateException() {
        CelEvaluator evaluator = new CelEvaluator(CelEnvironmentFactory.builder()
                .addMessageType(Struct.getDescriptor()).addVar("input").build());
        Struct input = Struct.getDefaultInstance();
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> new MetadataExtractor(evaluator).extract(Struct.getDescriptor(), input,
                        Map.of("title", "input.missing_key")));
        assertThat(e.getMessage()).contains("input.missing_key");
    }

    @Test
    void surfacesSelectorCompileErrorsEagerlyViaDescriptor() {
        Descriptor descriptor = documentDescriptor();
        CelEvaluator evaluator = new CelEvaluator();
        DynamicMessage input = DynamicMessage.getDefaultInstance(descriptor);
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> new MetadataExtractor(evaluator).extract(descriptor, input,
                        Map.of("bad", "input.no_such_field")));
        assertThat(e.getMessage()).contains("Invalid metadata selector").contains("input.no_such_field");
    }

    @Test
    void typedEnvironmentAcceptsValidSelectorOnDynamicMessage() {
        Descriptor descriptor = documentDescriptor();
        CelEvaluator evaluator = new CelEvaluator(CelEnvironmentFactory.builder()
                .addMessageType(descriptor).addVar("input").build());
        DynamicMessage input = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("title"), "Hello").build();
        assertEquals("Hello", new MetadataExtractor(evaluator).extract(descriptor, input,
                Map.of("title", "input.title")).get("title"));
    }

    @Test
    void surfacesEvaluatorEnvironmentMismatchEagerly() {
        // Typed validation passes, but the injected evaluator's environment has no `input`
        // variable; the mismatch must surface as an eager invalid-selector error.
        CelEvaluator evaluator = new CelEvaluator(CelEnvironmentFactory.builder().build());
        Struct input = Struct.getDefaultInstance();
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> new MetadataExtractor(evaluator).extract(Struct.getDescriptor(), input,
                        Map.of("title", "input.title")));
        assertThat(e.getMessage())
                .contains("Invalid metadata selector")
                .contains("evaluator environment");
    }

    @Test
    void repeatedExtractionReusesValidatedSelectors() {
        CelEvaluator evaluator = new CelEvaluator(CelEnvironmentFactory.builder()
                .addMessageType(Struct.getDescriptor()).addVar("input").build());
        MetadataExtractor extractor = new MetadataExtractor(evaluator);
        Struct input = Struct.newBuilder()
                .putFields("title", Value.newBuilder().setStringValue("Hello").build()).build();
        Map<String, String> selectors = Map.of("title", "input.title");
        assertEquals("Hello", extractor.extract(Struct.getDescriptor(), input, selectors).get("title"));
        assertEquals("Hello", extractor.extract(Struct.getDescriptor(), input, selectors).get("title"));
        // The evaluator compiled the selector exactly once and reuses its cached program.
        assertEquals(1, evaluator.cacheSize());
    }

    private static Descriptor documentDescriptor() {
        try {
            var document = DescriptorProtos.DescriptorProto.newBuilder().setName("Document")
                    .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                            .setName("title").setNumber(1)
                            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                    .build();
            return FileDescriptor.buildFrom(DescriptorProtos.FileDescriptorProto.newBuilder()
                            .setName("metadata_fixtures.proto").setPackage("metadatatest")
                            .addMessageType(document).build(),
                    new FileDescriptor[]{}).findMessageTypeByName("Document");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
