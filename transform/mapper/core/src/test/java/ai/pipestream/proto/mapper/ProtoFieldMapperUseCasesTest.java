package ai.pipestream.proto.mapper;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import com.google.protobuf.Any;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ProtoFieldMapperUseCasesTest {
    private ProtoFieldMapper mapper;

    @BeforeEach
    void setUp() {
        DescriptorRegistry registry = DescriptorRegistry.create();
        registry.registerFile(TestDescriptors.FILE);
        mapper = new ProtoFieldMapperImpl(registry);
    }

    @Test void mapInPlaceReadsProgressiveWrites() throws Exception {
        var builder = doc("title", "source");
        mapper.mapInPlace(builder, List.of("body = title", "language = body"));
        assertEquals("source", value(builder, "language"));
    }
    @Test void mapReadsOnlyImmutableSource() throws Exception {
        Message source = doc("title", "source", "body", "from-source").build();
        var target = TestDescriptors.document();
        mapper.map(source, target, List.of("body = title", "language = body"));
        assertEquals("from-source", value(target, "language"));
    }
    @Test void writesDeepStructKey() throws Exception {
        var builder = doc();
        mapper.setValue(builder, "metadata.inner", "value");
        assertEquals("value", mapper.getValue(builder, "metadata.inner"));
    }
    @Test void appendsStructListRepeatedly() throws Exception {
        var builder = doc();
        mapper.appendValue(builder, "metadata.tags", "one");
        mapper.appendValue(builder, "metadata.tags", "two");
        assertEquals(List.of("one", "two"), mapper.getValue(builder, "metadata.tags"));
    }
    @Test void clearsStructKey() throws Exception {
        var builder = doc();
        mapper.setValue(builder, "metadata.key", "value");
        mapper.clearField(builder, "metadata.key");
        assertNull(mapper.getValue(builder, "metadata.key"));
    }
    @Test void mapsNumericBooleanNullAndStringLiterals() throws Exception {
        var builder = doc();
        mapper.mapInPlace(builder, List.of("score = 2.5", "info.enabled = true", "title = \"quoted\""));
        assertEquals(2.5d, value(builder, "score"));
        assertEquals("quoted", value(builder, "title"));
        assertEquals(true, mapper.getValue(builder, "info.enabled"));
    }
    @Test void copiesSiblingNestedMessages() throws Exception {
        var source = doc("title", "copied").build();
        var target = doc();
        mapper.map(source, target, List.of("body = title"));
        assertEquals("copied", value(target, "body"));
    }
    @Test void exposesDirectFieldApis() throws Exception {
        var builder = doc();
        mapper.setValue(builder, "title", "x"); mapper.appendValue(builder, "tags", "x");
        assertEquals("x", mapper.getValue(builder, "title"));
        mapper.clearField(builder, "title");
        assertEquals("", value(builder, "title"));
    }
    @Test void coercesStringToInt64InStandaloneSchema() throws Exception {
        var descriptor = TestDescriptors.FILE.findMessageTypeByName("Info");
        var builder = DynamicMessage.newBuilder(descriptor);
        mapper.setValue(builder, "version", 12L);
        assertEquals("12", mapper.getValue(builder, "version"));
    }
    @Test void readsPathThroughPackedAny() throws Exception {
        var info = DynamicMessage.newBuilder(TestDescriptors.INFO)
                .setField(TestDescriptors.INFO.findFieldByName("version"), "v1").build();
        var builder = doc();
        mapper.setValue(builder, "payload", Any.pack(info));
        assertEquals("v1", mapper.getValue(builder, "payload.version"));
    }
    @Test void mapsEmptyRulesAsNoOp() throws Exception {
        var builder = doc("title", "unchanged"); mapper.mapInPlace(builder, List.of());
        assertEquals("unchanged", value(builder, "title"));
    }
    @Test void acceptsWhitespaceAroundRules() throws Exception {
        var builder = doc("title", "value"); mapper.mapInPlace(builder, List.of("  body  =  title  "));
        assertEquals("value", value(builder, "body"));
    }
    @Test void reportsOriginalRuleOnFailure() {
        MappingException error = assertThrows(MappingException.class,
                () -> mapper.mapInPlace(doc(), List.of("unknown = title")));
        assertTrue(error.getMessage().contains("unknown = title"));
    }
    @Test void overwritesExistingValue() throws Exception {
        var builder = doc("title", "old"); mapper.setValue(builder, "title", "new");
        assertEquals("new", value(builder, "title"));
    }
    @Test void mapsBetweenSameNamedDynamicSchemas() throws Exception {
        var source = doc("title", "shared").build(); var target = doc();
        mapper.map(source, target, List.of("title = title"));
        assertEquals("shared", value(target, "title"));
    }

    private static DynamicMessage.Builder doc(String... pairs) {
        var builder = TestDescriptors.document();
        for (int i = 0; i < pairs.length; i += 2) builder.setField(TestDescriptors.DOCUMENT.findFieldByName(pairs[i]), pairs[i + 1]);
        return builder;
    }
    private static Object value(Message.Builder builder, String path) {
        String[] parts = path.split("\\.");
        Message current = builder.build();
        for (int i = 0; i < parts.length - 1; i++) current = (Message) current.getField(current.getDescriptorForType().findFieldByName(parts[i]));
        return current.getField(current.getDescriptorForType().findFieldByName(parts[parts.length - 1]));
    }
}
