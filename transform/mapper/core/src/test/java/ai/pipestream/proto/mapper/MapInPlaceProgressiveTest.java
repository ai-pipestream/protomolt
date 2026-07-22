package ai.pipestream.proto.mapper;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class MapInPlaceProgressiveTest {
    private final ProtoFieldMapper mapper;

    MapInPlaceProgressiveTest() {
        var registry = DescriptorRegistry.create();
        registry.registerFile(TestDescriptors.FILE);
        mapper = new ProtoFieldMapperImpl(registry);
    }

    @Test void laterAssignmentReadsEarlierAssignment() throws Exception {
        var document = TestDescriptors.document().setField(TestDescriptors.DOCUMENT.findFieldByName("title"), "one");
        mapper.mapInPlace(document, List.of("body = title", "language = body"));
        assertEquals("one", mapper.getValue(document, "language"));
    }
    @Test void clearRemovesEarlierValue() throws Exception {
        var document = TestDescriptors.document().setField(TestDescriptors.DOCUMENT.findFieldByName("title"), "one");
        mapper.mapInPlace(document, List.of("body = title", "-title"));
        assertNull(mapper.getValue(document, "title"));
        assertEquals("one", mapper.getValue(document, "body"));
    }
    @Test void appendedValuesRemainVisible() throws Exception {
        var document = TestDescriptors.document().setField(TestDescriptors.DOCUMENT.findFieldByName("title"), "one");
        mapper.mapInPlace(document, List.of("tags += title", "tags += title"));
        assertEquals(2, ((List<?>) mapper.getValue(document, "tags")).size());
    }
}
