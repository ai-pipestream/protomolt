package ai.protomolt.proto.search.index.opensearch;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mapper.ProtoFieldMapperImpl;
import ai.protomolt.proto.search.index.spi.IndexMapping;
import com.google.protobuf.Struct;
import org.junit.jupiter.api.Test;

/** The guard rails: null components fail fast with the parameter named. */
class OpenSearchDocumentMapperGuardsTest {

    private final OpenSearchDocumentMapper mapper =
            new OpenSearchDocumentMapper(new ProtoFieldMapperImpl(new DescriptorRegistry()));

    @Test
    void fieldProjectionRejectsANullPath() {
        assertThatThrownBy(() -> new OpenSearchDocumentMapper.FieldProjection(null, "title"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("path");
    }

    @Test
    void fieldProjectionRejectsANullFieldName() {
        assertThatThrownBy(() -> new OpenSearchDocumentMapper.FieldProjection("title", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("fieldName");
    }

    @Test
    void mapRejectsANullMapping() {
        assertThatThrownBy(() -> mapper.map(Struct.getDefaultInstance(), (IndexMapping) null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("mapping");
    }
}
