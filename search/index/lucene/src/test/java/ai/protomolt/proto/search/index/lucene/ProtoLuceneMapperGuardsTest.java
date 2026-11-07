package ai.protomolt.proto.search.index.lucene;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mapper.ProtoFieldMapperImpl;
import ai.protomolt.proto.search.index.spi.IndexMapping;
import com.google.protobuf.Struct;
import org.junit.jupiter.api.Test;

/** The guard rails: null components fail fast with the parameter named. */
class ProtoLuceneMapperGuardsTest {

    private final ProtoLuceneMapper mapper =
            new ProtoLuceneMapper(new ProtoFieldMapperImpl(new DescriptorRegistry()));

    @Test
    void fieldProjectionRejectsANullPath() {
        assertThatThrownBy(() -> new ProtoLuceneMapper.FieldProjection(null, "title", true, true))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("path");
    }

    @Test
    void fieldProjectionRejectsANullFieldName() {
        assertThatThrownBy(() -> new ProtoLuceneMapper.FieldProjection("title", null, true, true))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("luceneFieldName");
    }

    @Test
    void mapRejectsANullMapping() {
        assertThatThrownBy(() -> mapper.map(Struct.getDefaultInstance(), (IndexMapping) null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("mapping");
    }
}
