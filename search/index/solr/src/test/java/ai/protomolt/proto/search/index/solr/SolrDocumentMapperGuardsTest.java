package ai.protomolt.proto.search.index.solr;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mapper.ProtoFieldMapperImpl;
import ai.protomolt.proto.search.index.spi.IndexMapping;
import com.google.protobuf.Struct;
import org.junit.jupiter.api.Test;

/** The guard rails: null components fail fast with the parameter named. */
class SolrDocumentMapperGuardsTest {

    private final SolrDocumentMapper mapper =
            new SolrDocumentMapper(new ProtoFieldMapperImpl(new DescriptorRegistry()));

    @Test
    void fieldProjectionRejectsANullPath() {
        assertThatThrownBy(() -> new SolrDocumentMapper.FieldProjection(null, "title_s"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("path");
    }

    @Test
    void fieldProjectionRejectsANullFieldName() {
        assertThatThrownBy(() -> new SolrDocumentMapper.FieldProjection("title", null))
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
