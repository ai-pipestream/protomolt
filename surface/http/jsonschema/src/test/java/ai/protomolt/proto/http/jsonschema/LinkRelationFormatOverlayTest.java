package ai.protomolt.proto.http.jsonschema;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.validate.model.StringConstraints;
import ai.protomolt.proto.validate.model.StringFormat;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * JSON Schema has no link-relation format keyword, so {@code link_relation} surfaces as the
 * descriptive {@code x-protomolt-format} keyword naming its rule id, alongside a standard
 * {@code format} when both are declared.
 */
class LinkRelationFormatOverlayTest {

    @Test
    void linkRelationSurfacesAsTheDescriptiveFormatKeyword() {
        Map<String, Object> overlay = ConstraintOverlays.stringOverlay(StringConstraints.builder()
                .format(StringFormat.LINK_RELATION)
                .build());

        assertThat(overlay).doesNotContainKey("format");
        assertThat(overlay).containsEntry("allOf",
                List.of(Map.of("x-protomolt-format", "string.link_relation")));
    }

    @Test
    void linkRelationComposesWithAStandardFormat() {
        Map<String, Object> overlay = ConstraintOverlays.stringOverlay(StringConstraints.builder()
                .format(StringFormat.URI)
                .format(StringFormat.LINK_RELATION)
                .build());

        assertThat(overlay).containsEntry("format", "uri");
        assertThat(overlay).containsEntry("allOf",
                List.of(Map.of("x-protomolt-format", "string.link_relation")));
    }
}
