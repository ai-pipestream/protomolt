package ai.protomolt.proto.shapes;

import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Merge edges beyond the order+ticket flow: the two-source minimum, map rejection,
 * cardinality clashes, custom rename targets, explicit coalesce and prefer on compatible
 * fields, message-typed clashes, and a three-way merge.
 */
class SchemaMergerEdgeCasesTest {

    private static final String ALPHA_PROTO = """
            syntax = "proto3";
            package merge.a;
            message Alpha {
              string id = 1;
              repeated string codes = 2;
              Shared thing = 3;
            }
            message Shared {
              string a_field = 1;
            }
            message Delta {
              string id = 1;
            }
            """;

    private static final String BETA_PROTO = """
            syntax = "proto3";
            package merge.b;
            message Beta {
              string id = 1;
              string codes = 2;
              Shared thing = 3;
            }
            message Shared {
              string b_field = 1;
            }
            message Gamma {
              string id = 1;
              int64 size = 2;
            }
            """;

    private static Descriptor alpha;
    private static Descriptor beta;
    private static Descriptor gamma;
    private static Descriptor delta;

    private final SchemaMerger merger = new SchemaMerger();

    @BeforeAll
    static void compile() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("merge/a/alpha.proto", ALPHA_PROTO, "test")
                .add("merge/b/beta.proto", BETA_PROTO, "test")
                .build());
        var aFile = compiled.descriptorFor("merge/a/alpha.proto").orElseThrow();
        var bFile = compiled.descriptorFor("merge/b/beta.proto").orElseThrow();
        alpha = aFile.findMessageTypeByName("Alpha");
        delta = aFile.findMessageTypeByName("Delta");
        beta = bFile.findMessageTypeByName("Beta");
        gamma = bFile.findMessageTypeByName("Gamma");
    }

    private static List<ShapeSynthesizer.NamedType> alphaBeta() {
        return List.of(new ShapeSynthesizer.NamedType("alpha", alpha),
                new ShapeSynthesizer.NamedType("beta", beta));
    }

    @Test
    void aMergeNeedsAtLeastTwoSources() {
        assertThatThrownBy(() -> merger.merge("derived.v1.X",
                List.of(new ShapeSynthesizer.NamedType("alpha", alpha)), Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least two sources");
    }

    @Test
    void mapFieldsAreRejectedUpFront() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("merge/mapped/mapped.proto", """
                        syntax = "proto3";
                        package merge.mapped;
                        message Mapped {
                          map<string, string> attrs = 1;
                        }
                        message Plain {
                          string id = 1;
                        }
                        """, "test").build());
        var file = compiled.descriptorFor("merge/mapped/mapped.proto").orElseThrow();
        List<ShapeSynthesizer.NamedType> sources = List.of(
                new ShapeSynthesizer.NamedType("mapped", file.findMessageTypeByName("Mapped")),
                new ShapeSynthesizer.NamedType("plain", file.findMessageTypeByName("Plain")));
        assertThatThrownBy(() -> merger.merge("derived.v1.X", sources, Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Map fields are not yet mergeable")
                .hasMessageContaining("mapped.attrs");
    }

    @Test
    void sameTypeDifferentCardinalityIsACardinalityClash() {
        SchemaMerger.MergeResult result = merger.merge("derived.v1.X", alphaBeta(), Map.of());
        assertThat(result.resolved()).isFalse();
        var byField = result.clashes().stream()
                .collect(java.util.stream.Collectors.toMap(SchemaMerger.Clash::field, c -> c));
        // Alpha.codes is repeated, Beta.codes is singular: same type, different cardinality.
        var codes = byField.get("codes");
        assertThat(codes.kind()).isEqualTo(SchemaMerger.ClashKind.CARDINALITY_CLASH);
        assertThat(codes.suggested().action()).isEqualTo("rename");
        assertThat(codes.suggested().names())
                .containsEntry("alpha", "alpha_codes")
                .containsEntry("beta", "beta_codes");
        assertThat(codes.origins()).extracting(SchemaMerger.Origin::display)
                .containsExactly("repeated string", "string");
    }

    @Test
    void messageFieldsOfDifferentTypesClashWithBothNamesInTheReport() {
        SchemaMerger.MergeResult result = merger.merge("derived.v1.X", alphaBeta(), Map.of());
        var thing = result.clashes().stream()
                .filter(c -> c.field().equals("thing")).findFirst().orElseThrow();
        assertThat(thing.kind()).isEqualTo(SchemaMerger.ClashKind.TYPE_CLASH);
        assertThat(thing.origins()).extracting(SchemaMerger.Origin::display)
                .containsExactly("merge.a.Shared", "merge.b.Shared");
    }

    @Test
    void renameResolutionHonoursCustomTargetsAndDefaultsTheRest() {
        SchemaMerger.MergeResult result = merger.merge("derived.v1.X", alphaBeta(),
                Map.of("codes", new SchemaMerger.Resolution("rename", null, Map.of()),
                        "thing", new SchemaMerger.Resolution("rename", null,
                                Map.of("alpha", "alpha_thing_custom"))));
        assertThat(result.resolved()).isTrue();
        Descriptor type = result.shape().type();
        // The custom target wins for alpha; beta falls back to <source>_<field>.
        assertThat(type.getFields()).extracting(FieldDescriptor::getName)
                .contains("alpha_codes", "beta_codes", "alpha_thing_custom", "beta_thing");
        assertThat(type.findFieldByName("alpha_codes").isRepeated()).isTrue();
        assertThat(type.findFieldByName("beta_codes").isRepeated()).isFalse();
        assertThat(type.findFieldByName("beta_thing").getMessageType().getFullName())
                .isEqualTo("merge.b.Shared");
        assertThat(result.shape().impliedRules())
                .contains("alpha_thing_custom = alpha.thing", "beta_thing = beta.thing");
    }

    @Test
    void explicitCoalesceOnACompatibleFieldResolves() {
        SchemaMerger.MergeResult result = merger.merge("derived.v1.X", alphaBeta(),
                Map.of("id", new SchemaMerger.Resolution("coalesce", null, Map.of()),
                        "codes", new SchemaMerger.Resolution("rename", null, Map.of()),
                        "thing", new SchemaMerger.Resolution("rename", null, Map.of())));
        assertThat(result.resolved()).isTrue();
        assertThat(result.shape().type().findFieldByName("id")).isNotNull();
        assertThat(result.shape().impliedRules())
                .contains("id = alpha.id", "id = beta.id");
    }

    @Test
    void preferOnACoalescedFieldKeepsOnlyTheWinningSource() {
        SchemaMerger.MergeResult result = merger.merge("derived.v1.X", alphaBeta(),
                Map.of("id", new SchemaMerger.Resolution("prefer", "alpha", Map.of()),
                        "codes", new SchemaMerger.Resolution("rename", null, Map.of()),
                        "thing", new SchemaMerger.Resolution("rename", null, Map.of())));
        assertThat(result.resolved()).isTrue();
        assertThat(result.shape().impliedRules())
                .contains("id = alpha.id")
                .doesNotContain("id = beta.id");
        // The union rules for beta lose the field entirely: it is not beta's anymore.
        assertThat(result.unionRules().get("beta"))
                .noneMatch(rule -> rule.startsWith("id"));
        assertThat(result.unionRules().get("alpha")).contains("id = alpha.id");
    }

    @Test
    void threeWayMergesCoalesceAcrossEverySource() {
        SchemaMerger.MergeResult result = merger.merge("derived.v1.X",
                List.of(new ShapeSynthesizer.NamedType("alpha", delta),
                        new ShapeSynthesizer.NamedType("beta", gamma),
                        new ShapeSynthesizer.NamedType("delta", alpha)),
                // alpha here is Delta: only 'id'. The real Alpha brings 'thing' and
                // repeated 'codes'... which nothing else declares, so no hard clash.
                Map.of());
        assertThat(result.resolved()).isTrue();
        assertThat(result.shape().impliedRules())
                .contains("id = alpha.id", "id = beta.id", "id = delta.id");
        assertThat(result.unionRules()).containsOnlyKeys("alpha", "beta", "delta");
        assertThat(result.unionRules().get("beta"))
                .containsExactly("id = beta.id", "size = beta.size");
    }

    @Test
    void resolutionsRequireTheContributingSourceToBeNamed() {
        assertThatThrownBy(() -> merger.merge("derived.v1.X", alphaBeta(),
                Map.of("thing", new SchemaMerger.Resolution("prefer", null, Map.of()))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'source' among the field's contributors");
        assertThatThrownBy(() -> merger.merge("derived.v1.X", alphaBeta(),
                Map.of("codes", new SchemaMerger.Resolution("shuffle", null, Map.of()))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown resolution action 'shuffle'");
    }
}
