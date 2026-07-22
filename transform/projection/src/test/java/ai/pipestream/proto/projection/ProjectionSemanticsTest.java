package ai.pipestream.proto.projection;

import ai.pipestream.proto.projection.test.BadParseDoc;
import ai.pipestream.proto.projection.test.CoercionDoc;
import ai.pipestream.proto.projection.test.MapDoc;
import ai.pipestream.proto.projection.test.PresenceDoc;
import ai.pipestream.proto.projection.test.ScalarZoo;
import ai.pipestream.proto.projection.test.SingularFromRepeatedDoc;
import ai.pipestream.proto.projection.test.ZooNested;
import ai.pipestream.proto.projection.test.ZooStatus;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Semantics matrix for {@link MessageProjection}: coercion across scalar kinds,
 * presence rules (proto3 implicit defaults count as absent), oneof and nesting,
 * repeated fields, and the deliberate errors for unsupported or mismatched shapes.
 */
class ProjectionSemanticsTest {

    private static final SourceResolver ZOO = SourceResolver.of(ScalarZoo.getDescriptor());

    private static MessageProjection projection(Descriptor target) {
        return MessageProjection.forTarget(target, ZOO).orElseThrow();
    }

    private static ScalarZoo.Builder zoo() {
        return ScalarZoo.newBuilder()
                .setSmall(7)
                .setBig(9L)
                .setRatio(2.5)
                .setActive(true)
                .setName("not-a-number")
                .setNumericText("42")
                .setStatus(ZooStatus.ZOO_STATUS_OPEN)
                .addScores(1).addScores(2)
                .putCounts("a", 1)
                .setWhen(Timestamp.newBuilder().setSeconds(1752900000L).build())
                .setAttrs(Struct.newBuilder()
                        .putFields("k", Value.newBuilder().setStringValue("v").build()))
                .setNested(ZooNested.newBuilder().setLeaf("deep"));
    }

    private static Object field(DynamicMessage message, Descriptor descriptor, String name) {
        return message.getField(descriptor.findFieldByName(name));
    }

    private static boolean has(DynamicMessage message, Descriptor descriptor, String name) {
        return message.hasField(descriptor.findFieldByName(name));
    }

    @Test
    void coercesScalarsAcrossTheMatrix() {
        ScalarZoo source = zoo().build();
        DynamicMessage out = projection(CoercionDoc.getDescriptor()).project(source);
        Descriptor d = CoercionDoc.getDescriptor();

        assertThat(field(out, d, "widened")).isEqualTo(7L);
        assertThat(field(out, d, "big_as_double")).isEqualTo(9.0);
        assertThat(field(out, d, "big_text")).isEqualTo("9");
        assertThat(field(out, d, "active_text")).isEqualTo("true");
        assertThat(field(out, d, "parsed")).isEqualTo(42);
        assertThat(((EnumValueDescriptor) field(out, d, "status")).getName())
                .isEqualTo("ZOO_STATUS_OPEN");
        assertThat(field(out, d, "when")).isEqualTo(source.getWhen());
        assertThat(field(out, d, "attrs")).isEqualTo(source.getAttrs());
    }

    @Test
    void copiesRepeatedFieldsElementWiseWithWidening() {
        DynamicMessage out = projection(CoercionDoc.getDescriptor()).project(zoo().build());
        assertThat((List<Object>) field(out, CoercionDoc.getDescriptor(), "scores"))
                .containsExactly(1L, 2L);
    }

    @Test
    void wrapsScalarLiteralsIntoValueFields() {
        DynamicMessage out = projection(CoercionDoc.getDescriptor()).project(zoo().build());
        assertThat(((Value) field(out, CoercionDoc.getDescriptor(), "tag")).getStringValue())
                .isEqualTo("v1");
    }

    @Test
    void treatsProto3ImplicitDefaultsAsAbsent() {
        DynamicMessage out = projection(PresenceDoc.getDescriptor())
                .project(zoo().setBig(0L).build());
        // big is present in the Java object but at the proto3 default, so it is absent.
        assertThat(has(out, PresenceDoc.getDescriptor(), "big_when_set")).isFalse();
    }

    @Test
    void honorsExplicitPresence() {
        Descriptor d = PresenceDoc.getDescriptor();

        DynamicMessage unset = projection(d).project(zoo().build());
        assertThat(has(unset, d, "maybe_count")).isFalse();

        DynamicMessage set = projection(d).project(zoo().setMaybeCount(17).build());
        assertThat(has(set, d, "maybe_count")).isTrue();
        assertThat(field(set, d, "maybe_count")).isEqualTo(17);
    }

    @Test
    void readsOnlyTheSetOneofMember() {
        DynamicMessage out = projection(PresenceDoc.getDescriptor())
                .project(zoo().setChoiceNum(5L).build());
        // choice_text is the mapped path, but the oneof member actually set is choice_num.
        assertThat(field(out, PresenceDoc.getDescriptor(), "choice")).isEqualTo("");
    }

    @Test
    void walksNestedPathsAndFallsBackThroughMissingIntermediates() {
        Descriptor d = PresenceDoc.getDescriptor();

        DynamicMessage nested = projection(d).project(zoo().build());
        assertThat(field(nested, d, "nested_leaf")).isEqualTo("deep");

        DynamicMessage flat = projection(d).project(zoo().clearNested().build());
        assertThat(field(flat, d, "nested_leaf")).isEqualTo("");
        // nested.missing.deep dies at the missing intermediate; name is the fallback.
        assertThat(field(flat, d, "fallback")).isEqualTo("not-a-number");
    }

    @Test
    void failsCoercionNamingTheField() {
        assertThatThrownBy(() -> projection(BadParseDoc.getDescriptor()).project(zoo().build()))
                .isInstanceOf(ProjectionException.class)
                .hasMessageContaining("BadParseDoc.parsed");
    }

    @Test
    void rejectsMapFieldsWithADeliberateError() {
        assertThatThrownBy(() -> projection(MapDoc.getDescriptor()).project(zoo().build()))
                .isInstanceOf(ProjectionException.class)
                .hasMessageContaining("Map fields are not yet supported");
    }

    @Test
    void rejectsRepeatedValueIntoSingularField() {
        assertThatThrownBy(() -> projection(SingularFromRepeatedDoc.getDescriptor())
                .project(zoo().build()))
                .isInstanceOf(ProjectionException.class)
                .hasMessageContaining("is a list");
    }
}
