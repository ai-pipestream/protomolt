package ai.protomolt.proto.projection;

import ai.protomolt.proto.projection.test.BadParseDoc;
import ai.protomolt.proto.projection.test.AnyDoc;
import ai.protomolt.proto.projection.test.CoercionDoc;
import ai.protomolt.proto.projection.test.ConflictingOneofDoc;
import ai.protomolt.proto.projection.test.DefaultedDoc;
import ai.protomolt.proto.projection.test.MapDoc;
import ai.protomolt.proto.projection.test.OneofDoc;
import ai.protomolt.proto.projection.test.PresenceDoc;
import ai.protomolt.proto.projection.test.RecursiveDoc;
import ai.protomolt.proto.projection.test.RecursiveNode;
import ai.protomolt.proto.projection.test.ScalarZoo;
import ai.protomolt.proto.projection.test.SingularFromRepeatedDoc;
import ai.protomolt.proto.projection.test.ZooNested;
import ai.protomolt.proto.projection.test.ZooStatus;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Duration;
import com.google.protobuf.Message;
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
                .setNested(ZooNested.newBuilder().setLeaf("deep"))
                .setPacked(Any.pack(ZooNested.newBuilder().setLeaf("packed").build()))
                .setTree(RecursiveNode.newBuilder().setLabel("root")
                        .setChild(RecursiveNode.newBuilder().setLabel("leaf")))
                .putNodes("left", RecursiveNode.newBuilder().setLabel("mapped").build())
                .setElapsed(Duration.newBuilder().setSeconds(12).setNanos(34));
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
    void materializesCelAndLiteralDefaultsWhenPrimaryValuesAreAbsent() {
        DynamicMessage out = projection(DefaultedDoc.getDescriptor()).project(zoo().build());
        Descriptor d = DefaultedDoc.getDescriptor();

        assertThat(field(out, d, "label")).isEqualTo("NOT-A-NUMBER");
        assertThat(field(out, d, "count")).isEqualTo(17);
        assertThat(field(out, d, "category")).isEqualTo("calculated-default");
        assertThat(field(out, d, "short_circuit")).isEqualTo(9L);
    }

    @Test
    void primaryProvenanceWinsWithoutEvaluatingTheDefault() {
        DynamicMessage out = projection(DefaultedDoc.getDescriptor()).project(
                zoo().setChoiceText("explicit").setMaybeCount(23).build());
        Descriptor d = DefaultedDoc.getDescriptor();

        assertThat(field(out, d, "label")).isEqualTo("explicit");
        assertThat(field(out, d, "count")).isEqualTo(23);
        assertThat(field(out, d, "short_circuit")).isEqualTo(9L);
    }

    @Test
    void evaluatesTheDefaultOnlyAfterPrimaryAbsence() {
        assertThatThrownBy(() -> projection(DefaultedDoc.getDescriptor()).project(
                zoo().setBig(0L).build()))
                .isInstanceOf(ProjectionException.class)
                .hasMessageContaining("DefaultedDoc.short_circuit");
    }

    @Test
    void defaultProvenanceParticipatesInDerivedMasks() {
        MessageProjection projection = projection(DefaultedDoc.getDescriptor());

        assertThat(projection.targetMask().getPathsList())
                .containsExactly("label", "count", "category", "short_circuit");
        MessageProjection.SourceMask reads = projection.sourceMask(ScalarZoo.getDescriptor());
        assertThat(reads.fieldMask().getPathsList())
                .containsExactly("choice_text", "maybe_count", "big");
        assertThat(reads.complete()).isFalse();
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
    void projectsScalarAndMessageValuedMaps() throws Exception {
        DynamicMessage out = projection(MapDoc.getDescriptor()).project(zoo().build());
        MapDoc mapped = MapDoc.parseFrom(out.toByteString());

        assertThat(mapped.getCountsMap()).containsEntry("a", 1);
        assertThat(mapped.getNodesMap().get("left").getLabel()).isEqualTo("mapped");
    }

    @Test
    void projectsMapsFromDynamicMessages() throws Exception {
        DynamicMessage source = DynamicMessage.parseFrom(
                ScalarZoo.getDescriptor(), zoo().build().toByteString());
        MapDoc mapped = MapDoc.parseFrom(projection(MapDoc.getDescriptor())
                .project(source).toByteString());

        assertThat(mapped.getCountsMap()).containsEntry("a", 1);
        assertThat(mapped.getNodesMap().get("left").getLabel()).isEqualTo("mapped");
    }

    @Test
    void duplicateMapKeysUseTheLastEntryLikeProtobufParsing() throws Exception {
        Descriptor sourceType = ScalarZoo.getDescriptor();
        FieldDescriptor counts = sourceType.findFieldByName("counts");
        FieldDescriptor key = counts.getMessageType().findFieldByName("key");
        FieldDescriptor value = counts.getMessageType().findFieldByName("value");
        DynamicMessage first = DynamicMessage.newBuilder(counts.getMessageType())
                .setField(key, "same").setField(value, 1).build();
        DynamicMessage second = DynamicMessage.newBuilder(counts.getMessageType())
                .setField(key, "same").setField(value, 2).build();
        DynamicMessage source = DynamicMessage.newBuilder(sourceType)
                .setField(sourceType.findFieldByName("small"), 7)
                .addRepeatedField(counts, first)
                .addRepeatedField(counts, second)
                .build();

        MapDoc mapped = MapDoc.parseFrom(projection(MapDoc.getDescriptor())
                .project(source).toByteString());
        assertThat(mapped.getCountsMap()).containsExactlyEntriesOf(java.util.Map.of("same", 2));
    }

    @Test
    void projectsExactlyOneTargetOneofMember() {
        DynamicMessage text = projection(OneofDoc.getDescriptor())
                .project(zoo().setChoiceText("selected").build());
        assertThat(text.getOneofFieldDescriptor(
                OneofDoc.getDescriptor().getOneofs().getFirst()).getName())
                .isEqualTo("label");

        DynamicMessage number = projection(OneofDoc.getDescriptor())
                .project(zoo().setChoiceNum(19).build());
        assertThat(number.getOneofFieldDescriptor(
                OneofDoc.getDescriptor().getOneofs().getFirst()).getName())
                .isEqualTo("amount");
    }

    @Test
    void refusesTwoWritesToOneTargetOneof() {
        assertThatThrownBy(() -> projection(ConflictingOneofDoc.getDescriptor())
                .project(zoo().build()))
                .isInstanceOf(ProjectionException.class)
                .hasMessageContaining("ConflictingOneofDoc.label")
                .hasMessageContaining("ConflictingOneofDoc.amount")
                .hasMessageContaining("ConflictingOneofDoc.selection");
    }

    @Test
    void projectsAnyWithoutConvertingItsPayloadToStruct() throws Exception {
        AnyDoc out = AnyDoc.parseFrom(projection(AnyDoc.getDescriptor())
                .project(zoo().build()).toByteString());
        assertThat(out.getPacked().getTypeUrl())
                .endsWith(ZooNested.getDescriptor().getFullName());
        assertThat(out.getPacked().unpack(ZooNested.class).getLeaf()).isEqualTo("packed");
    }

    @Test
    void projectsRecursiveMessagesAndDuration() throws Exception {
        RecursiveDoc out = RecursiveDoc.parseFrom(projection(RecursiveDoc.getDescriptor())
                .project(zoo().build()).toByteString());
        assertThat(out.getTree().getChild().getLabel()).isEqualTo("leaf");
        assertThat(out.getElapsed()).isEqualTo(
                Duration.newBuilder().setSeconds(12).setNanos(34).build());
    }

    @Test
    void preservesUnknownFieldsOnPassThroughMessages() {
        RecursiveNode tree = RecursiveNode.newBuilder().setLabel("root")
                .setUnknownFields(com.google.protobuf.UnknownFieldSet.newBuilder()
                        .addField(99, com.google.protobuf.UnknownFieldSet.Field.newBuilder()
                                .addLengthDelimited(ByteString.copyFromUtf8("future")).build())
                        .build())
                .build();
        DynamicMessage out = projection(RecursiveDoc.getDescriptor())
                .project(zoo().setTree(tree).build());
        Message projectedTree = (Message) field(out, RecursiveDoc.getDescriptor(), "tree");
        assertThat(projectedTree.getUnknownFields().hasField(99)).isTrue();
    }

    @Test
    void rejectsRepeatedValueIntoSingularField() {
        assertThatThrownBy(() -> projection(SingularFromRepeatedDoc.getDescriptor())
                .project(zoo().build()))
                .isInstanceOf(ProjectionException.class)
                .hasMessageContaining("is a list");
    }

}
