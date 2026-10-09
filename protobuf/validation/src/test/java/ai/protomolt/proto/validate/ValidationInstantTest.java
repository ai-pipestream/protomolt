package ai.protomolt.proto.validate;

import ai.protomolt.proto.validate.model.*;
import ai.protomolt.proto.validate.spi.ValidationRuleSource;
import com.google.protobuf.DescriptorProtos.*;
import com.google.protobuf.Descriptors.*;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.*;

class ValidationInstantTest {
    private static final Instant FIRST = Instant.parse("2000-01-01T00:00:00Z");
    private static final Instant SECOND = Instant.parse("2100-01-01T00:00:00Z");

    @Test void suppliedInstantReachesNestedCollectionAndCelRules() throws Exception {
        var type = type();
        var validator = validator();
        var candidate = candidate(type, FIRST);
        assertThat(validator.validate(candidate, FIRST).valid()).isTrue();
        assertThat(validator.validate(candidate, FIRST.minusSeconds(1)).violations())
                .anyMatch(v -> v.path().equals("stamp") && v.ruleId().equals("past"))
                .anyMatch(v -> v.path().equals("stamps[0]") && v.ruleId().equals("past"))
                .anyMatch(v -> v.path().equals("by_key[\"a\"]") && v.ruleId().equals("past"));
        var changed = validator.validate(candidate, FIRST.plusSeconds(5));
        assertThat(changed.valid()).isFalse();
        assertThat(changed.violations()).anyMatch(v -> v.path().equals("stamp") && v.ruleId().equals("timestamp.within"))
                .anyMatch(v -> v.path().equals("stamps[0]") && v.ruleId().equals("timestamp.within"))
                .anyMatch(v -> v.path().equals("by_key[\"a\"]") && v.ruleId().equals("timestamp.within"))
                .anyMatch(v -> v.path().equals("child.stamp") && v.ruleId().equals("timestamp.within"))
                .anyMatch(v -> v.path().equals("child") && v.ruleId().equals("expected.instant"));
        assertThat(validator.validate(candidate, FIRST)).isEqualTo(validator.validate(candidate, FIRST));
        assertThatThrownBy(() -> validator.validate(candidate, null)).isInstanceOf(NullPointerException.class);
    }

    @Test void concurrentChecksDoNotShareEvaluationTime() throws Exception {
        var type = type();
        var validator = validator();
        var first = candidate(type, FIRST);
        var second = candidate(type, SECOND);
        try (var executor = Executors.newFixedThreadPool(4)) {
            var tasks = new java.util.ArrayList<Callable<Boolean>>();
            for (int i = 0; i < 40; i++) {
                boolean useFirst = i % 2 == 0;
                tasks.add(() -> validator.validate(useFirst ? first : second, useFirst ? FIRST : SECOND).valid());
            }
            for (var result : executor.invokeAll(tasks)) assertThat(result.get()).isTrue();
        }
    }

    @Test void timestampBoundsRemainStrictAtTheEvaluationInstant() throws Exception {
        var type = type();
        var candidate = candidate(type, FIRST);
        for (boolean future : List.of(false, true)) {
            var validator = ProtoValidator.create(List.of(new ValidationRuleSource() {
                @Override public Optional<FieldConstraints> fieldConstraints(FieldDescriptor field) {
                    return field.getName().equals("stamp") ? Optional.of(FieldConstraints.builder()
                            .timestamp(new TimestampConstraints(Optional.empty(), Optional.empty(), Optional.empty(),
                                    Optional.empty(), Optional.empty(), !future, future, Optional.empty())).build()) : Optional.empty();
                }
                @Override public Optional<MessageConstraints> messageConstraints(Descriptor message) { return Optional.empty(); }
            }));
            var equal = validator.validate(candidate, FIRST.minusSeconds(1));
            assertThat(equal.violations()).extracting(ValidationResult.Violation::ruleId)
                    .containsOnly(future ? "timestamp.gt_now" : "timestamp.lt_now");
            assertThat(validator.validate(candidate, future ? FIRST.minusSeconds(2) : FIRST).valid()).isTrue();
        }
    }

    private static ProtoValidator validator() {
        var temporal = FieldConstraints.builder().timestamp(new TimestampConstraints(
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                true, false, Optional.of(Duration.ofSeconds(2))))
                .addCel(new CelConstraint("past", "this < now", "must precede evaluation")).build();
        return ProtoValidator.create(List.of(new ValidationRuleSource() {
            @Override public Optional<FieldConstraints> fieldConstraints(FieldDescriptor field) {
                return switch (field.getName()) {
                    case "stamp" -> Optional.of(temporal);
                    case "stamps" -> Optional.of(FieldConstraints.builder().repeated(new RepeatedConstraints(
                            java.util.OptionalLong.empty(), java.util.OptionalLong.empty(), false, Optional.of(temporal))).build());
                    case "by_key" -> Optional.of(FieldConstraints.builder().map(new MapConstraints(
                            java.util.OptionalLong.empty(), java.util.OptionalLong.empty(), Optional.empty(), Optional.of(temporal))).build());
                    default -> Optional.empty();
                };
            }
            @Override public Optional<MessageConstraints> messageConstraints(Descriptor message) {
                return message.getFullName().equals("instant.Node") ? Optional.of(new MessageConstraints(List.of(
                        new CelConstraint("expected.instant", "this.expected == now", "wrong evaluation instant")))) : Optional.empty();
            }
        }));
    }

    private static Descriptor type() throws Exception {
        var entry = DescriptorProto.newBuilder().setName("ByKeyEntry").setOptions(MessageOptions.newBuilder().setMapEntry(true))
                .addField(FieldDescriptorProto.newBuilder().setName("key").setNumber(1).setType(FieldDescriptorProto.Type.TYPE_STRING))
                .addField(field("value", 2, ".google.protobuf.Timestamp", false));
        var node = DescriptorProto.newBuilder().setName("Node").addNestedType(entry)
                .addField(field("expected", 1, ".google.protobuf.Timestamp", false))
                .addField(field("stamp", 2, ".google.protobuf.Timestamp", false))
                .addField(field("stamps", 3, ".google.protobuf.Timestamp", true))
                .addField(field("by_key", 4, ".instant.Node.ByKeyEntry", true))
                .addField(field("child", 5, ".instant.Node", false));
        return FileDescriptor.buildFrom(FileDescriptorProto.newBuilder().setName("validation_instant.proto")
                .setPackage("instant").setSyntax("proto3").addDependency("google/protobuf/timestamp.proto")
                .addMessageType(node).build(), new FileDescriptor[]{Timestamp.getDescriptor().getFile()}).findMessageTypeByName("Node");
    }

    private static FieldDescriptorProto.Builder field(String name, int number, String type, boolean repeated) {
        return FieldDescriptorProto.newBuilder().setName(name).setNumber(number).setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setTypeName(type).setLabel(repeated ? FieldDescriptorProto.Label.LABEL_REPEATED : FieldDescriptorProto.Label.LABEL_OPTIONAL);
    }

    private static DynamicMessage candidate(Descriptor type, Instant now) {
        var expected = Timestamp.newBuilder().setSeconds(now.getEpochSecond()).build();
        var stamp = Timestamp.newBuilder().setSeconds(now.minusSeconds(1).getEpochSecond()).build();
        var child = DynamicMessage.newBuilder(type).setField(type.findFieldByName("expected"), expected)
                .setField(type.findFieldByName("stamp"), stamp).build();
        var entryType = type.findNestedTypeByName("ByKeyEntry");
        var entry = DynamicMessage.newBuilder(entryType).setField(entryType.findFieldByName("key"), "a")
                .setField(entryType.findFieldByName("value"), stamp).build();
        return child.toBuilder().addRepeatedField(type.findFieldByName("stamps"), stamp)
                .addRepeatedField(type.findFieldByName("by_key"), entry).setField(type.findFieldByName("child"), child).build();
    }
}
