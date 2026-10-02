package ai.protomolt.proto.validate.protovalidate;

import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import ai.protomolt.proto.validate.protovalidate.testdata.PredefinedUser;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Predefined rules — {@code (buf.validate.predefined)} CEL expressions declared as extensions on
 * the {@code buf.validate.<T>Rules} messages — reach a field through its file's imports, and the
 * value the field configures binds as {@code rule} in the expression.
 *
 * <p>Two things are worth pinning beyond "the rule fires". The configured value has to survive the
 * round trip: a user extension is unknown to the generated {@code buf.validate} types, so it lands
 * as an unknown field and is recovered by reparsing the sub-rules bytes. And the rule has to be
 * found at all, which depends on walking the file's transitive imports rather than the file alone.
 * The second case is checked twice — once on generated descriptors and once on descriptors relinked
 * from a {@code FileDescriptorSet} without the extension registry, the path where annotations
 * survive only as unknown fields.
 */
class PredefinedRulesTest {

    private static final ProtoValidator VALIDATOR = ProtoValidator.create(
            List.of(new ProtovalidateRuleSource()));

    private static PredefinedUser.Builder valid() {
        return PredefinedUser.newBuilder()
                .setHandle("ada")
                .setLabel("Mixed Case")
                .setSeats(10)
                .setPlain("ok");
    }

    private static void assertViolation(Message message, String path, String ruleId) {
        assertThat(VALIDATOR.validate(message).violations())
                .as("expected %s at %s", ruleId, path)
                .anyMatch(v -> v.path().equals(path) && v.ruleId().equals(ruleId));
    }

    @Test
    void validMessagePasses() {
        ValidationResult result = VALIDATOR.validate(valid().build());
        assertThat(result.valid())
                .as("expected no violations, got %s", result.violations())
                .isTrue();
    }

    @Test
    void predefinedFlagRuleFires() {
        assertViolation(valid().setHandle("Ada").build(), "handle", "string.lowercase_only");
    }

    @Test
    void predefinedRuleCarriesItsMessage() {
        assertThat(VALIDATOR.validate(valid().setHandle("Ada").build()).violations())
                .filteredOn(v -> v.ruleId().equals("string.lowercase_only"))
                .singleElement()
                .satisfies(v -> assertThat(v.message()).isEqualTo("must be lowercase"));
    }

    @Test
    void predefinedRuleSetFalseIsSatisfied() {
        // `label` carries the same rule with rule=false, so the expression short-circuits.
        assertThat(VALIDATOR.validate(valid().setLabel("UPPER").build()).violations())
                .noneMatch(v -> v.path().equals("label"));
    }

    @Test
    void predefinedValuedRuleBindsItsConfiguredValue() {
        // seats sets multiple_of=5; 7 is not a multiple of 5, 10 is. A rule binding that lost the
        // configured value would either fail to compile or divide by zero.
        assertViolation(valid().setSeats(7).build(), "seats", "int32.multiple_of");
        assertThat(VALIDATOR.validate(valid().setSeats(10).build()).violations())
                .noneMatch(v -> v.ruleId().equals("int32.multiple_of"));
    }

    @Test
    void standardRulesStillApplyAlongsidePredefinedOnes() {
        assertViolation(valid().setHandle("a").build(), "handle", "string.min_len");
        assertViolation(valid().setSeats(-5).build(), "seats", "int32.gte");
    }

    @Test
    void fieldWithoutPredefinedRulesIsUnaffected() {
        assertViolation(valid().setPlain("a").build(), "plain", "string.min_len");
        // Uppercase would trip lowercase_only had the rule leaked across fields; it does not.
        assertThat(VALIDATOR.validate(valid().setPlain("AB").build()).violations())
                .noneMatch(v -> v.path().equals("plain"));
    }

    /**
     * The same checks against descriptors relinked from a {@link FileDescriptorSet} parsed without
     * the {@code buf.validate} extension registry: both the field annotation and the predefined
     * declaration survive only as unknown fields and have to be reparsed.
     */
    @Test
    void predefinedRulesSurviveRelinkedDescriptors() {
        Descriptor relinked = relink(PredefinedUser.getDescriptor());
        DynamicMessage bad = DynamicMessage.newBuilder(relinked)
                .setField(relinked.findFieldByName("handle"), "Ada")
                .setField(relinked.findFieldByName("seats"), 7)
                .setField(relinked.findFieldByName("plain"), "ok")
                .build();
        assertViolation(bad, "handle", "string.lowercase_only");
        assertViolation(bad, "seats", "int32.multiple_of");

        DynamicMessage good = DynamicMessage.newBuilder(relinked)
                .setField(relinked.findFieldByName("handle"), "ada")
                .setField(relinked.findFieldByName("label"), "UPPER")
                .setField(relinked.findFieldByName("seats"), 10)
                .setField(relinked.findFieldByName("plain"), "ok")
                .build();
        ValidationResult result = VALIDATOR.validate(good);
        assertThat(result.valid())
                .as("expected no violations, got %s", result.violations())
                .isTrue();
    }

    /** Rebuilds a descriptor through a FileDescriptorSet, dropping extension knowledge. */
    private static Descriptor relink(Descriptor original) {
        FileDescriptorSet.Builder set = FileDescriptorSet.newBuilder();
        collect(original.getFile(), new ArrayList<>(), set);
        Map<String, FileDescriptor> linked = new HashMap<>();
        FileDescriptor file = null;
        for (FileDescriptorProto proto : set.getFileList()) {
            file = link(proto, linked);
        }
        return file == null ? original : file.findMessageTypeByName(original.getName());
    }

    private static void collect(
            FileDescriptor file, List<String> seen, FileDescriptorSet.Builder set) {
        if (seen.contains(file.getFullName())) {
            return;
        }
        seen.add(file.getFullName());
        for (FileDescriptor dep : file.getDependencies()) {
            collect(dep, seen, set);
        }
        // Parsing the proto's bytes with no registry is what demotes the recognised buf.validate
        // extensions back to unknown fields, which is the condition under test.
        try {
            set.addFile(FileDescriptorProto.parseFrom(file.toProto().toByteString()));
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("cannot reparse " + file.getFullName(), e);
        }
    }

    private static FileDescriptor link(FileDescriptorProto proto, Map<String, FileDescriptor> done) {
        FileDescriptor existing = done.get(proto.getName());
        if (existing != null) {
            return existing;
        }
        FileDescriptor[] deps = new FileDescriptor[proto.getDependencyCount()];
        for (int i = 0; i < deps.length; i++) {
            deps[i] = done.get(proto.getDependency(i));
        }
        try {
            FileDescriptor linked = FileDescriptor.buildFrom(proto, deps);
            done.put(proto.getName(), linked);
            return linked;
        } catch (com.google.protobuf.Descriptors.DescriptorValidationException e) {
            throw new IllegalStateException("cannot relink " + proto.getName(), e);
        }
    }
}
