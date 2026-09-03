package ai.protomolt.proto.validate.conformance;

import ai.protomolt.proto.validate.conformance.testdata.v1.Cases.Person;
import build.buf.validate.ValidateProto;
import build.buf.validate.Violation;
import buf.validate.conformance.harness.Harness.TestConformanceRequest;
import buf.validate.conformance.harness.Harness.TestConformanceResponse;
import buf.validate.conformance.harness.Harness.TestResult;
import com.google.protobuf.Any;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises the phase-2 executor protocol end to end: build a {@link TestConformanceRequest},
 * serialize and re-parse it exactly as {@link ConformanceMain#main} does, then run it through
 * {@link ConformanceMain#process}.
 *
 * <p>The load-bearing assertion is that the {@code buf.validate} rule annotations carried in the
 * request's {@code fdset} survive the wire round-trip and drive validation — which only holds if
 * the request is parsed with the extension registry and those options reach the linked descriptors.
 */
class ConformanceMainTest {

    private static ExtensionRegistry registry() {
        ExtensionRegistry registry = ExtensionRegistry.newInstance();
        ValidateProto.registerAllExtensions(registry);
        return registry;
    }

    private static FileDescriptorSet fdsetOf(FileDescriptor file) {
        Map<String, FileDescriptorProto> acc = new LinkedHashMap<>();
        collect(file, acc);
        FileDescriptorSet.Builder set = FileDescriptorSet.newBuilder();
        acc.values().forEach(set::addFile);
        return set.build();
    }

    private static void collect(FileDescriptor file, Map<String, FileDescriptorProto> acc) {
        for (FileDescriptor dep : file.getDependencies()) {
            collect(dep, acc);
        }
        acc.putIfAbsent(file.getName(), file.toProto());
    }

    private static Any anyOf(Message message) {
        return Any.newBuilder()
                .setTypeUrl("type.googleapis.com/" + message.getDescriptorForType().getFullName())
                .setValue(message.toByteString())
                .build();
    }

    @Test
    void rulesSurviveTheWireRoundTrip() throws Exception {
        ExtensionRegistry registry = registry();
        Person valid = Person.newBuilder().setAge(18).setName("ok").addTags("x").build();
        Person invalid = Person.newBuilder().setAge(10).setName("ok").addTags("x").build();

        TestConformanceRequest request = TestConformanceRequest.newBuilder()
                .setFdset(fdsetOf(Person.getDescriptor().getFile()))
                .putCases("valid", anyOf(valid))
                .putCases("invalid", anyOf(invalid))
                .build();

        // Round-trip through bytes and re-parse with the registry, exactly as main() does.
        TestConformanceRequest onWire =
                TestConformanceRequest.parseFrom(request.toByteArray(), registry);
        TestConformanceResponse response = ConformanceMain.process(onWire, registry);

        assertThat(response.getResultsMap()).containsOnlyKeys("valid", "invalid");
        assertThat(response.getResultsOrThrow("valid").getSuccess()).isTrue();

        TestResult inv = response.getResultsOrThrow("invalid");
        assertThat(inv.getResultCase()).isEqualTo(TestResult.ResultCase.VALIDATION_ERROR);
        assertThat(inv.getValidationError().getViolationsCount()).isEqualTo(1);
        Violation v = inv.getValidationError().getViolations(0);
        assertThat(v.getRuleId()).isEqualTo("int32.gte");
        assertThat(v.getField().getElements(0).getFieldName()).isEqualTo("age");
        assertThat(v.getRule().getElements(0).getFieldName()).isEqualTo("int32");
    }

    @Test
    void unknownCaseTypeReportsCompilationError() throws Exception {
        ExtensionRegistry registry = registry();
        TestConformanceRequest request = TestConformanceRequest.newBuilder()
                .setFdset(fdsetOf(Person.getDescriptor().getFile()))
                .putCases("mystery", Any.newBuilder()
                        .setTypeUrl("type.googleapis.com/does.not.Exist")
                        .build())
                .build();

        TestConformanceResponse response = ConformanceMain.process(request, registry);
        assertThat(response.getResultsOrThrow("mystery").getResultCase())
                .isEqualTo(TestResult.ResultCase.COMPILATION_ERROR);
    }
}
