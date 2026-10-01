package ai.protomolt.proto.samples;

import ai.protomolt.proto.samples.authoring.v1.AuthoringFixture;
import ai.protomolt.proto.samples.authoring.v1.WriteRecordRequest;
import ai.protomolt.proto.workflow.CompiledWorkflow;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AuthoringStarterTemplateTest {
    @Test void acceptsTheInputPreservingTemplate() {
        assertThatCode(() -> AuthoringStarterTemplate.verify(workflow(normalize(), write(
                List.of("operation_id = input.operation_id", "content = normalize.text")), true)))
                .doesNotThrowAnyException();
    }

    @Test void rejectsFixedKeysAndContentThatBypassesNormalization() {
        for (var rules : List.of(
                List.of("operation_id = '00000000-0000-4000-8000-000000000001'", "content = normalize.text"),
                List.of("operation_id = input.operation_id", "content = input.content"),
                List.of("operation_id = input.operation_id", "content = normalize.text", "content = input.content"))) {
            rejected(workflow(normalize(), write(rules), true));
        }
    }

    @Test void rejectsReorderedSkippedAndUnvalidatedWorkflows() {
        var write = write(List.of("operation_id = input.operation_id", "content = normalize.text"));
        rejected(workflow(write, normalize(), true));
        rejected(workflow(normalize(), write, false));
        var skipped = CompiledWorkflow.Step.grpc("normalize", "fixture:9090", false,
                normalize().method(), "false", normalize().rules(), List.of(), true, 0, "");
        rejected(workflow(skipped, write, true));
    }

    private static CompiledWorkflow workflow(CompiledWorkflow.Step first, CompiledWorkflow.Step second,
            boolean validate) {
        return new CompiledWorkflow("starter", List.of(AuthoringFixture.getDescriptor()),
                WriteRecordRequest.getDescriptor(), 30_000, List.of(first, second), null, validate);
    }

    private static CompiledWorkflow.Step normalize() {
        return step("normalize", "NormalizeText", List.of("text = input.content"));
    }

    private static CompiledWorkflow.Step write(List<String> rules) {
        return step("write", "WriteRecord", rules);
    }

    private static CompiledWorkflow.Step step(String name, String method, List<String> rules) {
        return CompiledWorkflow.Step.grpc(name, "fixture:9090", false,
                AuthoringFixture.getDescriptor().findServiceByName("AuthoringFixtureService")
                        .findMethodByName(method), null, rules, List.of(), true, 0, "");
    }

    private static void rejected(CompiledWorkflow workflow) {
        assertThatThrownBy(() -> AuthoringStarterTemplate.verify(workflow))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
