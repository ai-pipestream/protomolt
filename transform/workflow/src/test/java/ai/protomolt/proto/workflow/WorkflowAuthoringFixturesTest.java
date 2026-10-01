package ai.protomolt.proto.workflow;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.grpc.invoke.DynamicGrpcCalls;
import ai.protomolt.proto.grpc.workflow.FileSystemArtifactRepository;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ServerCalls;
import java.io.InputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkflowAuthoringFixturesTest {
    private static final String VALIDATE = "ai/protomolt/proto/validate/v1/validate.proto";
    private static final String PROTO = """
            syntax = "proto3";
            package workflow.test;
            import "ai/protomolt/proto/validate/v1/validate.proto";
            message Text {
              string text = 1 [(ai.protomolt.proto.validate.v1.field) = {
                required: true
                string: {min_len: 3}
              }];
            }
            service Echo { rpc Say(Text) returns (Text); }
            """;

    @TempDir Path temp;
    private FileSystemArtifactRepository artifacts;
    private Descriptor textType;
    private Server server;
    private WorkflowRunner runner;
    private WorkflowAuthoringPreflight.Result admitted;
    private final AtomicInteger calls = new AtomicInteger();
    private final AtomicReference<String> replySuffix = new AtomicReference<>("!");
    private final AtomicReference<String> replyOverride = new AtomicReference<>();

    @BeforeEach
    void setup() throws Exception {
        artifacts = new FileSystemArtifactRepository(temp);
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add(VALIDATE, resource(VALIDATE), "test")
                .add("workflow/test/echo.proto", PROTO, "test").build());
        var file = compiled.descriptorFor("workflow/test/echo.proto").orElseThrow();
        textType = file.findMessageTypeByName("Text");
        var service = file.findServiceByName("Echo");
        var method = DynamicGrpcCalls.methodDescriptor(service.findMethodByName("Say"));
        String serverName = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(serverName)
                .addService(ServerServiceDefinition.builder(io.grpc.ServiceDescriptor
                        .newBuilder(service.getFullName()).addMethod(method).build())
                        .addMethod(method, ServerCalls.asyncUnaryCall((request, response) -> {
                            calls.incrementAndGet();
                            String input = (String) request.getField(textType.findFieldByName("text"));
                            String forced = replyOverride.get();
                            response.onNext(message(forced == null ? input + replySuffix.get() : forced));
                            response.onCompleted();
                        })).build()).build().start();
        runner = new WorkflowRunner(step -> InProcessChannelBuilder.forName(serverName).build());

        byte[] descriptors = compiled.descriptorSet().toByteArray();
        var source = new ObjectMapper().createObjectNode();
        source.put("name", "echo-text");
        source.putObject("schema").put("descriptorSetBase64",
                Base64.getEncoder().encodeToString(descriptors));
        source.put("inputType", "workflow.test.Text");
        var step = source.putArray("steps").addObject();
        step.put("name", "echo");
        step.put("target", "fixture:9090");
        step.put("method", "workflow.test.Echo/Say");
        step.put("validate", true);
        step.putArray("rules").add("text = input.text");
        var context = ActionContext.create();
        var durable = WorkflowCompiler.compile(WorkflowJson.parse(source, context));
        var workflowRef = artifacts.save(durable.toByteArray(), "application/x-protobuf", false);
        var sourceRef = artifacts.save(source.toString().getBytes(StandardCharsets.UTF_8),
                "application/json", false);
        var descriptorRef = artifacts.save(descriptors, "application/x-protobuf", false);
        var policy = new WorkflowAuthoringPreflight.Policy(descriptorRef,
                List.of(new WorkflowAuthoringPreflight.PermittedCall(
                        "fixture:9090", "workflow.test.Echo/Say", false)));
        admitted = WorkflowAuthoringPreflight.verify(durable, workflowRef, sourceRef,
                descriptorRef, policy, context, artifacts);
    }

    @AfterEach
    void stop() {
        if (server != null) server.shutdownNow();
    }

    @Test
    void executesTruthfulCallerFixtureAndStoresObservedOutput() throws Exception {
        var fixture = fixture("first", "hello", "hello!");
        var observed = WorkflowAuthoringFixtures.execute(admitted, List.of(fixture),
                List.of(fixture), artifacts, runner);
        assertThat(calls).hasValue(1);
        assertThat(observed).hasSize(1);
        assertThat(observed.getFirst().name()).isEqualTo("first");
        assertThat(observed.getFirst().input()).isEqualTo(fixture.input());
        assertThat(observed.getFirst().output()).isEqualTo(fixture.expectedOutput());
        assertThat(artifacts.find(observed.getFirst().output().getSha256())).isPresent();
    }

    @Test
    void comparesParsedMessagesWhilePreservingExactPolicyArtifactReference() throws Exception {
        byte[] first = message("replaced").toByteArray();
        byte[] last = message("hello").toByteArray();
        byte[] duplicateField = java.util.Arrays.copyOf(first, first.length + last.length);
        System.arraycopy(last, 0, duplicateField, first.length, last.length);
        var exactInput = artifacts.save(duplicateField, "application/x-protobuf", false);
        var fixture = new WorkflowAuthoringFixtures.Fixture("alternate-encoding", exactInput, ref("hello!"));
        var observed = WorkflowAuthoringFixtures.execute(admitted, List.of(fixture), List.of(fixture),
                artifacts, runner);
        assertThat(calls).hasValue(1);
        assertThat(observed.getFirst().input()).isEqualTo(exactInput);
        assertThat(observed.getFirst().output()).isEqualTo(ref("hello!"));
    }

    @Test
    void rejectsWorkerChangedExpectedReferenceBeforeCalls() throws Exception {
        var trusted = fixture("first", "hello", "hello!");
        var changed = new WorkflowAuthoringFixtures.Fixture("first", trusted.input(),
                ref("different!"));
        assertThatThrownBy(() -> WorkflowAuthoringFixtures.execute(admitted,
                List.of(changed), List.of(trusted), artifacts, runner))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("caller policy");
        assertThat(calls).hasValue(0);
    }

    @Test
    void rejectsWrongServiceResultAfterRealCall() throws Exception {
        var trusted = fixture("first", "hello", "hello!");
        replySuffix.set("?");
        assertThatThrownBy(() -> WorkflowAuthoringFixtures.execute(admitted,
                List.of(trusted), List.of(trusted), artifacts, runner))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("output differs");
        assertThat(calls).hasValue(1);
    }

    @Test
    void validatesEveryFixtureBeforeFirstCall() throws Exception {
        var first = fixture("first", "hello", "hello!");
        var invalidInput = new WorkflowAuthoringFixtures.Fixture("bad-input",
                artifacts.save(new byte[] {(byte) 0xff}, "application/x-protobuf", false),
                first.expectedOutput());
        assertRejectsWithoutCalls(List.of(invalidInput));

        var invalidExpected = new WorkflowAuthoringFixtures.Fixture("bad-output",
                first.input(), artifacts.save(new byte[] {(byte) 0xff},
                        "application/x-protobuf", false));
        assertRejectsWithoutCalls(List.of(invalidExpected));
        assertRejectsWithoutCalls(List.of(first, invalidExpected));

        var contractInvalidInput = new WorkflowAuthoringFixtures.Fixture("short-input",
                ref("x"), first.expectedOutput());
        assertThatThrownBy(() -> WorkflowAuthoringFixtures.execute(admitted,
                List.of(contractInvalidInput), List.of(contractInvalidInput), artifacts, runner))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("violates contract");
        assertThat(calls).hasValue(0);

        var contractInvalidExpected = new WorkflowAuthoringFixtures.Fixture("short-output",
                first.input(), ref("x"));
        assertThatThrownBy(() -> WorkflowAuthoringFixtures.execute(admitted,
                List.of(first, contractInvalidExpected), List.of(first, contractInvalidExpected),
                artifacts, runner))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("violates contract");
        assertThat(calls).hasValue(0);
    }

    @Test
    void rejectsContractInvalidSuccessfulServiceResponse() throws Exception {
        var trusted = fixture("first", "hello", "hello!");
        replyOverride.set("x");
        assertThatThrownBy(() -> WorkflowAuthoringFixtures.execute(admitted,
                List.of(trusted), List.of(trusted), artifacts, runner))
                .isInstanceOf(WorkflowRunner.WorkflowExecutionException.class)
                .hasMessageContaining("validat");
        assertThat(calls).hasValue(1);
    }

    @Test
    void rejectsUnknownFieldsInFixturesBeforeCalls() throws Exception {
        byte[] valid = message("hello").toByteArray();
        byte[] unknown = java.util.Arrays.copyOf(valid, valid.length + 2);
        unknown[valid.length] = 0x10;
        unknown[valid.length + 1] = 0x01;
        var bad = new WorkflowAuthoringFixtures.Fixture("unknown",
                artifacts.save(unknown, "application/x-protobuf", false), ref("hello!"));
        assertThatThrownBy(() -> WorkflowAuthoringFixtures.execute(admitted,
                List.of(bad), List.of(bad), artifacts, runner))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown fields");
        assertThat(calls).hasValue(0);
    }

    private void assertRejectsWithoutCalls(List<WorkflowAuthoringFixtures.Fixture> fixtures) {
        assertThatThrownBy(() -> WorkflowAuthoringFixtures.execute(admitted,
                fixtures, fixtures, artifacts, runner)).isInstanceOf(Exception.class);
        assertThat(calls).hasValue(0);
    }

    private WorkflowAuthoringFixtures.Fixture fixture(String name, String input,
            String expected) throws Exception {
        return new WorkflowAuthoringFixtures.Fixture(name, ref(input), ref(expected));
    }

    private ArtifactReference ref(String text) throws Exception {
        return artifacts.save(message(text).toByteArray(), "application/x-protobuf", false);
    }

    private DynamicMessage message(String value) {
        return DynamicMessage.newBuilder(textType)
                .setField(textType.findFieldByName("text"), value).build();
    }

    private static String resource(String name) throws IOException {
        try (InputStream in = WorkflowAuthoringFixturesTest.class.getClassLoader()
                .getResourceAsStream(name)) {
            if (in == null) throw new IllegalStateException(name + " not on the test classpath");
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
