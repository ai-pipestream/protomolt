package ai.protomolt.proto.workflow;

import ai.protomolt.proto.grpc.invoke.DynamicGrpcCalls;
import ai.protomolt.proto.grpc.workflow.WorkflowValidation;
import ai.protomolt.proto.grpc.workflow.v1.Workflow;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DynamicMessage;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ServerCalls;
import java.util.List;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Strict workflow contracts apply to mapped boundaries even when a step opts out. */
class WorkflowContractValidationTest {
    private static final String VALIDATE = "ai/protomolt/proto/validate/v1/validate.proto";
    private static final String PROJECTION =
            "ai/protomolt/proto/projection/v1/projection.proto";
    private static final String PROTO = """
            syntax = "proto3";
            package strict.test;
            import "ai/protomolt/proto/validate/v1/validate.proto";
            import "ai/protomolt/proto/projection/v1/projection.proto";
            message Input { string text = 1 [(ai.protomolt.proto.validate.v1.field) = { string: { min_len: 3 } }]; }
            message Request { string text = 1 [(ai.protomolt.proto.validate.v1.field) = { string: { min_len: 6 } }]; }
            message Reply { string text = 1 [(ai.protomolt.proto.validate.v1.field) = { string: { min_len: 3 } }]; }
            message Output { string text = 1 [(ai.protomolt.proto.validate.v1.field) = { string: { min_len: 5 } }]; }
            message ItemSource { string text = 1; }
            message ItemCheck {
              option (ai.protomolt.proto.projection.v1.sources) = { source: "strict.test.ItemSource" };
              string text = 1 [(ai.protomolt.proto.projection.v1.from) = { paths: { path: "text" } },
                               (ai.protomolt.proto.validate.v1.field) = { string: { min_len: 6 } }];
            }
            message Batch { repeated ItemSource items = 1; }
            message BatchResult { repeated Reply results = 1; }
            service Echo { rpc Call(Request) returns (Reply); }
            service Worker { rpc Process(ItemCheck) returns (Reply); }
            """;
    private static FileDescriptor file;
    private static Descriptor inputType, requestType, replyType, outputType;
    private static Server server;
    private static String serverName;
    private static final AtomicInteger calls = new AtomicInteger();
    private static final AtomicInteger opens = new AtomicInteger();
    private static final AtomicInteger replyLength = new AtomicInteger(3);
    private static WorkflowRunner runner;

    @BeforeAll static void setup() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add(VALIDATE, resource(VALIDATE), "test")
                .add(PROJECTION, resource(PROJECTION), "test")
                .add("strict/test.proto", PROTO, "test").build());
        file = compiled.descriptorFor("strict/test.proto").orElseThrow();
        inputType = file.findMessageTypeByName("Input");
        requestType = file.findMessageTypeByName("Request");
        replyType = file.findMessageTypeByName("Reply");
        outputType = file.findMessageTypeByName("Output");
        ServiceDescriptor service = file.findServiceByName("Echo");
        var method = DynamicGrpcCalls.methodDescriptor(service.findMethodByName("Call"));
        ServiceDescriptor worker = file.findServiceByName("Worker");
        var process = DynamicGrpcCalls.methodDescriptor(worker.findMethodByName("Process"));
        serverName = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(serverName)
                .addService(ServerServiceDefinition.builder(
                                io.grpc.ServiceDescriptor.newBuilder(service.getFullName())
                                        .addMethod(method).build())
                        .addMethod(method, ServerCalls.asyncUnaryCall((request, observer) -> {
                            calls.incrementAndGet();
                            String value = "x".repeat(replyLength.get());
                            observer.onNext(DynamicMessage.newBuilder(replyType)
                                    .setField(replyType.findFieldByName("text"), value).build());
                            observer.onCompleted();
                        })).build())
                .addService(ServerServiceDefinition.builder(
                                io.grpc.ServiceDescriptor.newBuilder(worker.getFullName())
                                        .addMethod(process).build())
                        .addMethod(process, ServerCalls.asyncUnaryCall((request, observer) -> {
                            calls.incrementAndGet();
                            observer.onNext(DynamicMessage.newBuilder(replyType)
                                    .setField(replyType.findFieldByName("text"), "xxx").build());
                            observer.onCompleted();
                        })).build()).build().start();
        runner = new WorkflowRunner(new WorkflowRunner.ChannelFactory() {
            @Override public ManagedChannel open(CompiledWorkflow.Step step) {
                opens.incrementAndGet();
                return InProcessChannelBuilder.forName(serverName).build();
            }
        });
    }

    @AfterAll static void stop() { server.shutdownNow(); }

    private static DynamicMessage msg(Descriptor type, String value) {
        return DynamicMessage.newBuilder(type).setField(type.findFieldByName("text"), value).build();
    }

    private static String resource(String name) {
        try (InputStream in = WorkflowContractValidationTest.class.getClassLoader()
                .getResourceAsStream(name)) {
            if (in == null) throw new IllegalStateException(name + " missing from test classpath");
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    private static CompiledWorkflow workflow(boolean strict, boolean output) {
        List<FileDescriptor> files = List.of(file);
        return new CompiledWorkflow("strict", files, inputType, 10_000,
                List.of(CompiledWorkflow.Step.grpc("echo", "in-process", false,
                        CompiledWorkflow.resolveMethod(files, "strict.test.Echo/Call"), null,
                        List.of("text = input.text"), List.of(), false, 0, "")),
                output ? new CompiledWorkflow.Output(outputType,
                        List.of("text = echo.text"), List.of()) : null, strict);
    }

    private static DynamicMessage batch(String... values) {
        Descriptor batchType = file.findMessageTypeByName("Batch");
        Descriptor itemType = file.findMessageTypeByName("ItemSource");
        DynamicMessage.Builder result = DynamicMessage.newBuilder(batchType);
        for (String value : values) {
            result.addRepeatedField(batchType.findFieldByName("items"), msg(itemType, value));
        }
        return result.build();
    }

    private static DynamicMessage weakerReply(String value) throws Exception {
        var weakFile = FileDescriptor.buildFrom(DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("weak-reply.proto")
                .setPackage("strict.test")
                .setSyntax("proto3")
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("Reply")
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setName("text").setNumber(1)
                                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)))
                .build(), new FileDescriptor[0]);
        Descriptor weakType = weakFile.findMessageTypeByName("Reply");
        return msg(weakType, value);
    }

    @BeforeEach void resetCounters() { calls.set(0); opens.set(0); replyLength.set(3); }

    @Test void invalidInputIsRejectedBeforeOpeningAnyChannel() {
        assertThatThrownBy(() -> runner.run(workflow(true, false), msg(inputType, "x")))
                .isInstanceOf(WorkflowRunner.WorkflowExecutionException.class)
                .hasMessageContaining("input").hasMessageContaining("validation");
        assertThat(opens).hasValue(0);
        assertThat(calls).hasValue(0);
    }

    @Test void invalidMappedUnaryRequestIsRejectedBeforeOpeningChannel() {
        assertThatThrownBy(() -> runner.run(workflow(true, false), msg(inputType, "valid")))
                .isInstanceOf(WorkflowRunner.WorkflowExecutionException.class);
        assertThat(opens).hasValue(0);
        assertThat(calls).hasValue(0);
    }

    @Test void strictModeValidatesResponseEvenWhenStepValidationIsDisabled() throws Exception {
        replyLength.set(1);
        assertThatThrownBy(() -> runner.run(workflow(true, false), msg(inputType, "valid!")))
                .isInstanceOf(WorkflowRunner.WorkflowExecutionException.class)
                .hasMessageContaining("response").hasMessageContaining("validation");
        assertThat(opens).hasValue(1);
        assertThat(calls).hasValue(1);
    }

    @Test void strictModeValidatesFinalMappedOutput() throws Exception {
        replyLength.set(3);
        assertThatThrownBy(() -> runner.run(workflow(true, true), msg(inputType, "valid!")))
                .isInstanceOf(WorkflowRunner.WorkflowExecutionException.class)
                .hasMessageContaining("output").hasMessageContaining("validation");
    }

    @Test void validStrictWorkflowCompletes() throws Exception {
        replyLength.set(5);
        var result = runner.run(workflow(true, true), msg(inputType, "valid!"));
        assertThat(result.output().getField(outputType.findFieldByName("text")))
                .isEqualTo("xxxxx");
    }

    @Test void strictFanOutRejectsInvalidProjectedItemBeforeOpeningAnyChannel() {
        List<FileDescriptor> files = List.of(file);
        Descriptor batchType = file.findMessageTypeByName("Batch");
        Descriptor batchResult = file.findMessageTypeByName("BatchResult");
        var process = CompiledWorkflow.resolveMethod(files, "strict.test.Worker/Process");
        CompiledWorkflow.Step step = new CompiledWorkflow.Step("process", "in-process", false,
                process, null, List.of(), List.of(), false, 0, "", null,
                new CompiledWorkflow.EdgeSpec(List.of("input"), batchType,
                        List.of("items = input.items"), List.of(),
                        file.findMessageTypeByName("ItemCheck"), false),
                new CompiledWorkflow.FanOutSpec("items", 8, 2,
                        CompiledWorkflow.BranchFailurePolicy.FAIL_FAST, batchResult, "results"));
        CompiledWorkflow strict = new CompiledWorkflow("strict-fanout", files, batchType,
                10_000, List.of(step), null, true);

        assertThatThrownBy(() -> runner.run(strict, batch("x")))
                .isInstanceOf(WorkflowRunner.WorkflowExecutionException.class)
                .hasMessageContaining("validation");
        assertThat(opens).hasValue(0);
        assertThat(calls).hasValue(0);
    }

    @Test void strictCheckpointValidationUsesPinnedOutputDescriptor() throws Exception {
        assertThat(weakerReply("x").getDescriptorForType().getFullName())
                .isEqualTo(replyType.getFullName());
        assertThatThrownBy(() -> runner.runSegment(workflow(true, false),
                msg(inputType, "valid!"),
                List.of(new WorkflowRunner.Checkpoint("echo", false, weakerReply("x")))))
                .isInstanceOf(WorkflowRunner.WorkflowExecutionException.class)
                .hasMessageContaining("validation");
        assertThat(opens).hasValue(0);
        assertThat(calls).hasValue(0);
    }

    @Test void resumedSkippedFinalCheckpointPreservesPriorSuccessfulOutput() throws Exception {
        List<FileDescriptor> files = List.of(file);
        var method = CompiledWorkflow.resolveMethod(files, "strict.test.Echo/Call");
        CompiledWorkflow workflow = new CompiledWorkflow("resume", files, inputType, 10_000,
                List.of(CompiledWorkflow.Step.grpc("first", "in-process", false, method,
                                null, List.of("text = input.text"), List.of(), false, 0, ""),
                        CompiledWorkflow.Step.grpc("last", "in-process", false, method,
                                "false", List.of("text = first.text"), List.of(), false, 0, "")),
                null, true);
        var prior = List.of(new WorkflowRunner.Checkpoint("first", false,
                        msg(replyType, "xxx")),
                new WorkflowRunner.Checkpoint("last", true, null));

        var segment = runner.runSegment(workflow, msg(inputType, "valid!"), prior);

        assertThat(segment).isInstanceOf(WorkflowRunner.Segment.Completed.class);
        var result = ((WorkflowRunner.Segment.Completed) segment).result();
        assertThat(result.output().getField(replyType.findFieldByName("text"))).isEqualTo("xxx");
        assertThat(result.steps()).extracting(WorkflowRunner.StepOutcome::skipped)
                .containsExactly(false, true);
        assertThat(opens).hasValue(0);
        assertThat(calls).hasValue(0);
    }

    @Test void defaultCompatibilityConstructorRemainsNonStrict() throws Exception {
        List<FileDescriptor> files = List.of(file);
        CompiledWorkflow legacy = new CompiledWorkflow("legacy", files, inputType, 10_000,
                List.of(CompiledWorkflow.Step.grpc("echo", "in-process", false,
                        CompiledWorkflow.resolveMethod(files, "strict.test.Echo/Call"), null,
                        List.of("text = input.text"), List.of(), false, 0, "")), null);
        replyLength.set(1);
        assertThat(runner.run(legacy, msg(inputType, "valid")).output()).isNotNull();
    }

    @Test void omittedAndExplicitFalseContractFlagHaveIdenticalDurableBytesAndFingerprints() {
        List<FileDescriptor> files = List.of(file);
        CompiledWorkflow definition = new CompiledWorkflow("compat", files, inputType, 10_000,
                List.of(CompiledWorkflow.Step.grpc("echo", "in-process", false,
                        CompiledWorkflow.resolveMethod(files, "strict.test.Echo/Call"), null,
                        List.of("text = input.text"), List.of(), false, 0, "")), null, false);
        Workflow omitted = WorkflowCompiler.compile(definition).toBuilder()
                .clearValidateContract().build();
        Workflow explicitFalse = WorkflowCompiler.compile(definition);
        assertThat(explicitFalse.toByteArray()).isEqualTo(omitted.toByteArray());
        assertThat(WorkflowValidation.fingerprint(explicitFalse))
                .isEqualTo(WorkflowValidation.fingerprint(omitted));
    }
}
