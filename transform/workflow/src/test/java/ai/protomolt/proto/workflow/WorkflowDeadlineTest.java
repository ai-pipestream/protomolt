package ai.protomolt.proto.workflow;

import ai.protomolt.proto.grpc.invoke.DynamicGrpcCalls;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.DynamicMessage;
import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ServerCalls;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The runner's deadline arithmetic: every call is bounded by what is left of the workflow's
 * budget, a step's own {@code deadlineMs} can only shorten that, and a step whose turn comes
 * after the budget is gone is refused instead of dialled. The service records the deadline
 * gRPC actually propagated, so these assert the wire effect rather than the local maths.
 */
class WorkflowDeadlineTest {

    private static final String PROTO = """
            syntax = "proto3";
            package workflow.deadline;
            message Ping { string text = 1; }
            message Pong { string text = 1; }
            service Echo { rpc Call(Ping) returns (Pong); }
            """;

    private static FileDescriptor file;
    private static Server server;
    private static String serverName;

    /** Milliseconds left on the deadline the server saw, one entry per call, in order. */
    private static final List<Long> observed = new CopyOnWriteArrayList<>();

    @BeforeAll
    static void start() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("workflow/deadline/workflow.proto", PROTO, "test").build());
        file = compiled.descriptorFor("workflow/deadline/workflow.proto").orElseThrow();

        ServiceDescriptor echo = file.findServiceByName("Echo");
        var call = DynamicGrpcCalls.methodDescriptor(echo.findMethodByName("Call"));
        Descriptor pong = file.findMessageTypeByName("Pong");

        serverName = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(serverName)
                .addService(ServerServiceDefinition
                        .builder(io.grpc.ServiceDescriptor.newBuilder(echo.getFullName())
                                .addMethod(call).build())
                        .addMethod(call, ServerCalls.asyncUnaryCall((request, out) -> {
                            Deadline deadline = Context.current().getDeadline();
                            observed.add(deadline == null
                                    ? -1L
                                    : deadline.timeRemaining(TimeUnit.MILLISECONDS));
                            out.onNext(DynamicMessage.newBuilder(pong)
                                    .setField(pong.findFieldByName("text"), "pong").build());
                            out.onCompleted();
                        }))
                        .build())
                .build()
                .start();
    }

    @AfterAll
    static void stop() {
        server.shutdownNow();
    }

    @BeforeEach
    void clear() {
        observed.clear();
    }

    private static CompiledWorkflow.Step step(String name, long deadlineMs) {
        return CompiledWorkflow.Step.grpc(name, "in-process", false,
                CompiledWorkflow.resolveMethod(List.of(file), "workflow.deadline.Echo/Call"),
                null, List.of("text = input.text"), List.of(), false, deadlineMs, "");
    }

    private static CompiledWorkflow workflow(long workflowDeadlineMs, CompiledWorkflow.Step... steps) {
        return new CompiledWorkflow("deadlines", List.of(file),
                file.findMessageTypeByName("Ping"), workflowDeadlineMs, List.of(steps), null);
    }

    private static DynamicMessage ping() {
        Descriptor type = file.findMessageTypeByName("Ping");
        return DynamicMessage.newBuilder(type)
                .setField(type.findFieldByName("text"), "hi").build();
    }

    private static WorkflowRunner runner() {
        return new WorkflowRunner(step -> InProcessChannelBuilder.forName(serverName).build());
    }

    /** A manually advanced budget clock: the deadline maths race no scheduler. */
    private static final class FakeNanoClock implements java.util.function.LongSupplier {
        private long nanos;

        @Override
        public long getAsLong() {
            return nanos;
        }

        void advanceMillis(long millis) {
            nanos += TimeUnit.MILLISECONDS.toNanos(millis);
        }
    }

    @Test
    void aStepWithoutItsOwnDeadlineGetsWhatRemainsOfTheWorkflowBudget() throws Exception {
        runner().run(workflow(10_000, step("first", 0)), ping());

        assertThat(observed).hasSize(1);
        assertThat(observed.get(0)).isBetween(8_000L, 10_000L);
    }

    @Test
    void aStepDeadlineShorterThanTheRemainingBudgetWins() throws Exception {
        runner().run(workflow(10_000, step("first", 250)), ping());

        assertThat(observed).hasSize(1);
        assertThat(observed.get(0)).isBetween(100L, 250L);
    }

    /** A step may not extend the workflow's budget: the call is still clamped to what remains. */
    @Test
    void aStepDeadlineLongerThanTheWorkflowBudgetIsClampedToWhatRemains() throws Exception {
        runner().run(workflow(10_000, step("first", 600_000)), ping());

        assertThat(observed).hasSize(1);
        assertThat(observed.get(0)).isBetween(8_000L, 10_000L);
    }

    @Test
    void eachStepSeesNoMoreOfTheBudgetThanTheStepBeforeIt() throws Exception {
        runner().run(workflow(10_000, step("first", 0), step("second", 0)), ping());

        assertThat(observed).hasSize(2);
        assertThat(observed.get(1)).isLessThanOrEqualTo(observed.get(0));
        assertThat(observed.get(1)).isBetween(8_000L, 10_000L);
    }

    /**
     * The budget is spent by work between calls too. Dialling the channel is charged to the
     * workflow, so the second step's turn arrives with nothing left and must be refused before
     * a request goes out rather than dialled with an already-expired deadline. The budget
     * clock is injected and advanced by the dial itself, so the arithmetic races no
     * scheduler: this test once flaked under parallel-build load when a real 600ms sleep
     * ate a real 200ms budget before the FIRST step's check.
     */
    @Test
    void aStepWhoseTurnComesAfterTheBudgetIsGoneIsRefused() {
        FakeNanoClock clock = new FakeNanoClock();
        WorkflowRunner slowToDial = new WorkflowRunner(step -> {
            clock.advanceMillis(600);
            return InProcessChannelBuilder.forName(serverName).build();
        }, null, clock);

        assertThatThrownBy(() -> slowToDial.run(
                workflow(200, step("first", 0), step("second", 0)), ping()))
                .isInstanceOf(WorkflowRunner.WorkflowExecutionException.class)
                .hasMessage("workflow deadline exhausted before the step ran")
                .satisfies(e -> assertThat(((WorkflowRunner.WorkflowExecutionException) e).step())
                        .isEqualTo("second"));

        // The first step did run - only the second was refused.
        assertThat(observed).hasSize(1);
    }

    @Test
    void anExhaustedWorkflowClosesTheChannelsItOpened() {
        FakeNanoClock clock = new FakeNanoClock();
        List<ManagedChannel> opened = new CopyOnWriteArrayList<>();
        WorkflowRunner slowToDial = new WorkflowRunner(step -> {
            clock.advanceMillis(600);
            ManagedChannel channel = InProcessChannelBuilder.forName(serverName).build();
            opened.add(channel);
            return channel;
        }, null, clock);

        assertThatThrownBy(() -> slowToDial.run(
                workflow(200, step("first", 0), step("second", 0)), ping()))
                .isInstanceOf(WorkflowRunner.WorkflowExecutionException.class);

        assertThat(opened).hasSize(1);
        assertThat(opened.get(0).isShutdown()).isTrue();
    }
}
