package ai.pipestream.proto.chain;

import ai.pipestream.proto.grpc.invoke.DynamicGrpcCalls;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
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
 * The runner's deadline arithmetic: every call is bounded by what is left of the chain's
 * budget, a step's own {@code deadlineMs} can only shorten that, and a step whose turn comes
 * after the budget is gone is refused instead of dialled. The service records the deadline
 * gRPC actually propagated, so these assert the wire effect rather than the local maths.
 */
class ChainDeadlineTest {

    private static final String PROTO = """
            syntax = "proto3";
            package chain.deadline;
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
                .add("chain/deadline/chain.proto", PROTO, "test").build());
        file = compiled.descriptorFor("chain/deadline/chain.proto").orElseThrow();

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

    private static ChainDefinition.Step step(String name, long deadlineMs) {
        return new ChainDefinition.Step(name, "in-process", false,
                ChainDefinition.resolveMethod(List.of(file), "chain.deadline.Echo/Call"),
                null, List.of("text = input.text"), List.of(), false, deadlineMs);
    }

    private static ChainDefinition chain(long chainDeadlineMs, ChainDefinition.Step... steps) {
        return new ChainDefinition("deadlines", List.of(file),
                file.findMessageTypeByName("Ping"), chainDeadlineMs, List.of(steps), null);
    }

    private static DynamicMessage ping() {
        Descriptor type = file.findMessageTypeByName("Ping");
        return DynamicMessage.newBuilder(type)
                .setField(type.findFieldByName("text"), "hi").build();
    }

    private static ChainRunner runner() {
        return new ChainRunner(step -> InProcessChannelBuilder.forName(serverName).build());
    }

    @Test
    void aStepWithoutItsOwnDeadlineGetsWhatRemainsOfTheChainBudget() throws Exception {
        runner().run(chain(10_000, step("first", 0)), ping());

        assertThat(observed).hasSize(1);
        assertThat(observed.get(0)).isBetween(8_000L, 10_000L);
    }

    @Test
    void aStepDeadlineShorterThanTheRemainingBudgetWins() throws Exception {
        runner().run(chain(10_000, step("first", 250)), ping());

        assertThat(observed).hasSize(1);
        assertThat(observed.get(0)).isBetween(100L, 250L);
    }

    /** A step may not extend the chain's budget: the call is still clamped to what remains. */
    @Test
    void aStepDeadlineLongerThanTheChainBudgetIsClampedToWhatRemains() throws Exception {
        runner().run(chain(10_000, step("first", 600_000)), ping());

        assertThat(observed).hasSize(1);
        assertThat(observed.get(0)).isBetween(8_000L, 10_000L);
    }

    @Test
    void eachStepSeesNoMoreOfTheBudgetThanTheStepBeforeIt() throws Exception {
        runner().run(chain(10_000, step("first", 0), step("second", 0)), ping());

        assertThat(observed).hasSize(2);
        assertThat(observed.get(1)).isLessThanOrEqualTo(observed.get(0));
        assertThat(observed.get(1)).isBetween(8_000L, 10_000L);
    }

    /**
     * The budget is spent by work between calls too. Dialling the channel is charged to the
     * chain, so the second step's turn arrives with nothing left and must be refused before
     * a request goes out rather than dialled with an already-expired deadline.
     */
    @Test
    void aStepWhoseTurnComesAfterTheBudgetIsGoneIsRefused() {
        ChainRunner slowToDial = new ChainRunner(step -> {
            try {
                Thread.sleep(600);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
            return InProcessChannelBuilder.forName(serverName).build();
        });

        assertThatThrownBy(() -> slowToDial.run(
                chain(200, step("first", 0), step("second", 0)), ping()))
                .isInstanceOf(ChainRunner.ChainExecutionException.class)
                .hasMessage("chain deadline exhausted before the step ran")
                .satisfies(e -> assertThat(((ChainRunner.ChainExecutionException) e).step())
                        .isEqualTo("second"));

        // The first step did run - only the second was refused.
        assertThat(observed).hasSize(1);
    }

    @Test
    void anExhaustedChainClosesTheChannelsItOpened() {
        List<ManagedChannel> opened = new CopyOnWriteArrayList<>();
        ChainRunner slowToDial = new ChainRunner(step -> {
            try {
                Thread.sleep(600);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
            ManagedChannel channel = InProcessChannelBuilder.forName(serverName).build();
            opened.add(channel);
            return channel;
        });

        assertThatThrownBy(() -> slowToDial.run(
                chain(200, step("first", 0), step("second", 0)), ping()))
                .isInstanceOf(ChainRunner.ChainExecutionException.class);

        assertThat(opened).hasSize(1);
        assertThat(opened.get(0).isShutdown()).isTrue();
    }
}
