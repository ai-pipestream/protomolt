package ai.pipestream.proto.chain;

import ai.pipestream.proto.grpc.invoke.DynamicGrpcCalls;
import ai.pipestream.proto.shapes.ShapeSynthesizer;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.DynamicMessage;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ServerCalls;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Two live gRPC server streams joined by key and by zip: shuffled arrival matches, bounded
 * buffers drop oldest, and the joined shape is built through the standard scoped rules.
 */
class StreamJoinerTest {

    private static final String PROTO = """
            syntax = "proto3";
            package sj.test;
            message Subscribe { int64 count = 1; bool shuffle = 2; }
            message Click { string user = 1; string page = 2; }
            message Profile { string user = 1; string plan = 2; }
            message Enriched { string user = 1; string page = 2; string plan = 3; }
            message Order { int64 id = 1; repeated string tags = 2; }
            service Clicks {
              rpc Watch(Subscribe) returns (stream Click);
              rpc Get(Subscribe) returns (Click);
            }
            service Profiles { rpc Watch(Subscribe) returns (stream Profile); }
            service Orders { rpc Watch(Subscribe) returns (stream Order); }
            """;

    private static FileDescriptor file;
    private static Server server;
    private static String serverName;
    private static ManagedChannel channel;

    @BeforeAll
    static void start() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("sj/test/sj.proto", PROTO, "test").build());
        file = compiled.descriptorFor("sj/test/sj.proto").orElseThrow();

        ServiceDescriptor clicks = file.findServiceByName("Clicks");
        ServiceDescriptor profiles = file.findServiceByName("Profiles");
        var clicksWatch = DynamicGrpcCalls.methodDescriptor(clicks.findMethodByName("Watch"));
        var profilesWatch = DynamicGrpcCalls.methodDescriptor(profiles.findMethodByName("Watch"));

        Descriptor click = file.findMessageTypeByName("Click");
        Descriptor profile = file.findMessageTypeByName("Profile");
        serverName = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(serverName)
                .addService(ServerServiceDefinition
                        .builder(io.grpc.ServiceDescriptor.newBuilder(clicks.getFullName())
                                .addMethod(clicksWatch).build())
                        .addMethod(clicksWatch, ServerCalls.asyncServerStreamingCall((request, out) -> {
                            long count = (long) request.getField(
                                    request.getDescriptorForType().findFieldByName("count"));
                            for (long i = 0; i < count; i++) {
                                out.onNext(DynamicMessage.newBuilder(click)
                                        .setField(click.findFieldByName("user"), "u" + i)
                                        .setField(click.findFieldByName("page"), "/p" + i)
                                        .build());
                            }
                            out.onCompleted();
                        }))
                        .build())
                .addService(ServerServiceDefinition
                        .builder(io.grpc.ServiceDescriptor.newBuilder(profiles.getFullName())
                                .addMethod(profilesWatch).build())
                        .addMethod(profilesWatch, ServerCalls.asyncServerStreamingCall((request, out) -> {
                            long count = (long) request.getField(
                                    request.getDescriptorForType().findFieldByName("count"));
                            boolean shuffle = (boolean) request.getField(
                                    request.getDescriptorForType().findFieldByName("shuffle"));
                            // Shuffled arrival: profiles come back-to-front.
                            for (long i = shuffle ? count - 1 : 0;
                                    shuffle ? i >= 0 : i < count;
                                    i += shuffle ? -1 : 1) {
                                out.onNext(DynamicMessage.newBuilder(profile)
                                        .setField(profile.findFieldByName("user"), "u" + i)
                                        .setField(profile.findFieldByName("plan"), "plan" + i)
                                        .build());
                            }
                            out.onCompleted();
                        }))
                        .build())
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(serverName).build();
    }

    @AfterAll
    static void stop() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    private StreamJoiner joiner;

    @AfterEach
    void closeJoiner() {
        if (joiner != null) {
            joiner.close();
            joiner = null;
        }
    }

    private static DynamicMessage subscribe(long count, boolean shuffle) {
        Descriptor type = file.findMessageTypeByName("Subscribe");
        return DynamicMessage.newBuilder(type)
                .setField(type.findFieldByName("count"), count)
                .setField(type.findFieldByName("shuffle"), shuffle)
                .build();
    }

    private StreamJoiner open(StreamJoiner.Mode mode, long count, boolean shuffle, int limit) {
        List<FileDescriptor> files = List.of(file);
        joiner = new StreamJoiner(mode,
                new StreamJoiner.Side("click", channel,
                        ChainDefinition.resolveMethod(files, "sj.test.Clicks/Watch"),
                        subscribe(count, false), "user"),
                new StreamJoiner.Side("profile", channel,
                        ChainDefinition.resolveMethod(files, "sj.test.Profiles/Watch"),
                        subscribe(count, shuffle), "user"),
                limit,
                file.findMessageTypeByName("Enriched"),
                List.of("user = click.user", "page = click.page", "plan = profile.plan"),
                List.of());
        return joiner;
    }

    private static String field(DynamicMessage message, String name) {
        return (String) message.getField(
                message.getDescriptorForType().findFieldByName(name));
    }

    @Test
    void keyedJoinMatchesShuffledArrival() throws Exception {
        StreamJoiner join = open(StreamJoiner.Mode.KEYED, 5, true, 100);
        List<DynamicMessage> out = join.take(10, Duration.ofSeconds(5));
        assertThat(out).hasSize(5);
        for (DynamicMessage message : out) {
            String user = field(message, "user");
            String index = user.substring(1);
            assertThat(field(message, "page")).isEqualTo("/p" + index);
            assertThat(field(message, "plan")).isEqualTo("plan" + index);
        }
        assertThat(join.isClosed()).isTrue();
    }

    @Test
    void zipJoinPairsByArrivalOrder() throws Exception {
        StreamJoiner join = open(StreamJoiner.Mode.ZIP, 3, false, 100);
        List<DynamicMessage> out = join.take(10, Duration.ofSeconds(5));
        assertThat(out).hasSize(3);
        assertThat(field(out.get(0), "user")).isEqualTo("u0");
        assertThat(field(out.get(0), "plan")).isEqualTo("plan0");
        assertThat(field(out.get(2), "plan")).isEqualTo("plan2");
    }

    @Test
    void smallTakesNeverStrandMatchedPairs() throws Exception {
        // A caller draining two at a time must still see every match: joins completed
        // while the output was full wait in the ready queue instead of stranding both
        // halves in the input buffers.
        StreamJoiner join = open(StreamJoiner.Mode.KEYED, 6, true, 100);
        List<DynamicMessage> all = new java.util.ArrayList<>();
        long deadline = System.currentTimeMillis() + 5_000;
        while (all.size() < 6 && System.currentTimeMillis() < deadline) {
            all.addAll(join.take(2, Duration.ofMillis(250)));
        }
        assertThat(all).hasSize(6);
        assertThat(all).extracting(m -> field(m, "user")).doesNotHaveDuplicates();
    }

    @Test
    void sidesAreValidatedBeforeAnyStreamOpens() {
        List<FileDescriptor> files = List.of(file);
        Descriptor enriched = file.findMessageTypeByName("Enriched");
        StreamJoiner.Side clicksByUser = new StreamJoiner.Side("click", channel,
                ChainDefinition.resolveMethod(files, "sj.test.Clicks/Watch"),
                subscribe(1, false), "user");

        // Key types must agree across sides (string vs int64).
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> new StreamJoiner(
                StreamJoiner.Mode.KEYED, clicksByUser,
                new StreamJoiner.Side("order", channel,
                        ChainDefinition.resolveMethod(files, "sj.test.Orders/Watch"),
                        subscribe(1, false), "id"),
                10, enriched, List.of(), List.of()))
                .hasMessageContaining("key types differ");

        // The key path must exist and be singular.
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> new StreamJoiner(
                StreamJoiner.Mode.KEYED, clicksByUser,
                new StreamJoiner.Side("profile", channel,
                        ChainDefinition.resolveMethod(files, "sj.test.Profiles/Watch"),
                        subscribe(1, false), "nope"),
                10, enriched, List.of(), List.of()))
                .hasMessageContaining("no field 'nope'");
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> new StreamJoiner(
                StreamJoiner.Mode.KEYED, clicksByUser,
                new StreamJoiner.Side("order", channel,
                        ChainDefinition.resolveMethod(files, "sj.test.Orders/Watch"),
                        subscribe(1, false), "tags"),
                10, enriched, List.of(), List.of()))
                .hasMessageContaining("is repeated");

        // Both methods must be server-streaming.
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> new StreamJoiner(
                StreamJoiner.Mode.ZIP,
                new StreamJoiner.Side("click", channel,
                        ChainDefinition.resolveMethod(files, "sj.test.Clicks/Get"),
                        subscribe(1, false), null),
                clicksByUser, 10, enriched, List.of(), List.of()))
                .hasMessageContaining("not server-streaming");
    }

    @Test
    void boundedBuffersDropOldestUnmatched() throws Exception {
        // 50 keys arrive on each side, profiles reversed, but only 10 may wait:
        // early clicks are evicted before their (late) profiles arrive, so matches
        // happen only where the buffered windows overlap - and memory stayed bounded.
        StreamJoiner join = open(StreamJoiner.Mode.KEYED, 50, true, 10);
        List<DynamicMessage> out = join.take(100, Duration.ofSeconds(5));
        assertThat(out.size()).isLessThan(50).isGreaterThan(0);
        assertThat(join.isClosed()).isTrue();
    }
}
