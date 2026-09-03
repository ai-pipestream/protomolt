package ai.protomolt.proto.kafka.connect;

import ai.protomolt.proto.grpc.invoke.DynamicGrpcCalls;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.DynamicMessage;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ServerCalls;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Resume-token mechanics against a live in-process stream: token values that are not strings
 * are stringified (numbers) or base64'd (bytes) for the Connect offset, a stored token is
 * injected into a nested request field on (re)subscribe, and the API token rides as call
 * metadata. {@link GrpcSourceTaskTest} covers the poll/reconnect lifecycle; this class covers
 * what the tokens look like while it happens.
 */
class GrpcSourceTaskTokenTest {

    private static final String PROTO = """
            syntax = "proto3";
            package tok.test;
            message Envelope { string cursor = 1; }
            message Subscribe { string resume_token = 1; Envelope position = 2; }
            message Tick { int64 seq = 1; string cursor = 2; bytes blob = 3; }
            service Feed { rpc Watch(Subscribe) returns (stream Tick); }
            """;

    private static final Metadata.Key<String> API_TOKEN =
            Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER);
    private static final byte[] BLOB = {1, 2, 3};

    private static FileDescriptor file;
    private static Server server;
    private static String serverName;
    private static final List<DynamicMessage> subscribes = new CopyOnWriteArrayList<>();
    private static final List<String> apiTokens = new CopyOnWriteArrayList<>();

    private GrpcSourceTask task;

    @BeforeAll
    static void start() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("tok/test/tok.proto", PROTO, "test").build());
        file = compiled.descriptorFor("tok/test/tok.proto").orElseThrow();
        ServiceDescriptor service = file.findServiceByName("Feed");

        var watch = DynamicGrpcCalls.methodDescriptor(service.findMethodByName("Watch"));
        io.grpc.ServiceDescriptor grpcService = io.grpc.ServiceDescriptor
                .newBuilder(service.getFullName())
                .setSchemaDescriptor((io.grpc.protobuf.ProtoFileDescriptorSupplier) () -> file)
                .addMethod(watch)
                .build();

        ServerServiceDefinition definition = ServerServiceDefinition.builder(grpcService)
                .addMethod(watch, ServerCalls.asyncServerStreamingCall((request, out) -> {
                    subscribes.add(request);
                    // The top-level token drives where the stream resumes: "3" means the next
                    // tick is seq 4. The nested position is asserted, not parsed.
                    String token = (String) request.getField(
                            request.getDescriptorForType().findFieldByName("resume_token"));
                    long from = token.isEmpty() ? 0 : Long.parseLong(token) + 1;
                    out.onNext(tick(from));
                    out.onCompleted();
                }))
                .build();

        serverName = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(serverName)
                .addService(ServerInterceptors.intercept(definition, new ServerInterceptor() {
                    @Override
                    public <Q, S> ServerCall.Listener<Q> interceptCall(
                            ServerCall<Q, S> call, Metadata headers, ServerCallHandler<Q, S> next) {
                        apiTokens.add(headers.get(API_TOKEN));
                        return next.startCall(call, headers);
                    }
                }))
                .build()
                .start();
    }

    @AfterAll
    static void stop() {
        server.shutdownNow();
    }

    @AfterEach
    void stopTask() {
        if (task != null) {
            task.stop();
            task = null;
        }
        subscribes.clear();
        apiTokens.clear();
    }

    private static DynamicMessage tick(long seq) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(
                file.findMessageTypeByName("Tick"));
        builder.setField(builder.getDescriptorForType().findFieldByName("seq"), seq);
        builder.setField(builder.getDescriptorForType().findFieldByName("cursor"), "c" + seq);
        builder.setField(builder.getDescriptorForType().findFieldByName("blob"),
                ByteString.copyFrom(BLOB));
        return builder.build();
    }

    private static String descriptorSetBase64() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("tok/test/tok.proto", PROTO, "test").build());
        return Base64.getEncoder().encodeToString(compiled.descriptorSet().toByteArray());
    }

    private static Map<String, String> config() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(GrpcSourceConfig.TARGET, "in-process");
        props.put(GrpcSourceConfig.METHOD, "tok.test.Feed/Watch");
        props.put(GrpcSourceConfig.DESCRIPTOR_SET, descriptorSetBase64());
        props.put(GrpcSourceConfig.TOPIC, "ticks");
        props.put(GrpcSourceConfig.RESUME_TOKEN_CEL, "input.cursor");
        props.put(GrpcSourceConfig.POLL_TIMEOUT_MS, "250");
        props.put(GrpcSourceConfig.RECONNECT_BACKOFF_MS, "0");
        return props;
    }

    private GrpcSourceTask startTask(Map<String, String> props, Map<String, Object> offset) {
        GrpcSourceTask started = new GrpcSourceTask();
        started.channelFactory = config -> InProcessChannelBuilder.forName(serverName).build();
        started.initialize(contextWithOffset(offset));
        started.start(props);
        task = started;
        return started;
    }

    private static SourceTaskContext contextWithOffset(Map<String, Object> offset) {
        OffsetStorageReader reader = new OffsetStorageReader() {
            @Override
            public <T> Map<String, Object> offset(Map<String, T> partition) {
                return offset;
            }

            @Override
            public <T> Map<Map<String, T>, Map<String, Object>> offsets(
                    Collection<Map<String, T>> partitions) {
                Map<Map<String, T>, Map<String, Object>> all = new HashMap<>();
                for (Map<String, T> partition : partitions) {
                    all.put(partition, offset);
                }
                return all;
            }
        };
        return new SourceTaskContext() {
            @Override
            public Map<String, String> configs() {
                return Map.of();
            }

            @Override
            public OffsetStorageReader offsetStorageReader() {
                return reader;
            }

            @Override
            public org.apache.kafka.common.metrics.PluginMetrics pluginMetrics() {
                return null;
            }
        };
    }

    private static Object field(DynamicMessage message, String name) {
        return message.getField(message.getDescriptorForType().findFieldByName(name));
    }

    /**
     * A dotted token field descends through a nested message: the stored token lands at
     * {@code position.cursor}, not at some flattened top-level name.
     */
    @Test
    void aStoredTokenIsInjectedIntoANestedRequestField() throws Exception {
        Map<String, String> props = config();
        props.put(GrpcSourceConfig.RESUME_TOKEN_FIELD, "position.cursor");
        GrpcSourceTask source = startTask(props, Map.of(GrpcSourceTask.OFFSET_TOKEN, "c9"));

        List<SourceRecord> records = source.poll();

        assertThat(records).hasSize(1);
        assertThat(subscribes).hasSize(1);
        DynamicMessage subscribe = subscribes.get(0);
        DynamicMessage position = (DynamicMessage) field(subscribe, "position");
        assertThat(field(position, "cursor")).isEqualTo("c9");
        // The top-level token field is untouched by the nested injection.
        assertThat(field(subscribe, "resume_token")).isEqualTo("");
    }

    /**
     * A token expression over a numeric field yields its decimal string; on resubscribe that
     * string is what the request's string token field carries.
     */
    @Test
    void numericTokensAreStringifiedAndResumeFromTheLatest() throws Exception {
        Map<String, String> props = config();
        props.put(GrpcSourceConfig.RESUME_TOKEN_CEL, "input.seq");
        props.put(GrpcSourceConfig.RESUME_TOKEN_FIELD, "resume_token");
        GrpcSourceTask source = startTask(props, null);

        List<SourceRecord> first = source.poll();
        assertThat(first).hasSize(1);
        assertThat(first.get(0).sourceOffset().get(GrpcSourceTask.OFFSET_TOKEN))
                .isEqualTo("0");

        // The stream completed after one tick, so the next poll resubscribes from "0".
        List<SourceRecord> second = source.poll();
        assertThat(second).hasSize(1);
        assertThat(second.get(0).sourceOffset().get(GrpcSourceTask.OFFSET_TOKEN))
                .isEqualTo("1");
        assertThat(subscribes).hasSize(2);
        assertThat(field(subscribes.get(1), "resume_token")).isEqualTo("0");
    }

    /**
     * A bytes-valued token cannot ride in a Connect offset raw; it is base64-encoded, which is
     * also what the offset REST APIs would do to it.
     */
    @Test
    void bytesTokensAreBase64Encoded() throws Exception {
        Map<String, String> props = config();
        props.put(GrpcSourceConfig.RESUME_TOKEN_CEL, "input.blob");
        GrpcSourceTask source = startTask(props, null);

        List<SourceRecord> records = source.poll();

        assertThat(records).hasSize(1);
        assertThat(records.get(0).sourceOffset().get(GrpcSourceTask.OFFSET_TOKEN))
                .isEqualTo(Base64.getEncoder().encodeToString(BLOB));
    }

    @Test
    void theApiTokenRidesAsCallMetadata() throws Exception {
        Map<String, String> props = config();
        props.put(GrpcSourceConfig.API_TOKEN, "tok-123");
        GrpcSourceTask source = startTask(props, null);
        source.poll();
        assertThat(apiTokens).containsExactly("tok-123");
    }

    @Test
    void noApiTokenMeansNoTokenHeader() throws Exception {
        GrpcSourceTask source = startTask(config(), null);
        source.poll();
        assertThat(apiTokens).containsExactly((String) null);
    }
}
