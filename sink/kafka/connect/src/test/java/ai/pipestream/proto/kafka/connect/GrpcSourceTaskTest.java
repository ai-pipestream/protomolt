package ai.pipestream.proto.kafka.connect;

import ai.pipestream.proto.grpc.invoke.DynamicGrpcCalls;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.DynamicMessage;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ServerCalls;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The source against a live in-process streaming service: records with CEL resume-token
 * offsets, token injection on (re)subscribe, reconnect-and-resume after transient failures
 * and graceful completions, and eager config validation.
 */
class GrpcSourceTaskTest {

    private static final String PROTO = """
            syntax = "proto3";
            package source.test;
            message Subscribe { string shard = 1; string resume_token = 2; }
            message Tick { int64 seq = 1; string cursor = 2; }
            service Feed {
              rpc Watch(Subscribe) returns (stream Tick);
              rpc One(Subscribe) returns (Tick);
            }
            """;

    /** How the Watch handler ends the stream after emitting. */
    private enum EndMode { STAY_OPEN, COMPLETE, FAIL_UNAVAILABLE, FAIL_INVALID }

    private static FileDescriptor file;
    private static Server server;
    private static String serverName;
    private static final List<DynamicMessage> subscribes = new CopyOnWriteArrayList<>();
    private static final AtomicInteger emitCount = new AtomicInteger(3);
    private static final AtomicReference<EndMode> endMode = new AtomicReference<>(EndMode.STAY_OPEN);

    private GrpcSourceTask task;

    @BeforeAll
    static void start() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("source/test/source.proto", PROTO, "test").build());
        file = compiled.descriptorFor("source/test/source.proto").orElseThrow();
        ServiceDescriptor service = file.findServiceByName("Feed");

        var watch = DynamicGrpcCalls.methodDescriptor(service.findMethodByName("Watch"));
        var one = DynamicGrpcCalls.methodDescriptor(service.findMethodByName("One"));
        io.grpc.ServiceDescriptor grpcService = io.grpc.ServiceDescriptor
                .newBuilder(service.getFullName())
                .setSchemaDescriptor((io.grpc.protobuf.ProtoFileDescriptorSupplier) () -> file)
                .addMethod(watch).addMethod(one)
                .build();

        ServerServiceDefinition definition = ServerServiceDefinition.builder(grpcService)
                .addMethod(watch, ServerCalls.asyncServerStreamingCall((request, out) -> {
                    subscribes.add(request);
                    // Resume just past the token: "c4" means the next tick is seq 5.
                    String token = (String) request.getField(
                            request.getDescriptorForType().findFieldByName("resume_token"));
                    long from = token.isEmpty() ? 0 : Long.parseLong(token.substring(1)) + 1;
                    for (int i = 0; i < emitCount.get(); i++) {
                        out.onNext(tick(from + i));
                    }
                    switch (endMode.get()) {
                        case COMPLETE -> out.onCompleted();
                        case FAIL_UNAVAILABLE -> out.onError(Status.UNAVAILABLE
                                .withDescription("shard moved").asRuntimeException());
                        case FAIL_INVALID -> out.onError(Status.INVALID_ARGUMENT
                                .withDescription("bad shard").asRuntimeException());
                        default -> { /* stays open until the client hangs up */ }
                    }
                }))
                .addMethod(one, ServerCalls.asyncUnaryCall((request, out) -> {
                    out.onNext(tick(0));
                    out.onCompleted();
                }))
                .build();

        serverName = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(serverName)
                .addService(definition)
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
        emitCount.set(3);
        endMode.set(EndMode.STAY_OPEN);
    }

    private static DynamicMessage tick(long seq) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(
                file.findMessageTypeByName("Tick"));
        builder.setField(builder.getDescriptorForType().findFieldByName("seq"), seq);
        builder.setField(builder.getDescriptorForType().findFieldByName("cursor"), "c" + seq);
        return builder.build();
    }

    private static String descriptorSetBase64() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("source/test/source.proto", PROTO, "test").build());
        return Base64.getEncoder().encodeToString(compiled.descriptorSet().toByteArray());
    }

    private static Map<String, String> config() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(GrpcSourceConfig.TARGET, "in-process");
        props.put(GrpcSourceConfig.METHOD, "source.test.Feed/Watch");
        props.put(GrpcSourceConfig.DESCRIPTOR_SET, descriptorSetBase64());
        props.put(GrpcSourceConfig.TOPIC, "ticks");
        props.put(GrpcSourceConfig.RESUME_TOKEN_CEL, "input.cursor");
        props.put(GrpcSourceConfig.RESUME_TOKEN_FIELD, "resume_token");
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
        };
    }

    private static List<Long> seqsOf(List<SourceRecord> records) throws Exception {
        List<Long> seqs = new ArrayList<>();
        for (SourceRecord record : records) {
            DynamicMessage tick = DynamicMessage.parseFrom(
                    file.findMessageTypeByName("Tick"), (byte[]) record.value());
            seqs.add((Long) tick.getField(tick.getDescriptorForType().findFieldByName("seq")));
        }
        return seqs;
    }

    @Test
    void pollDeliversRecordsWithResumeTokenOffsets() throws Exception {
        GrpcSourceTask source = startTask(config(), null);
        List<SourceRecord> records = source.poll();
        assertThat(records).hasSize(3);
        assertThat(seqsOf(records)).containsExactly(0L, 1L, 2L);
        assertThat(records).allSatisfy(record -> {
            assertThat(record.topic()).isEqualTo("ticks");
            assertThat(record.sourcePartition().get("method"))
                    .isEqualTo("source.test.Feed/Watch");
        });
        assertThat(records.get(0).sourceOffset().get(GrpcSourceTask.OFFSET_TOKEN))
                .isEqualTo("c0");
        assertThat(records.get(2).sourceOffset().get(GrpcSourceTask.OFFSET_TOKEN))
                .isEqualTo("c2");
    }

    @Test
    void storedOffsetResumesTheStream() throws Exception {
        GrpcSourceTask source = startTask(config(),
                Map.of(GrpcSourceTask.OFFSET_TOKEN, "c9"));
        List<SourceRecord> records = source.poll();
        assertThat(seqsOf(records)).containsExactly(10L, 11L, 12L);
        DynamicMessage subscribe = subscribes.get(0);
        assertThat(subscribe.getField(
                subscribe.getDescriptorForType().findFieldByName("resume_token")))
                .isEqualTo("c9");
    }

    @Test
    void transientFailureResubscribesFromTheLatestToken() throws Exception {
        endMode.set(EndMode.FAIL_UNAVAILABLE);
        emitCount.set(2);
        GrpcSourceTask source = startTask(config(), null);
        assertThat(seqsOf(source.poll())).containsExactly(0L, 1L);   // drained before the error
        assertThat(source.poll()).isEmpty();                          // failure seen, reconnect queued
        List<SourceRecord> resumed = source.poll();                   // fresh stream, resumed
        assertThat(seqsOf(resumed)).containsExactly(2L, 3L);
        assertThat(subscribes).hasSize(2);
        DynamicMessage second = subscribes.get(1);
        assertThat(second.getField(
                second.getDescriptorForType().findFieldByName("resume_token")))
                .isEqualTo("c1");
    }

    @Test
    void gracefulCompletionResubscribes() throws Exception {
        endMode.set(EndMode.COMPLETE);
        emitCount.set(1);
        GrpcSourceTask source = startTask(config(), null);
        assertThat(seqsOf(source.poll())).containsExactly(0L);
        assertThat(seqsOf(source.poll())).containsExactly(1L);
        assertThat(subscribes).hasSize(2);
    }

    @Test
    void permanentFailureFailsTheTask() throws Exception {
        endMode.set(EndMode.FAIL_INVALID);
        emitCount.set(0);
        GrpcSourceTask source = startTask(config(), null);
        assertThatThrownBy(source::poll)
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("INVALID_ARGUMENT");
    }

    @Test
    void jsonValueFormatEmitsProto3Json() throws Exception {
        Map<String, String> props = config();
        props.put(GrpcSourceConfig.VALUE_FORMAT, "json");
        GrpcSourceTask source = startTask(props, null);
        List<SourceRecord> records = source.poll();
        assertThat(records).isNotEmpty();
        assertThat(records.get(0).value()).isInstanceOf(String.class);
        assertThat((String) records.get(0).value()).contains("\"cursor\"").contains("c0");
    }

    @Test
    void keyCelProducesRecordKeys() throws Exception {
        Map<String, String> props = config();
        props.put(GrpcSourceConfig.KEY_CEL, "input.cursor");
        GrpcSourceTask source = startTask(props, null);
        List<SourceRecord> records = source.poll();
        assertThat(records.get(0).key()).isEqualTo("c0");
        assertThat(records.get(1).key()).isEqualTo("c1");
    }

    /**
     * A key expression that compiles but fails on a message must not quietly yield a null key:
     * the record would land on another partition and stop compacting against its predecessors.
     */
    @Test
    void keyCelFailureFailsTheRecord() throws Exception {
        Map<String, String> props = config();
        // Compiles against Tick, then divides by zero on the first tick, whose seq is 0.
        props.put(GrpcSourceConfig.KEY_CEL, "string(1 / input.seq)");
        GrpcSourceTask source = startTask(props, null);
        assertThatThrownBy(source::poll)
                .isInstanceOf(DataException.class)
                .hasMessageContaining(GrpcSourceConfig.KEY_CEL);
    }

    /**
     * The worker calls stop() from a thread other than the one running poll(), so the stream
     * poll() opened has to be safely published or stop() can read a stale null and leave the
     * call uncancelled.
     */
    @Test
    void theActiveStreamIsSafelyPublishedToStop() throws Exception {
        assertThat(Modifier.isVolatile(
                GrpcSourceTask.class.getDeclaredField("stream").getModifiers())).isTrue();
    }

    @Test
    void methodsThatAreNotServerStreamingAreRejected() throws Exception {
        Map<String, String> props = config();
        props.put(GrpcSourceConfig.METHOD, "source.test.Feed/One");
        assertThatThrownBy(() -> startTask(props, null))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("server-streaming");
    }

    @Test
    void badCelAndBadTokenFieldFailAtStart() throws Exception {
        Map<String, String> badCel = config();
        badCel.put(GrpcSourceConfig.RESUME_TOKEN_CEL, "input.no_such_field");
        assertThatThrownBy(() -> startTask(badCel, null))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining(GrpcSourceConfig.RESUME_TOKEN_CEL);

        Map<String, String> badField = config();
        badField.put(GrpcSourceConfig.RESUME_TOKEN_FIELD, "seq");
        assertThatThrownBy(() -> startTask(badField, null))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining(GrpcSourceConfig.RESUME_TOKEN_FIELD);
    }

    @Test
    void connectorValidatesEagerlyAndPinsOneTask() throws Exception {
        GrpcSourceConnector connector = new GrpcSourceConnector();
        connector.start(config());
        assertThat(connector.taskConfigs(4)).hasSize(1);
        assertThat(connector.taskClass()).isEqualTo(GrpcSourceTask.class);
        connector.stop();

        Map<String, String> bad = config();
        bad.put(GrpcSourceConfig.REQUEST_JSON, "{\"no_such\": 1}");
        assertThatThrownBy(() -> new GrpcSourceConnector().start(bad))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining(GrpcSourceConfig.REQUEST_JSON);
    }
}
