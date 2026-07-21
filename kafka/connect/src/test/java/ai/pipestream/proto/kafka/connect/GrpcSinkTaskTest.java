package ai.pipestream.proto.kafka.connect;

import ai.pipestream.proto.grpc.invoke.DynamicGrpcCalls;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.DynamicMessage;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.Deadline;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The sink against a live in-process service: unary per record, client-streaming per batch,
 * every value format, and the status-to-retry mapping a Connect worker relies on.
 */
class GrpcSinkTaskTest {

    private static final String PROTO = """
            syntax = "proto3";
            package sink.test;
            message Event { int64 seq = 1; string note = 2; }
            message Ack { int64 count = 1; }
            service Collector {
              rpc Record(Event) returns (Ack);
              rpc RecordBatch(stream Event) returns (Ack);
              rpc Feed(Event) returns (stream Ack);
            }
            """;

    private static FileDescriptor file;
    private static Server server;
    private static String serverName;
    private static final List<Long> unarySeen = new CopyOnWriteArrayList<>();
    private static final AtomicLong batchTotal = new AtomicLong();
    private static final AtomicBoolean unavailable = new AtomicBoolean();
    /** How long the unary handler dawdles, so a batch can outlive a deadline built once. */
    private static final AtomicLong unaryDelayMs = new AtomicLong();
    /** The deadline each outgoing call actually carried, captured client-side. */
    private static final List<Deadline> callDeadlines = new CopyOnWriteArrayList<>();

    private GrpcSinkTask task;

    @BeforeAll
    static void start() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("sink/test/sink.proto", PROTO, "test").build());
        file = compiled.descriptorFor("sink/test/sink.proto").orElseThrow();
        ServiceDescriptor service = file.findServiceByName("Collector");

        var record = DynamicGrpcCalls.methodDescriptor(service.findMethodByName("Record"));
        var batch = DynamicGrpcCalls.methodDescriptor(service.findMethodByName("RecordBatch"));
        var feed = DynamicGrpcCalls.methodDescriptor(service.findMethodByName("Feed"));
        io.grpc.ServiceDescriptor grpcService = io.grpc.ServiceDescriptor
                .newBuilder(service.getFullName())
                .setSchemaDescriptor((io.grpc.protobuf.ProtoFileDescriptorSupplier) () -> file)
                .addMethod(record).addMethod(batch).addMethod(feed)
                .build();

        ServerServiceDefinition definition = ServerServiceDefinition.builder(grpcService)
                .addMethod(record, ServerCalls.asyncUnaryCall((request, out) -> {
                    if (unavailable.get()) {
                        out.onError(Status.UNAVAILABLE.withDescription("draining")
                                .asRuntimeException());
                        return;
                    }
                    long delay = unaryDelayMs.get();
                    if (delay > 0) {
                        try {
                            Thread.sleep(delay);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    long seq = seqOf(request);
                    if (seq < 0) {
                        out.onError(Status.INVALID_ARGUMENT.withDescription("negative seq")
                                .asRuntimeException());
                        return;
                    }
                    unarySeen.add(seq);
                    out.onNext(ack(1));
                    out.onCompleted();
                }))
                .addMethod(batch, ServerCalls.asyncClientStreamingCall(out ->
                        new StreamObserver<DynamicMessage>() {
                            long count;

                            @Override
                            public void onNext(DynamicMessage value) {
                                count++;
                                batchTotal.incrementAndGet();
                            }

                            @Override
                            public void onError(Throwable t) {
                                // Client hung up.
                            }

                            @Override
                            public void onCompleted() {
                                out.onNext(ack(count));
                                out.onCompleted();
                            }
                        }))
                .addMethod(feed, ServerCalls.asyncServerStreamingCall((request, out) -> {
                    out.onNext(ack(1));
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
        unarySeen.clear();
        batchTotal.set(0);
        unavailable.set(false);
        unaryDelayMs.set(0);
        callDeadlines.clear();
    }

    /** Records the deadline every call is dispatched with, without altering the call. */
    private static final ClientInterceptor DEADLINE_RECORDER = new ClientInterceptor() {
        @Override
        public <Q, S> ClientCall<Q, S> interceptCall(
                io.grpc.MethodDescriptor<Q, S> method, CallOptions options, Channel next) {
            callDeadlines.add(options.getDeadline());
            return next.newCall(method, options);
        }
    };

    private static long seqOf(DynamicMessage event) {
        return (long) event.getField(event.getDescriptorForType().findFieldByName("seq"));
    }

    private static DynamicMessage ack(long count) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(
                file.findMessageTypeByName("Ack"));
        builder.setField(builder.getDescriptorForType().findFieldByName("count"), count);
        return builder.build();
    }

    private static DynamicMessage event(long seq) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(
                file.findMessageTypeByName("Event"));
        builder.setField(builder.getDescriptorForType().findFieldByName("seq"), seq);
        return builder.build();
    }

    private static String descriptorSetBase64() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("sink/test/sink.proto", PROTO, "test").build());
        return Base64.getEncoder().encodeToString(compiled.descriptorSet().toByteArray());
    }

    private Map<String, String> config(String method, String format) throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(GrpcSinkConfig.TARGET, "in-process");
        props.put(GrpcSinkConfig.METHOD, method);
        props.put(GrpcSinkConfig.DESCRIPTOR_SET, descriptorSetBase64());
        props.put(GrpcSinkConfig.VALUE_FORMAT, format);
        return props;
    }

    private GrpcSinkTask startTask(Map<String, String> props) {
        GrpcSinkTask started = new GrpcSinkTask();
        started.channelFactory = config -> InProcessChannelBuilder.forName(serverName)
                .intercept(DEADLINE_RECORDER)
                .build();
        started.start(props);
        task = started;
        return started;
    }

    private static SinkRecord record(Object value, long offset) {
        return new SinkRecord("events", 0, null, null, null, value, offset);
    }

    @Test
    void unaryMethodDeliversOneCallPerRecord() throws Exception {
        GrpcSinkTask sink = startTask(config("sink.test.Collector/Record", "protobuf"));
        List<SinkRecord> records = new ArrayList<>();
        for (long i = 0; i < 5; i++) {
            records.add(record(event(i).toByteArray(), i));
        }
        sink.put(records);
        assertThat(unarySeen).containsExactly(0L, 1L, 2L, 3L, 4L);
    }

    /**
     * A gRPC deadline is an absolute instant, so one built for the whole batch leaves the last
     * record of a slow batch less time than the first — and eventually none. Each call must
     * carry its own, which shows up as the second call's deadline sitting later than the first's
     * by roughly the time the first call took.
     */
    @Test
    void everyRecordInABatchCarriesItsOwnDeadline() throws Exception {
        Map<String, String> props = config("sink.test.Collector/Record", "protobuf");
        props.put(GrpcSinkConfig.DEADLINE_MS, "30000");
        GrpcSinkTask sink = startTask(props);
        unaryDelayMs.set(150);

        sink.put(List.of(record(event(1).toByteArray(), 0), record(event(2).toByteArray(), 1)));

        assertThat(callDeadlines).hasSize(2).doesNotContainNull();
        long first = callDeadlines.get(0).timeRemaining(TimeUnit.MILLISECONDS);
        long second = callDeadlines.get(1).timeRemaining(TimeUnit.MILLISECONDS);
        assertThat(second - first)
                .as("the second call's deadline is set after the first call returned")
                .isGreaterThanOrEqualTo(100);
    }

    @Test
    void clientStreamingMethodDeliversOneStreamPerBatch() throws Exception {
        GrpcSinkTask sink = startTask(config("sink.test.Collector/RecordBatch", "protobuf"));
        List<SinkRecord> records = new ArrayList<>();
        for (long i = 0; i < 100; i++) {
            records.add(record(event(i).toByteArray(), i));
        }
        sink.put(records);
        assertThat(batchTotal.get()).isEqualTo(100);
    }

    @Test
    void confluentFramedValuesAreUnwrapped() throws Exception {
        GrpcSinkTask sink = startTask(config("sink.test.Collector/Record", "confluent"));
        ByteArrayOutputStream framed = new ByteArrayOutputStream();
        framed.write(0);
        framed.writeBytes(new byte[] {0, 0, 0, 7});   // schema id 7
        framed.write(0);                               // message-indexes: [0]
        framed.writeBytes(event(42).toByteArray());
        sink.put(List.of(record(framed.toByteArray(), 0)));
        assertThat(unarySeen).containsExactly(42L);
    }

    @Test
    void jsonValuesParseAsProto3Json() throws Exception {
        GrpcSinkTask sink = startTask(config("sink.test.Collector/Record", "json"));
        sink.put(List.of(record("{\"seq\": \"7\", \"note\": \"hi\"}", 0)));
        assertThat(unarySeen).containsExactly(7L);
    }

    @Test
    void transientStatusesAreRetriable() throws Exception {
        GrpcSinkTask sink = startTask(config("sink.test.Collector/Record", "protobuf"));
        unavailable.set(true);
        assertThatThrownBy(() -> sink.put(List.of(record(event(1).toByteArray(), 0))))
                .isInstanceOf(RetriableException.class)
                .hasMessageContaining("UNAVAILABLE");
    }

    @Test
    void permanentStatusesAreHardFailures() throws Exception {
        GrpcSinkTask sink = startTask(config("sink.test.Collector/Record", "protobuf"));
        assertThatThrownBy(() -> sink.put(List.of(record(event(-1).toByteArray(), 0))))
                .isInstanceOf(ConnectException.class)
                .isNotInstanceOf(RetriableException.class)
                .hasMessageContaining("INVALID_ARGUMENT");
    }

    @Test
    void undecodableValuesAreDataExceptions() throws Exception {
        GrpcSinkTask sink = startTask(config("sink.test.Collector/Record", "json"));
        assertThatThrownBy(() -> sink.put(List.of(record("{not json", 0))))
                .isInstanceOf(DataException.class);
        assertThatThrownBy(() -> sink.put(List.of(record(null, 0))))
                .isInstanceOf(DataException.class);
    }

    @Test
    void serverStreamingMethodsAreRejectedAtStart() throws Exception {
        Map<String, String> props = config("sink.test.Collector/Feed", "protobuf");
        assertThatThrownBy(() -> startTask(props))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("server-streaming");
    }

    @Test
    void connectorValidatesEagerlyAndFansOutTasks() throws Exception {
        GrpcSinkConnector connector = new GrpcSinkConnector();
        Map<String, String> props = config("sink.test.Collector/Record", "protobuf");
        connector.start(props);
        assertThat(connector.taskConfigs(3)).hasSize(3);
        assertThat(connector.taskClass()).isEqualTo(GrpcSinkTask.class);
        connector.stop();

        Map<String, String> bad = new HashMap<>(props);
        bad.put(GrpcSinkConfig.METHOD, "sink.test.Collector/Nope");
        assertThatThrownBy(() -> new GrpcSinkConnector().start(bad))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("Nope");
    }
}
