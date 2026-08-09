package ai.pipestream.proto.kafka.connect;

import ai.pipestream.proto.grpc.invoke.DynamicGrpcCalls;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
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
import io.grpc.stub.StreamObserver;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code grpc.api.token} contract against a live in-process service: the configured token
 * reaches the server as {@code api_token} metadata on every call — unary and client-streaming
 * alike — and no token is sent when none is configured.
 */
class GrpcSinkTaskHeadersTest {

    private static final String PROTO = """
            syntax = "proto3";
            package hdr.test;
            message Event { int64 seq = 1; }
            message Ack { int64 count = 1; }
            service Collector {
              rpc Record(Event) returns (Ack);
              rpc RecordBatch(stream Event) returns (Ack);
            }
            """;

    private static final Metadata.Key<String> API_TOKEN =
            Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER);

    private static FileDescriptor file;
    private static Server server;
    private static String serverName;
    private static final List<String> apiTokens = new CopyOnWriteArrayList<>();
    private static final AtomicLong received = new AtomicLong();

    private GrpcSinkTask task;

    @BeforeAll
    static void start() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("hdr/test/hdr.proto", PROTO, "test").build());
        file = compiled.descriptorFor("hdr/test/hdr.proto").orElseThrow();
        ServiceDescriptor service = file.findServiceByName("Collector");

        var record = DynamicGrpcCalls.methodDescriptor(service.findMethodByName("Record"));
        var batch = DynamicGrpcCalls.methodDescriptor(service.findMethodByName("RecordBatch"));
        io.grpc.ServiceDescriptor grpcService = io.grpc.ServiceDescriptor
                .newBuilder(service.getFullName())
                .setSchemaDescriptor((io.grpc.protobuf.ProtoFileDescriptorSupplier) () -> file)
                .addMethod(record).addMethod(batch)
                .build();

        ServerServiceDefinition definition = ServerServiceDefinition.builder(grpcService)
                .addMethod(record, ServerCalls.asyncUnaryCall((request, out) -> {
                    received.incrementAndGet();
                    out.onNext(ack());
                    out.onCompleted();
                }))
                .addMethod(batch, ServerCalls.asyncClientStreamingCall(out ->
                        new StreamObserver<DynamicMessage>() {
                            @Override
                            public void onNext(DynamicMessage value) {
                                received.incrementAndGet();
                            }

                            @Override
                            public void onError(Throwable t) {
                                // Client hung up.
                            }

                            @Override
                            public void onCompleted() {
                                out.onNext(ack());
                                out.onCompleted();
                            }
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
        apiTokens.clear();
        received.set(0);
    }

    private static DynamicMessage ack() {
        return DynamicMessage.newBuilder(file.findMessageTypeByName("Ack")).build();
    }

    private static DynamicMessage event(long seq) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(
                file.findMessageTypeByName("Event"));
        builder.setField(builder.getDescriptorForType().findFieldByName("seq"), seq);
        return builder.build();
    }

    private static String descriptorSetBase64() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("hdr/test/hdr.proto", PROTO, "test").build());
        return Base64.getEncoder().encodeToString(compiled.descriptorSet().toByteArray());
    }

    private GrpcSinkTask startTask(String method, String apiToken) throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(GrpcSinkConfig.TARGET, "in-process");
        props.put(GrpcSinkConfig.METHOD, method);
        props.put(GrpcSinkConfig.DESCRIPTOR_SET, descriptorSetBase64());
        if (apiToken != null) {
            props.put(GrpcSinkConfig.API_TOKEN, apiToken);
        }
        GrpcSinkTask started = new GrpcSinkTask();
        started.channelFactory = config -> InProcessChannelBuilder.forName(serverName).build();
        started.start(props);
        task = started;
        return started;
    }

    private static SinkRecord record(long seq) {
        return new SinkRecord("events", 0, null, null, null, event(seq).toByteArray(), seq);
    }

    @Test
    void unaryCallsCarryTheApiToken() throws Exception {
        GrpcSinkTask sink = startTask("hdr.test.Collector/Record", "s3cr3t");
        sink.put(List.of(record(1), record(2)));
        assertThat(received.get()).isEqualTo(2);
        assertThat(apiTokens).containsExactly("s3cr3t", "s3cr3t");
    }

    @Test
    void clientStreamingCallsCarryTheApiToken() throws Exception {
        GrpcSinkTask sink = startTask("hdr.test.Collector/RecordBatch", "s3cr3t");
        sink.put(List.of(record(1), record(2), record(3)));
        assertThat(received.get()).isEqualTo(3);
        assertThat(apiTokens).containsExactly("s3cr3t");
    }

    @Test
    void noConfiguredTokenSendsNoHeader() throws Exception {
        GrpcSinkTask sink = startTask("hdr.test.Collector/Record", null);
        sink.put(List.of(record(1)));
        assertThat(apiTokens).containsExactly((String) null);
    }
}
