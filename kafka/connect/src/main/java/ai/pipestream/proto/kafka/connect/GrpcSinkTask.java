package ai.pipestream.proto.kafka.connect;

import ai.pipestream.proto.grpc.invoke.DynamicGrpcCalls;
import ai.pipestream.proto.kafka.wire.ConfluentWireFormat;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * The sink task: each delivered record becomes the gRPC method's request message. Unary
 * methods are invoked once per record; client-streaming methods receive the whole delivered
 * batch as one stream and complete it, so the service acknowledges per batch.
 *
 * <p>Transient gRPC statuses (UNAVAILABLE, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED, ABORTED)
 * surface as {@link RetriableException} so the framework redelivers; anything else is a
 * hard {@link ConnectException}. Undecodable record values are {@link DataException}s,
 * which the framework routes by its configured error tolerance (fail or DLQ).</p>
 */
public final class GrpcSinkTask extends SinkTask {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcSinkTask.class);

    private static final EnumSet<Status.Code> RETRIABLE = EnumSet.of(
            Status.Code.UNAVAILABLE,
            Status.Code.DEADLINE_EXCEEDED,
            Status.Code.RESOURCE_EXHAUSTED,
            Status.Code.ABORTED);

    /** Test hook: replaces the channel construction. */
    Function<GrpcSinkConfig, ManagedChannel> channelFactory =
            config -> GrpcConnectorSupport.channel(config.target(), config.plaintext());

    private GrpcSinkConfig config;
    private ManagedChannel channel;
    private MethodDescriptor method;
    private Metadata headers;

    @Override
    public void start(Map<String, String> props) {
        config = new GrpcSinkConfig(props);
        method = resolveMethod(config);
        if (method.isServerStreaming()) {
            throw new ConnectException("Method " + method.getFullName()
                    + " is server-streaming; the sink supports unary and client-streaming methods");
        }
        headers = new Metadata();
        if (config.apiToken() != null) {
            headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER),
                    config.apiToken());
        }
        channel = channelFactory.apply(config);
        LOG.info("gRPC sink started: {} -> {} ({})", config.target(), config.method(),
                method.isClientStreaming() ? "client-streaming per batch" : "unary per record");
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        List<DynamicMessage> requests = new ArrayList<>(records.size());
        for (SinkRecord record : records) {
            requests.add(decode(record));
        }
        try {
            if (method.isClientStreaming()) {
                // The batch is one call, so one deadline covers it.
                DynamicGrpcCalls.callClientStreaming(
                        channel, method, requests.iterator(), callOptions(), cloneHeaders());
            } else {
                for (DynamicMessage request : requests) {
                    DynamicGrpcCalls.call(channel, method, request, callOptions(), cloneHeaders(), 1);
                }
            }
        } catch (StatusRuntimeException e) {
            if (RETRIABLE.contains(e.getStatus().getCode())) {
                throw new RetriableException("gRPC " + e.getStatus().getCode() + " from "
                        + config.target() + ": " + e.getStatus().getDescription(), e);
            }
            throw new ConnectException("gRPC " + e.getStatus().getCode() + " from "
                    + config.target() + ": " + e.getStatus().getDescription(), e);
        }
    }

    /**
     * A deadline is an absolute instant once set, so it is derived per call: a single one built
     * for the batch would leave the last record of a large batch with none of it left.
     */
    private CallOptions callOptions() {
        return CallOptions.DEFAULT.withDeadlineAfter(config.deadlineMs(), TimeUnit.MILLISECONDS);
    }

    private DynamicMessage decode(SinkRecord record) {
        Object value = record.value();
        if (value == null) {
            throw new DataException("Record value is null (topic " + record.topic()
                    + ", offset " + record.kafkaOffset() + ")");
        }
        try {
            switch (config.valueFormat()) {
                case PROTOBUF -> {
                    return DynamicMessage.parseFrom(method.getInputType(), asBytes(value));
                }
                case CONFLUENT -> {
                    return DynamicMessage.parseFrom(method.getInputType(),
                            ConfluentWireFormat.payload(asBytes(value)));
                }
                default -> {
                    String json = value instanceof byte[] bytes
                            ? new String(bytes, StandardCharsets.UTF_8)
                            : value.toString();
                    DynamicMessage.Builder builder = DynamicMessage.newBuilder(method.getInputType());
                    JsonFormat.parser().ignoringUnknownFields().merge(json, builder);
                    return builder.build();
                }
            }
        } catch (DataException e) {
            throw e;
        } catch (Exception e) {
            throw new DataException("Record value does not decode as "
                    + method.getInputType().getFullName() + " (" + config.valueFormat()
                    + ", topic " + record.topic() + ", offset " + record.kafkaOffset() + "): "
                    + e.getMessage(), e);
        }
    }

    private static byte[] asBytes(Object value) {
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        throw new DataException("Record value must be byte[] for this format; got "
                + value.getClass().getName()
                + " (use the ByteArrayConverter for value.converter)");
    }

    private Metadata cloneHeaders() {
        Metadata copy = new Metadata();
        copy.merge(headers);
        return copy;
    }

    @Override
    public void stop() {
        if (channel != null) {
            channel.shutdown();
            try {
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                channel.shutdownNow();
            }
        }
    }

    @Override
    public String version() {
        return GrpcSinkConnector.pluginVersion();
    }

    static MethodDescriptor resolveMethod(GrpcSinkConfig config) {
        return GrpcConnectorSupport.resolveMethod(config.descriptorSetBase64(), config.method());
    }
}
