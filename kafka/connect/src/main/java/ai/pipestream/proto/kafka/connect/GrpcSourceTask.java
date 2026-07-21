package ai.pipestream.proto.kafka.connect;

import ai.pipestream.proto.cel.CelCompilationException;
import ai.pipestream.proto.cel.CelEnvironmentFactory;
import ai.pipestream.proto.cel.CelEvaluationException;
import ai.pipestream.proto.cel.CelEvaluator;
import ai.pipestream.proto.grpc.invoke.DynamicGrpcCalls;
import ai.pipestream.proto.grpc.invoke.DynamicGrpcStream;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * The source task: one long-lived server-streaming call, drained in batches — the stream's
 * flow control is the poll loop's pace, so the local buffer stays bounded no matter how fast
 * the server produces. Each streamed message becomes one record on the configured topic.
 *
 * <p>Resume: when {@code resume.token.cel} is set, the expression is evaluated over each
 * message and the result rides along as the record's Connect offset. On task (re)start the
 * last committed token is read back and, when {@code resume.token.request.field} names a
 * string field of the request message, injected into the subscribe request — so the server
 * can resume the stream where Kafka left off. Delivery is at-least-once: after a mid-stream
 * failure the task resubscribes from the latest <em>emitted</em> token, which may replay
 * messages Kafka already has.</p>
 *
 * <p>Transient statuses (UNAVAILABLE, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED, ABORTED) and
 * graceful stream completion resubscribe after {@code reconnect.backoff.ms}; anything else
 * fails the task.</p>
 */
public final class GrpcSourceTask extends SourceTask {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcSourceTask.class);

    /** Key of the resume token inside the Connect source offset map. */
    static final String OFFSET_TOKEN = "resume.token";

    private static final EnumSet<Status.Code> RECONNECT = EnumSet.of(
            Status.Code.UNAVAILABLE,
            Status.Code.DEADLINE_EXCEEDED,
            Status.Code.RESOURCE_EXHAUSTED,
            Status.Code.ABORTED);

    /** Test hook: replaces the channel construction. */
    Function<GrpcSourceConfig, ManagedChannel> channelFactory =
            config -> GrpcConnectorSupport.channel(config.target(), config.plaintext());

    private GrpcSourceConfig config;
    private Prepared prepared;
    private Map<String, String> sourcePartition;
    private Metadata headers;
    private ManagedChannel channel;
    // poll() opens and drops the stream; stop() reads it from the worker's thread, so the
    // publication has to be safe or stop() can miss the stream it needs to cancel.
    private volatile DynamicGrpcStream stream;
    private String lastToken;
    private long reopenNotBeforeNanos;
    private boolean tokenFailureLogged;
    private volatile boolean stopped;

    /** Everything derived from the config that must validate before any task runs. */
    record Prepared(MethodDescriptor method,
                    DynamicMessage requestTemplate,
                    List<FieldDescriptor> tokenPath,
                    CelEvaluator evaluator) {
    }

    /**
     * Resolves and validates the configured method, request template, token field path, and
     * CEL expressions — everything that should fail at connector start, not first poll.
     */
    static Prepared prepare(GrpcSourceConfig config) {
        MethodDescriptor method = GrpcConnectorSupport.resolveMethod(
                config.descriptorSetBase64(), config.method());
        if (!method.isServerStreaming() || method.isClientStreaming()) {
            throw new ConnectException("Method " + method.getFullName()
                    + " is not server-streaming; the source subscribes to one server stream");
        }
        DynamicMessage requestTemplate;
        try {
            DynamicMessage.Builder builder = DynamicMessage.newBuilder(method.getInputType());
            JsonFormat.parser().merge(config.requestJson(), builder);
            requestTemplate = builder.build();
        } catch (Exception e) {
            throw new ConnectException("'" + GrpcSourceConfig.REQUEST_JSON
                    + "' does not parse as " + method.getInputType().getFullName()
                    + ": " + e.getMessage(), e);
        }
        List<FieldDescriptor> tokenPath = config.resumeTokenField() == null
                ? null
                : resolveTokenPath(method.getInputType(), config.resumeTokenField());
        CelEvaluator evaluator = null;
        if (config.resumeTokenCel() != null || config.keyCel() != null) {
            evaluator = new CelEvaluator(CelEnvironmentFactory.builder()
                    .addMessageVar("input", method.getOutputType())
                    .build());
            precompile(evaluator, GrpcSourceConfig.RESUME_TOKEN_CEL, config.resumeTokenCel());
            precompile(evaluator, GrpcSourceConfig.KEY_CEL, config.keyCel());
        }
        return new Prepared(method, requestTemplate, tokenPath, evaluator);
    }

    @Override
    public void start(Map<String, String> props) {
        config = new GrpcSourceConfig(props);
        prepared = prepare(config);
        sourcePartition = Map.of("target", config.target(), "method", config.method());
        headers = new Metadata();
        if (config.apiToken() != null) {
            headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER),
                    config.apiToken());
        }
        if (context != null) {
            Map<String, Object> offset = context.offsetStorageReader().offset(sourcePartition);
            if (offset != null && offset.get(OFFSET_TOKEN) instanceof String token) {
                lastToken = token;
            }
        }
        channel = channelFactory.apply(config);
        LOG.info("gRPC source started: {} {} -> topic {}{}", config.target(), config.method(),
                config.topic(), lastToken == null ? "" : " (resuming from " + lastToken + ")");
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        if (stopped) {
            return List.of();
        }
        if (stream == null && !awaitReconnectWindow()) {
            return List.of();
        }
        if (stream == null) {
            stream = DynamicGrpcCalls.openServerStream(channel, prepared.method(),
                    subscribeRequest(), CallOptions.DEFAULT, cloneHeaders());
        }
        List<DynamicMessage> messages;
        try {
            messages = stream.take(config.pollMaxRecords(),
                    Duration.ofMillis(config.pollTimeoutMs()));
        } catch (StatusRuntimeException e) {
            dropStream();
            if (stopped) {
                return List.of();
            }
            if (RECONNECT.contains(e.getStatus().getCode())) {
                LOG.warn("stream failed with {} ({}); resubscribing{}",
                        e.getStatus().getCode(), e.getStatus().getDescription(),
                        lastToken == null ? "" : " from " + lastToken);
                return List.of();
            }
            throw new ConnectException("gRPC " + e.getStatus().getCode() + " from "
                    + config.target() + ": " + e.getStatus().getDescription(), e);
        }
        Status terminal = stream.terminalStatus();
        if (terminal != null && terminal.isOk()) {
            LOG.info("stream completed; resubscribing{}",
                    lastToken == null ? "" : " from " + lastToken);
            dropStream();
        }
        List<SourceRecord> records = new ArrayList<>(messages.size());
        for (DynamicMessage message : messages) {
            records.add(toRecord(message));
        }
        return records;
    }

    private SourceRecord toRecord(DynamicMessage message) {
        if (prepared.evaluator() != null && config.resumeTokenCel() != null) {
            String token = evaluate(config.resumeTokenCel(), message, true);
            if (token != null) {
                lastToken = token;
            }
        }
        Map<String, ?> offset = lastToken == null ? null : Map.of(OFFSET_TOKEN, lastToken);
        String key = prepared.evaluator() != null && config.keyCel() != null
                ? evaluate(config.keyCel(), message, false)
                : null;
        Object value;
        Schema valueSchema;
        if (config.valueFormat() == GrpcSourceConfig.ValueFormat.PROTOBUF) {
            value = message.toByteArray();
            valueSchema = Schema.BYTES_SCHEMA;
        } else {
            try {
                value = JsonFormat.printer().print(message);
            } catch (Exception e) {
                throw new DataException("Streamed message does not print as proto3 JSON: "
                        + e.getMessage(), e);
            }
            valueSchema = Schema.STRING_SCHEMA;
        }
        return new SourceRecord(sourcePartition, offset, config.topic(), null,
                key == null ? null : Schema.OPTIONAL_STRING_SCHEMA, key, valueSchema, value);
    }

    /**
     * The two expressions fail differently. A resume token that cannot be computed costs replay
     * at worst, so the previous token stands and the stream carries on; a key that cannot be
     * computed would emit a differently partitioned, non-compacting record under the same name,
     * so it fails the record and the framework routes it by its error tolerance.
     */
    private String evaluate(String expression, DynamicMessage message, boolean isToken) {
        try {
            return tokenString(prepared.evaluator()
                    .evaluateValue(expression, Map.of("input", message)));
        } catch (CelEvaluationException e) {
            if (!isToken) {
                throw new DataException("'" + GrpcSourceConfig.KEY_CEL + "' (" + expression
                        + ") failed over a streamed message: " + e.getMessage(), e);
            }
            if (!tokenFailureLogged) {
                tokenFailureLogged = true;
                LOG.warn("'{}' failed over a streamed message; keeping the previous resume "
                                + "token (logged once): {}",
                        GrpcSourceConfig.RESUME_TOKEN_CEL, e.getMessage());
            }
            return null;
        }
    }

    private static String tokenString(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String text) {
            return text;
        }
        if (value instanceof ByteString bytes) {
            return Base64.getEncoder().encodeToString(bytes.toByteArray());
        }
        return String.valueOf(value);
    }

    private DynamicMessage subscribeRequest() {
        if (prepared.tokenPath() == null || lastToken == null) {
            return prepared.requestTemplate();
        }
        return inject(prepared.requestTemplate(), prepared.tokenPath(), 0, lastToken);
    }

    private static DynamicMessage inject(DynamicMessage message, List<FieldDescriptor> path,
                                         int index, String token) {
        DynamicMessage.Builder builder = message.toBuilder();
        FieldDescriptor field = path.get(index);
        if (index == path.size() - 1) {
            builder.setField(field, token);
        } else {
            builder.setField(field,
                    inject((DynamicMessage) message.getField(field), path, index + 1, token));
        }
        return builder.build();
    }

    static List<FieldDescriptor> resolveTokenPath(Descriptor root, String dotted) {
        List<FieldDescriptor> path = new ArrayList<>();
        Descriptor current = root;
        String[] segments = dotted.split("\\.");
        for (int i = 0; i < segments.length; i++) {
            FieldDescriptor field = current.findFieldByName(segments[i]);
            if (field == null || field.isRepeated()) {
                throw new ConnectException("'" + GrpcSourceConfig.RESUME_TOKEN_FIELD
                        + "': no singular field '" + segments[i] + "' on "
                        + current.getFullName());
            }
            path.add(field);
            if (i == segments.length - 1) {
                if (field.getType() != FieldDescriptor.Type.STRING) {
                    throw new ConnectException("'" + GrpcSourceConfig.RESUME_TOKEN_FIELD
                            + "': field '" + dotted + "' must be a string; it is "
                            + field.getType().name().toLowerCase(java.util.Locale.ROOT));
                }
            } else {
                if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                    throw new ConnectException("'" + GrpcSourceConfig.RESUME_TOKEN_FIELD
                            + "': field '" + segments[i] + "' on " + current.getFullName()
                            + " is not a message; cannot descend into '" + dotted + "'");
                }
                current = field.getMessageType();
            }
        }
        return List.copyOf(path);
    }

    private static void precompile(CelEvaluator evaluator, String key, String expression) {
        if (expression == null) {
            return;
        }
        try {
            evaluator.precompile(expression);
        } catch (CelCompilationException e) {
            throw new ConnectException("'" + key + "' does not compile: " + e.getMessage(), e);
        }
    }

    /**
     * Sleeps out the remainder of the reconnect backoff (bounded by the poll timeout so the
     * task stays responsive to stop). Returns whether the window has fully elapsed.
     */
    private boolean awaitReconnectWindow() throws InterruptedException {
        long remainingNanos = reopenNotBeforeNanos - System.nanoTime();
        if (remainingNanos <= 0) {
            return true;
        }
        long sleepMs = Math.min(TimeUnit.NANOSECONDS.toMillis(remainingNanos) + 1,
                config.pollTimeoutMs());
        Thread.sleep(sleepMs);
        return System.nanoTime() >= reopenNotBeforeNanos;
    }

    private void dropStream() {
        if (stream != null) {
            stream.close();
            stream = null;
        }
        reopenNotBeforeNanos = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(config.reconnectBackoffMs());
    }

    @Override
    public void stop() {
        stopped = true;
        DynamicGrpcStream active = stream;
        if (active != null) {
            active.cancel();
        }
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
        return GrpcConnectorSupport.pluginVersion();
    }

    private Metadata cloneHeaders() {
        Metadata copy = new Metadata();
        copy.merge(headers);
        return copy;
    }
}
