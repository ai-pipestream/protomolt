package ai.pipestream.proto.kafka.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;

import java.util.Map;

/**
 * Configuration for the gRPC source: which server-streaming method to subscribe to, the
 * schema that declares it, the topic the streamed messages land on, and how the stream
 * resumes after a restart (a CEL expression extracts a resume token from each message; the
 * token is stored as the Connect offset and injected back into the subscribe request).
 */
public final class GrpcSourceConfig extends AbstractConfig {

    public static final String TARGET = "grpc.target";
    public static final String METHOD = "grpc.method";
    public static final String DESCRIPTOR_SET = "schema.descriptor.set.base64";
    public static final String TOPIC = "topic";
    public static final String REQUEST_JSON = "grpc.request.json";
    public static final String RESUME_TOKEN_CEL = "resume.token.cel";
    public static final String RESUME_TOKEN_FIELD = "resume.token.request.field";
    public static final String KEY_CEL = "record.key.cel";
    public static final String VALUE_FORMAT = "value.format";
    public static final String POLL_MAX_RECORDS = "poll.max.records";
    public static final String POLL_TIMEOUT_MS = "poll.timeout.ms";
    public static final String RECONNECT_BACKOFF_MS = "reconnect.backoff.ms";
    public static final String API_TOKEN = "grpc.api.token";
    public static final String PLAINTEXT = "grpc.plaintext";

    /** How a streamed message becomes the record value. */
    public enum ValueFormat {
        /** The value is the serialized message bytes (pair with the ByteArrayConverter). */
        PROTOBUF,
        /** The value is the message as canonical proto3 JSON text (pair with the StringConverter). */
        JSON
    }

    public static ConfigDef definition() {
        return new ConfigDef()
                .define(TARGET, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                        "gRPC target, e.g. 'grpc-host:9090' or 'dns:///svc:443'.")
                .define(METHOD, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                        "Fully qualified server-streaming method as 'package.Service/Method'; "
                                + "every streamed message becomes one record.")
                .define(DESCRIPTOR_SET, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                        "Base64-encoded serialized google.protobuf.FileDescriptorSet declaring "
                                + "the service (e.g. from ProtoMolt's compile or reflect verbs, "
                                + "or a registry descriptor-set endpoint).")
                .define(TOPIC, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                        "Kafka topic the streamed messages are written to.")
                .define(REQUEST_JSON, ConfigDef.Type.STRING, "{}",
                        ConfigDef.Importance.MEDIUM,
                        "The subscribe request message, as canonical proto3 JSON.")
                .define(RESUME_TOKEN_CEL, ConfigDef.Type.STRING, null,
                        ConfigDef.Importance.MEDIUM,
                        "CEL expression over each streamed message (bound as 'input') that "
                                + "yields the message's resume token, stored as the Connect "
                                + "offset (e.g. 'input.cursor'). Without it the source has no "
                                + "offsets and every (re)start subscribes from the request as "
                                + "configured.")
                .define(RESUME_TOKEN_FIELD, ConfigDef.Type.STRING, null,
                        ConfigDef.Importance.MEDIUM,
                        "Dotted path of a string field in the request message where the stored "
                                + "resume token is injected when (re)subscribing "
                                + "(e.g. 'resume_token' or 'position.cursor').")
                .define(KEY_CEL, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW,
                        "Optional CEL expression over each streamed message (bound as 'input') "
                                + "that yields the record key as a string.")
                .define(VALUE_FORMAT, ConfigDef.Type.STRING, "protobuf",
                        ConfigDef.CaseInsensitiveValidString.in("protobuf", "json"),
                        ConfigDef.Importance.MEDIUM,
                        "How streamed messages encode into record values: serialized "
                                + "'protobuf' bytes or canonical proto3 'json' text.")
                .define(POLL_MAX_RECORDS, ConfigDef.Type.INT, 500,
                        ConfigDef.Range.atLeast(1), ConfigDef.Importance.MEDIUM,
                        "Maximum records returned per poll.")
                .define(POLL_TIMEOUT_MS, ConfigDef.Type.LONG, 1_000L,
                        ConfigDef.Range.atLeast(1), ConfigDef.Importance.LOW,
                        "How long a poll waits for the stream to produce before returning "
                                + "what it has.")
                .define(RECONNECT_BACKOFF_MS, ConfigDef.Type.LONG, 1_000L,
                        ConfigDef.Range.atLeast(0), ConfigDef.Importance.LOW,
                        "Pause before resubscribing after the stream ends or fails with a "
                                + "transient status.")
                .define(API_TOKEN, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.MEDIUM,
                        "Optional shared secret sent as 'api_token' metadata on the call.")
                .define(PLAINTEXT, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM,
                        "Use plaintext transport; set false for TLS.");
    }

    public GrpcSourceConfig(Map<String, String> props) {
        super(definition(), props);
    }

    public String target() {
        return getString(TARGET);
    }

    public String method() {
        return getString(METHOD);
    }

    public String descriptorSetBase64() {
        return getString(DESCRIPTOR_SET);
    }

    public String topic() {
        return getString(TOPIC);
    }

    public String requestJson() {
        return getString(REQUEST_JSON);
    }

    public String resumeTokenCel() {
        return getString(RESUME_TOKEN_CEL);
    }

    public String resumeTokenField() {
        return getString(RESUME_TOKEN_FIELD);
    }

    public String keyCel() {
        return getString(KEY_CEL);
    }

    public ValueFormat valueFormat() {
        return ValueFormat.valueOf(getString(VALUE_FORMAT).toUpperCase(java.util.Locale.ROOT));
    }

    public int pollMaxRecords() {
        return getInt(POLL_MAX_RECORDS);
    }

    public long pollTimeoutMs() {
        return getLong(POLL_TIMEOUT_MS);
    }

    public long reconnectBackoffMs() {
        return getLong(RECONNECT_BACKOFF_MS);
    }

    public String apiToken() {
        Password password = getPassword(API_TOKEN);
        return password == null ? null : password.value();
    }

    public boolean plaintext() {
        return getBoolean(PLAINTEXT);
    }
}
