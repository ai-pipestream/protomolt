package ai.pipestream.proto.kafka.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;

import java.util.Map;

/**
 * Configuration for the gRPC sink: where to call, which method, the schema that declares it,
 * and how record values are decoded into the method's request message.
 */
public final class GrpcSinkConfig extends AbstractConfig {

    public static final String TARGET = "grpc.target";
    public static final String METHOD = "grpc.method";
    public static final String DESCRIPTOR_SET = "schema.descriptor.set.base64";
    public static final String VALUE_FORMAT = "value.format";
    public static final String DEADLINE_MS = "grpc.deadline.ms";
    public static final String API_TOKEN = "grpc.api.token";
    public static final String PLAINTEXT = "grpc.plaintext";

    /** How a record value becomes the request message. */
    public enum ValueFormat {
        /** The value bytes are the serialized request message. */
        PROTOBUF,
        /** Confluent wire format: magic byte, schema id, message indexes, then the message. */
        CONFLUENT,
        /** The value is the request message as canonical proto3 JSON text. */
        JSON
    }

    public static ConfigDef definition() {
        return new ConfigDef()
                .define(TARGET, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                        "gRPC target, e.g. 'grpc-host:9090' or 'dns:///svc:443'.")
                .define(METHOD, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                        "Fully qualified method as 'package.Service/Method'. Unary methods are "
                                + "called once per record; client-streaming methods receive one "
                                + "stream per delivered batch.")
                .define(DESCRIPTOR_SET, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                        "Base64-encoded serialized google.protobuf.FileDescriptorSet declaring "
                                + "the service (e.g. from ProtoMolt's compile or reflect verbs, "
                                + "or a registry descriptor-set endpoint).")
                .define(VALUE_FORMAT, ConfigDef.Type.STRING, "protobuf",
                        ConfigDef.CaseInsensitiveValidString.in("protobuf", "confluent", "json"),
                        ConfigDef.Importance.MEDIUM,
                        "How record values decode into the request message: raw 'protobuf' "
                                + "bytes, 'confluent' wire format (framed with a schema id), or "
                                + "proto3 'json' text.")
                .define(DEADLINE_MS, ConfigDef.Type.LONG, 30_000L,
                        ConfigDef.Range.atLeast(1), ConfigDef.Importance.MEDIUM,
                        "Deadline per call (unary) or per delivered batch (client-streaming).")
                .define(API_TOKEN, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.MEDIUM,
                        "Optional shared secret sent as 'api_token' metadata on every call.")
                .define(PLAINTEXT, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM,
                        "Use plaintext transport; set false for TLS.");
    }

    public GrpcSinkConfig(Map<String, String> props) {
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

    public ValueFormat valueFormat() {
        return ValueFormat.valueOf(getString(VALUE_FORMAT).toUpperCase(java.util.Locale.ROOT));
    }

    public long deadlineMs() {
        return getLong(DEADLINE_MS);
    }

    public String apiToken() {
        Password password = getPassword(API_TOKEN);
        return password == null ? null : password.value();
    }

    public boolean plaintext() {
        return getBoolean(PLAINTEXT);
    }
}
