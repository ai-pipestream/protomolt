package ai.pipestream.proto.kafka.connect;

import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Locale;
import java.util.Map;

/**
 * Validates each record value against the validation rules declared on its protobuf schema
 * ({@code ai.pipestream.proto.validate.v1} options). Valid records pass through untouched.
 * Invalid records fail the record (the worker's error tolerance routes them — fail, skip,
 * or dead-letter queue), are dropped, or pass through with the violations attached as a
 * JSON header, per {@code on.invalid}. Tombstones (null values) always pass through.
 *
 * <p>Descriptor-native: the schema arrives as a serialized descriptor set, so the rules
 * travel with the schema and no generated classes are needed in the worker.</p>
 */
public class ValidateMessage<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String ON_INVALID = "on.invalid";
    public static final String HEADER_NAME = "header.name";

    public static final ConfigDef CONFIG_DEF = ValueCodec.baseConfigDef()
            .define(ON_INVALID, ConfigDef.Type.STRING, "fail",
                    ConfigDef.CaseInsensitiveValidString.in("fail", "drop", "header"),
                    ConfigDef.Importance.MEDIUM,
                    "What to do with an invalid record: 'fail' it (the worker's error "
                            + "tolerance then applies — fail, skip, or DLQ), 'drop' it, or "
                            + "'header' — pass it through with the violations as a JSON "
                            + "header.")
            .define(HEADER_NAME, ConfigDef.Type.STRING, "protomolt.violations",
                    ConfigDef.Importance.LOW,
                    "Header carrying the violations JSON when on.invalid=header.");

    private ValueCodec codec;
    private ProtoValidator validator;
    private String onInvalid;
    private String headerName;

    @Override
    public void configure(Map<String, ?> props) {
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, props);
        codec = ValueCodec.fromConfig(config);
        validator = ProtoValidator.forMessageType(codec.type());
        onInvalid = config.getString(ON_INVALID).toLowerCase(Locale.ROOT);
        headerName = config.getString(HEADER_NAME);
    }

    @Override
    public R apply(R record) {
        if (record.value() == null) {
            return record;
        }
        ValidationResult result = validator.validate(
                codec.decode(record.value(), "topic " + record.topic()));
        if (result.valid()) {
            return record;
        }
        String violations = violationsJson(result);
        switch (onInvalid) {
            case "drop" -> {
                return null;
            }
            case "header" -> {
                R out = record.newRecord(record.topic(), record.kafkaPartition(),
                        record.keySchema(), record.key(),
                        record.valueSchema(), record.value(), record.timestamp());
                out.headers().addString(headerName, violations);
                return out;
            }
            default -> throw new DataException("Message failed validation ("
                    + codec.type().getFullName() + ", topic " + record.topic() + "): "
                    + violations);
        }
    }

    private static String violationsJson(ValidationResult result) {
        ListValue.Builder list = ListValue.newBuilder();
        for (ValidationResult.Violation violation : result.violations()) {
            Struct.Builder entry = Struct.newBuilder();
            entry.putFields("field", Value.newBuilder()
                    .setStringValue(violation.path()).build());
            entry.putFields("rule", Value.newBuilder()
                    .setStringValue(violation.rulePath()).build());
            entry.putFields("ruleId", Value.newBuilder()
                    .setStringValue(violation.ruleId()).build());
            entry.putFields("message", Value.newBuilder()
                    .setStringValue(violation.message()).build());
            list.addValues(Value.newBuilder().setStructValue(entry));
        }
        try {
            return JsonFormat.printer().omittingInsignificantWhitespace()
                    .print(list.build());
        } catch (Exception e) {
            throw new DataException("Failed to render violations: " + e.getMessage(), e);
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        // Nothing held.
    }
}
