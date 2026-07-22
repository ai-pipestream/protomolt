package ai.pipestream.proto.kafka.connect;

import ai.pipestream.proto.cel.CelCompilationException;
import ai.pipestream.proto.cel.CelEnvironmentFactory;
import ai.pipestream.proto.cel.CelEvaluationException;
import ai.pipestream.proto.cel.CelEvaluator;
import ai.pipestream.proto.cel.CelMappingRule;
import ai.pipestream.proto.cel.CelProtoMapper;
import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.mapper.MappingException;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Reshapes each record value in place with ProtoMolt's field-mapping rules: text rules like
 * {@code note = details.summary} or {@code -internal_field}, and/or CEL rules
 * {@code {filter?, selector?, target, fallback?}} where expressions see the current message
 * as {@code input}. The message type is unchanged, so Confluent-framed values keep their
 * original frame (the schema id stays true) and downstream deserializers are undisturbed.
 * Tombstones (null values) pass through.
 */
public class MapMessage<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String RULES = "rules";
    public static final String CEL_RULES_JSON = "cel.rules.json";

    public static final ConfigDef CONFIG_DEF = ValueCodec.baseConfigDef()
            .define(RULES, ConfigDef.Type.LIST, List.of(), ConfigDef.Importance.HIGH,
                    "Text mapping rules applied in order: 'target = source.path', "
                            + "'target += source.path', '-field.to.clear'. Paths are "
                            + "protobuf field paths on the message itself.")
            .define(CEL_RULES_JSON, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "JSON array of CEL mapping rules applied after 'rules', each "
                            + "{\"filter\"?: bool-CEL, \"selector\"?: value-CEL, "
                            + "\"target\": field path, \"fallback\"?: [text rules]}; "
                            + "expressions see the progressive message as 'input'.");

    private ValueCodec codec;
    private ProtoFieldMapperImpl fieldMapper;
    private List<String> textRules;
    private List<CelMappingRule> celRules;
    private CelProtoMapper celMapper;

    @Override
    public void configure(Map<String, ?> props) {
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, props);
        List<FileDescriptor> files = GrpcConnectorSupport.linkedFiles(
                config.getString(ValueCodec.DESCRIPTOR_SET));
        codec = new ValueCodec(
                GrpcConnectorSupport.messageType(files, config.getString(ValueCodec.MESSAGE_TYPE)),
                config.getString(ValueCodec.VALUE_FORMAT));
        textRules = List.copyOf(config.getList(RULES));
        celRules = parseCelRules(config.getString(CEL_RULES_JSON));
        if (textRules.isEmpty() && celRules.isEmpty()) {
            throw new ConfigException("At least one of '" + RULES + "' or '"
                    + CEL_RULES_JSON + "' must be provided");
        }
        DescriptorRegistry registry = DescriptorRegistry.create();
        files.forEach(registry::registerFile);
        fieldMapper = new ProtoFieldMapperImpl(registry);
        if (!celRules.isEmpty()) {
            CelEvaluator evaluator = new CelEvaluator(CelEnvironmentFactory.builder()
                    .addMessageVar("input", codec.type())
                    .build());
            for (CelMappingRule rule : celRules) {
                precompile(evaluator, rule.filterExpression());
                precompile(evaluator, rule.selectorExpression());
            }
            celMapper = new CelProtoMapper(fieldMapper, evaluator);
        }
    }

    @Override
    public R apply(R record) {
        if (record.value() == null) {
            return record;
        }
        DynamicMessage message = codec.decode(record.value(), "topic " + record.topic());
        Message.Builder builder = message.toBuilder();
        try {
            if (!textRules.isEmpty()) {
                fieldMapper.mapInPlace(builder, textRules);
            }
            if (celMapper != null) {
                celMapper.map(builder, celRules);
            }
        } catch (MappingException | CelEvaluationException e) {
            throw new DataException("Mapping failed (" + codec.type().getFullName()
                    + ", topic " + record.topic() + "): " + e.getMessage(), e);
        }
        return record.newRecord(record.topic(), record.kafkaPartition(),
                record.keySchema(), record.key(),
                record.valueSchema(), codec.encode(builder.build(), record.value()),
                record.timestamp());
    }

    private static List<CelMappingRule> parseCelRules(String json) {
        if (json == null || json.isBlank()) {
            return List.of();
        }
        Value.Builder parsed = Value.newBuilder();
        try {
            JsonFormat.parser().merge(json, parsed);
        } catch (Exception e) {
            throw new ConfigException(CEL_RULES_JSON, json,
                    "not valid JSON: " + e.getMessage());
        }
        if (!parsed.hasListValue()) {
            throw new ConfigException(CEL_RULES_JSON, json, "must be a JSON array");
        }
        List<CelMappingRule> rules = new ArrayList<>();
        for (Value element : parsed.getListValue().getValuesList()) {
            if (!element.hasStructValue()) {
                throw new ConfigException(CEL_RULES_JSON, json,
                        "each rule must be a JSON object");
            }
            Struct rule = element.getStructValue();
            Value target = rule.getFieldsMap().get("target");
            if (target == null || target.getStringValue().isBlank()) {
                throw new ConfigException(CEL_RULES_JSON, json,
                        "each rule needs a 'target' field path");
            }
            List<String> fallback = new ArrayList<>();
            Value fallbackValue = rule.getFieldsMap().get("fallback");
            if (fallbackValue != null) {
                for (Value entry : fallbackValue.getListValue().getValuesList()) {
                    fallback.add(entry.getStringValue());
                }
            }
            rules.add(new CelMappingRule(
                    stringField(rule, "filter"),
                    stringField(rule, "selector"),
                    target.getStringValue(),
                    fallback));
        }
        return List.copyOf(rules);
    }

    private static String stringField(Struct struct, String name) {
        Value value = struct.getFieldsMap().get(name);
        return value == null || value.getStringValue().isBlank()
                ? null
                : value.getStringValue();
    }

    private static void precompile(CelEvaluator evaluator, String expression) {
        if (expression == null) {
            return;
        }
        try {
            evaluator.precompile(expression);
        } catch (CelCompilationException e) {
            throw new ConfigException("'" + CEL_RULES_JSON + "' expression does not compile: "
                    + e.getMessage());
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
