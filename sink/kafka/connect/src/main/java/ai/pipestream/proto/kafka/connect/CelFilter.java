package ai.pipestream.proto.kafka.connect;

import ai.pipestream.proto.cel.CelCompilationException;
import ai.pipestream.proto.cel.CelEnvironmentFactory;
import ai.pipestream.proto.cel.CelEvaluator;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Locale;
import java.util.Map;

/**
 * Keeps records for which a CEL predicate over the decoded message (bound as {@code input})
 * is true and drops the rest — a protobuf-aware filter/router where the stock Connect
 * predicates are byte-blind. The expression is compiled and type-checked against the
 * message type at configure time. Tombstones (null values) always pass through.
 */
public class CelFilter<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String EXPRESSION = "expression";
    public static final String ON_ERROR = "on.error";

    public static final ConfigDef CONFIG_DEF = ValueCodec.baseConfigDef()
            .define(EXPRESSION, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                    "Boolean CEL expression over the decoded message (bound as 'input'); "
                            + "true keeps the record, false drops it. "
                            + "Example: \"input.qty > 0 && input.region == 'us-east'\".")
            .define(ON_ERROR, ConfigDef.Type.STRING, "fail",
                    ConfigDef.CaseInsensitiveValidString.in("fail", "keep", "drop"),
                    ConfigDef.Importance.MEDIUM,
                    "What to do when the value does not decode or the expression fails at "
                            + "runtime: 'fail' the record (worker error tolerance applies), "
                            + "'keep' it, or 'drop' it.");

    private ValueCodec codec;
    private CelEvaluator evaluator;
    private String expression;
    private String onError;

    @Override
    public void configure(Map<String, ?> props) {
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, props);
        codec = ValueCodec.fromConfig(config);
        expression = config.getString(EXPRESSION);
        onError = config.getString(ON_ERROR).toLowerCase(Locale.ROOT);
        evaluator = new CelEvaluator(CelEnvironmentFactory.builder()
                .addMessageVar("input", codec.type())
                .build());
        try {
            evaluator.precompile(expression);
        } catch (CelCompilationException e) {
            throw new ConfigException("'" + EXPRESSION + "' does not compile: "
                    + e.getMessage());
        }
    }

    @Override
    public R apply(R record) {
        if (record.value() == null) {
            return record;
        }
        boolean keep;
        try {
            keep = evaluator.evaluateBooleanOrFail(expression,
                    Map.of("input", codec.decode(record.value(), "topic " + record.topic())));
        } catch (Exception e) {
            switch (onError) {
                case "keep" -> {
                    return record;
                }
                case "drop" -> {
                    return null;
                }
                default -> throw new DataException("Filter failed ("
                        + codec.type().getFullName() + ", topic " + record.topic() + "): "
                        + e.getMessage(), e);
            }
        }
        return keep ? record : null;
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
