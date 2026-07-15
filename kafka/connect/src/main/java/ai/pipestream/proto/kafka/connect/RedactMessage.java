package ai.pipestream.proto.kafka.connect;

import ai.pipestream.proto.meta.SensitivityMasker;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Masks record values by their schema-declared sensitivity classes
 * ({@code ai.pipestream.proto.meta.v1.field.sensitivity}) — declare {@code pii} once in
 * the proto and every topic this transform touches honors it, with zero per-deployment
 * field lists. {@code remove} clears the fields; {@code redact} turns strings into
 * {@code ***}. Recurses through nested and repeated messages; tombstones pass through.
 */
public class RedactMessage<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String CLASSES = "classes";
    public static final String STRATEGY = "strategy";
    public static final String KEY = "key";

    public static final ConfigDef CONFIG_DEF = ValueCodec.baseConfigDef()
            .define(CLASSES, ConfigDef.Type.LIST, List.of("pii"),
                    ConfigDef.Importance.HIGH,
                    "Sensitivity classes to mask, e.g. 'pii,secret'.")
            .define(STRATEGY, ConfigDef.Type.STRING, "remove",
                    ConfigDef.CaseInsensitiveValidString.in(
                            "remove", "redact", "encrypt", "decrypt"),
                    ConfigDef.Importance.MEDIUM,
                    "'remove' clears masked fields; 'redact' turns strings into ***; "
                            + "'encrypt' seals string/bytes with AES-GCM (reversible only "
                            + "with the key) and clears other types; 'decrypt' reverses.")
            .define(KEY, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.MEDIUM,
                    "Base64 AES key (16/24/32 bytes); required for encrypt/decrypt.");

    private ValueCodec codec;
    private Set<String> classes;
    private SensitivityMasker.Strategy strategy;
    private byte[] key;

    @Override
    public void configure(Map<String, ?> props) {
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, props);
        codec = ValueCodec.fromConfig(config);
        classes = new LinkedHashSet<>(config.getList(CLASSES));
        strategy = SensitivityMasker.Strategy.of(
                config.getString(STRATEGY).toUpperCase(Locale.ROOT));
        var password = config.getPassword(KEY);
        key = password == null ? null : java.util.Base64.getDecoder().decode(password.value());
        if ((strategy == SensitivityMasker.Strategy.ENCRYPT
                || strategy == SensitivityMasker.Strategy.DECRYPT) && key == null) {
            throw new org.apache.kafka.common.config.ConfigException(
                    "'" + KEY + "' is required for strategy " + strategy);
        }
    }

    @Override
    public R apply(R record) {
        if (record.value() == null) {
            return record;
        }
        SensitivityMasker.MaskResult masked = SensitivityMasker.mask(
                codec.decode(record.value(), "topic " + record.topic()), classes, strategy,
                key);
        if (masked.maskedPaths().isEmpty()) {
            return record;
        }
        return record.newRecord(record.topic(), record.kafkaPartition(),
                record.keySchema(), record.key(),
                record.valueSchema(), codec.encode(masked.message(), record.value()),
                record.timestamp());
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
