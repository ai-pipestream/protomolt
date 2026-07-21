package ai.pipestream.proto.kafka.serde;

import ai.pipestream.proto.cel.CelEnvironmentFactory;
import ai.pipestream.proto.cel.CelEvaluator;
import ai.pipestream.proto.cel.CelMappingRule;
import ai.pipestream.proto.cel.CelProtoMapper;
import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.mapper.ProtoFieldMapper;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import org.apache.kafka.common.config.ConfigException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The serde as a transformer: an ordered list of mapping rules applied to every record.
 *
 * <p>This is what Confluent's data contracts call migration rules, done with the mapping
 * machinery this project already has instead of a second expression language. On the write path
 * the rules run <em>before</em> validation, so a producer can normalize (fill derived fields,
 * strip scratch data) and the contract judges the normalized message. On the read path they run
 * right after parsing, which is how a consumer reshapes records written before a schema moved —
 * without waiting for every producer to upgrade.</p>
 *
 * <p>Two rule forms share one ordered list:</p>
 * <ul>
 *   <li><b>Text rules</b>, exactly as in the mapping docs: {@code target.path = source.path},
 *       {@code repeated.path += source.path}, {@code -path.to.clear}.</li>
 *   <li><b>CEL rules</b>: {@code target.path := <selector>} or
 *       {@code target.path := <selector> if <filter>}, where both expressions see the current
 *       message as {@code input}. The filter separator is the <em>last</em> {@code " if "} in
 *       the entry.</li>
 * </ul>
 *
 * <p>Rules are the deployment's, not the schema's, so they apply to whatever type flows through;
 * a rule naming a field the type does not have fails that record rather than being silently
 * skipped. An entry whose shape is not a rule at all fails at configure time. CEL expressions
 * are compiled against the message type, which is not known until a record arrives, so a CEL
 * expression that does not compile fails the first record of that type.</p>
 */
final class SerdeMapper {

    /** Bound on the per-type CEL mapper cache; a serde sees a handful of types. */
    private static final int MAX_CACHED_TYPES = 64;
    private static final String CEL_MARKER = " := ";
    private static final String FILTER_MARKER = " if ";

    private sealed interface Step {
        record Text(String rule) implements Step {
        }

        record Cel(String target, String selector, String filter) implements Step {
        }
    }

    private final List<Step> steps;
    private final ProtoFieldMapper fieldMapper = new ProtoFieldMapperImpl(
            DescriptorRegistry.create());
    private final ConcurrentMap<Descriptor, CelProtoMapper> byType = new ConcurrentHashMap<>();

    private SerdeMapper(List<Step> steps) {
        this.steps = steps;
    }

    /** @return null when no rules are configured, so the record path pays nothing */
    static SerdeMapper parse(List<String> rules, String configKey) {
        if (rules == null || rules.isEmpty()) {
            return null;
        }
        List<Step> steps = new ArrayList<>(rules.size());
        for (String rule : rules) {
            String trimmed = rule.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            steps.add(parseStep(trimmed, configKey));
        }
        return steps.isEmpty() ? null : new SerdeMapper(List.copyOf(steps));
    }

    private static Step parseStep(String rule, String configKey) {
        int cel = rule.indexOf(CEL_MARKER);
        if (cel < 0) {
            // A text rule: assignment, append, or clear.
            if (rule.startsWith("-") || rule.contains("=")) {
                return new Step.Text(rule);
            }
            throw new ConfigException(configKey + " entry is not a mapping rule: '" + rule
                    + "'. Expected 'target = source', 'target += source', '-field', or "
                    + "'target := <cel>' optionally followed by ' if <cel>'.");
        }
        String target = rule.substring(0, cel).trim();
        String rest = rule.substring(cel + CEL_MARKER.length()).trim();
        if (target.isEmpty() || rest.isEmpty()) {
            throw new ConfigException(configKey + " entry '" + rule
                    + "' needs a target path and a CEL selector around ':='");
        }
        int filter = rest.lastIndexOf(FILTER_MARKER);
        if (filter < 0) {
            return new Step.Cel(target, rest, null);
        }
        String selector = rest.substring(0, filter).trim();
        String condition = rest.substring(filter + FILTER_MARKER.length()).trim();
        if (selector.isEmpty() || condition.isEmpty()) {
            throw new ConfigException(configKey + " entry '" + rule
                    + "' has an empty selector or filter around ' if '");
        }
        return new Step.Cel(target, selector, condition);
    }

    /**
     * The message with every rule applied, in order, each seeing the previous rules' work. The
     * result is built from the message's own builder, so a generated class stays a generated
     * class.
     *
     * @throws ai.pipestream.proto.mapper.MappingException  when a rule's path does not fit the
     *                                                      message
     * @throws ai.pipestream.proto.cel.CelEvaluationException when a CEL expression fails to
     *                                                        compile or evaluate
     */
    Message apply(Message message) throws ai.pipestream.proto.mapper.MappingException {
        Message.Builder builder = message.toBuilder();
        CelProtoMapper cel = null;
        for (Step step : steps) {
            if (step instanceof Step.Text text) {
                fieldMapper.mapInPlace(builder, List.of(text.rule()));
            } else if (step instanceof Step.Cel rule) {
                if (cel == null) {
                    cel = celMapperFor(message.getDescriptorForType());
                }
                cel.map(builder, List.of(new CelMappingRule(
                        rule.filter(), rule.selector(), rule.target())));
            }
        }
        return builder.build();
    }

    /** A CEL mapper whose {@code input} is typed as the message, compiled once per type. */
    private CelProtoMapper celMapperFor(Descriptor descriptor) {
        CelProtoMapper existing = byType.get(descriptor);
        if (existing != null) {
            return existing;
        }
        if (byType.size() >= MAX_CACHED_TYPES) {
            byType.clear();
        }
        return byType.computeIfAbsent(descriptor, d -> new CelProtoMapper(fieldMapper,
                new CelEvaluator(CelEnvironmentFactory.builder()
                        .addMessageVar("input", d)
                        .build())));
    }
}
