package ai.protomolt.proto.validate.protovalidate;

import build.buf.validate.Rule;
import com.google.protobuf.Descriptors.FieldDescriptor;

import java.util.List;
import java.util.Map;

/**
 * The {@code (buf.validate.predefined)} extensions visible from one proto file, keyed by the
 * {@code buf.validate.<T>Rules} type they extend and then by extension number — the two facts
 * needed to match an extension set on a field's sub-rules message back to its CEL rules.
 *
 * <p>Built and cached by {@link PredefinedRules}; consumed by {@link FieldRuleTranslation}, which
 * carries it down through nested {@code items}/{@code keys}/{@code values} rules.</p>
 */
record PredefinedIndex(Map<String, Map<Integer, PredefinedIndex.Ext>> bySubRules) {

    static final PredefinedIndex EMPTY = new PredefinedIndex(Map.of());

    /** One predefined extension: its descriptor and the CEL rules attached to it. */
    record Ext(FieldDescriptor descriptor, List<Rule> rules) {
    }

    Map<Integer, Ext> forType(String rulesTypeFullName) {
        return bySubRules.getOrDefault(rulesTypeFullName, Map.of());
    }

    boolean isEmpty() {
        return bySubRules.isEmpty();
    }
}
