package ai.protomolt.proto.compat;

import ai.protomolt.proto.descriptors.ApiStability;
import ai.protomolt.proto.descriptors.StabilityTier;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Deterministic snapshot of action catalog verb contracts (action envelopes), including
 * required scopes, streaming capabilities, input/output message schemas, and stability tiers.
 *
 * @param actions sorted list of action envelope entries
 */
@ApiStability(StabilityTier.STABLE)
public record ActionEnvelopeSnapshot(List<ActionEnvelopeEntry> actions) {

    public ActionEnvelopeSnapshot {
        Objects.requireNonNull(actions, "actions");
        List<ActionEnvelopeEntry> copy = new ArrayList<>(actions);
        copy.sort(Comparator.comparing(ActionEnvelopeEntry::name));
        actions = List.copyOf(copy);
    }

    /**
     * Indexes the captured actions by their kebab-case action name.
     *
     * @return immutable map of action name to {@link ActionEnvelopeEntry}
     */
    public Map<String, ActionEnvelopeEntry> actionsByName() {
        Map<String, ActionEnvelopeEntry> map = new LinkedHashMap<>();
        for (ActionEnvelopeEntry entry : actions) {
            map.put(entry.name(), entry);
        }
        return Map.copyOf(map);
    }

    /**
     * Summarizes the fields of a protobuf message {@link Descriptor} as a sorted map of
     * field name to wire/type signature.
     *
     * @param descriptor the protobuf message descriptor
     * @return immutable sorted map of field name to type summary
     */
    public static Map<String, String> describeMessageFields(Descriptor descriptor) {
        if (descriptor == null) {
            return Map.of();
        }
        Map<String, String> fields = new TreeMap<>();
        for (FieldDescriptor field : descriptor.getFields()) {
            fields.put(field.getName(), formatFieldSignature(field));
        }
        return Collections.unmodifiableMap(fields);
    }

    /**
     * Extracts sorted required input property names from an action's JSON Schema
     * {@link ObjectNode}.
     *
     * @param inputSchema JSON Schema object node
     * @return immutable sorted list of required property names
     */
    public static List<String> extractRequiredFields(ObjectNode inputSchema) {
        if (inputSchema == null) {
            return List.of();
        }
        JsonNode requiredNode = inputSchema.get("required");
        if (requiredNode == null || !requiredNode.isArray()) {
            return List.of();
        }
        List<String> required = new ArrayList<>();
        for (JsonNode item : requiredNode) {
            if (item.isTextual() && !item.asText().trim().isEmpty()) {
                required.add(item.asText().trim());
            }
        }
        required.sort(Comparator.naturalOrder());
        return List.copyOf(required);
    }

    private static String formatFieldSignature(FieldDescriptor field) {
        if (field.isMapField()) {
            Descriptor entry = field.getMessageType();
            FieldDescriptor key = entry.findFieldByNumber(1);
            FieldDescriptor val = entry.findFieldByNumber(2);
            return "map<" + scalarOrTypeName(key) + "," + scalarOrTypeName(val) + ">";
        }
        String prefix = field.isRepeated() ? "repeated " : "";
        return prefix + scalarOrTypeName(field);
    }

    private static String scalarOrTypeName(FieldDescriptor field) {
        if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
            return field.getMessageType().getFullName();
        }
        if (field.getJavaType() == FieldDescriptor.JavaType.ENUM) {
            return field.getEnumType().getFullName();
        }
        return field.getType().name();
    }

    /**
     * Contract snapshot of a single action envelope.
     *
     * @param name                kebab-case action name
     * @param requiredScope       required authorization scope (empty for unscoped)
     * @param streaming           whether the action supports incremental streaming
     * @param requestTypeName     fully-qualified protobuf request message type name
     * @param responseTypeName    fully-qualified protobuf response message type name
     * @param requiredInputFields sorted required input property names from the JSON Schema
     * @param inputFieldTypes     sorted map of request field name to type signature
     * @param outputFieldTypes    sorted map of response field name to type signature
     * @param stability           stability tier of this action contract
     */
    @ApiStability(StabilityTier.STABLE)
    public record ActionEnvelopeEntry(
            String name,
            String requiredScope,
            boolean streaming,
            String requestTypeName,
            String responseTypeName,
            List<String> requiredInputFields,
            Map<String, String> inputFieldTypes,
            Map<String, String> outputFieldTypes,
            StabilityTier stability) {

        public ActionEnvelopeEntry {
            if (name == null || name.trim().isEmpty()) {
                throw new IllegalArgumentException("name must not be blank");
            }
            name = name.trim();
            requiredScope = requiredScope == null ? "" : requiredScope.trim();
            if (requestTypeName == null || requestTypeName.trim().isEmpty()) {
                throw new IllegalArgumentException("requestTypeName must not be blank");
            }
            requestTypeName = requestTypeName.trim();
            if (responseTypeName == null || responseTypeName.trim().isEmpty()) {
                throw new IllegalArgumentException("responseTypeName must not be blank");
            }
            responseTypeName = responseTypeName.trim();

            List<String> reqCopy = new ArrayList<>(
                    requiredInputFields == null ? List.of() : requiredInputFields);
            reqCopy.sort(Comparator.naturalOrder());
            requiredInputFields = List.copyOf(reqCopy);

            inputFieldTypes = Collections.unmodifiableMap(
                    new TreeMap<>(inputFieldTypes == null ? Map.of() : inputFieldTypes));
            outputFieldTypes = Collections.unmodifiableMap(
                    new TreeMap<>(outputFieldTypes == null ? Map.of() : outputFieldTypes));
            Objects.requireNonNull(stability, "stability");
        }

        /**
         * Creates an {@link ActionEnvelopeEntry} from protobuf request/response descriptors
         * and the action's published JSON Schema.
         */
        public static ActionEnvelopeEntry fromDescriptors(
                String name,
                String requiredScope,
                boolean streaming,
                Descriptor requestType,
                Descriptor responseType,
                ObjectNode inputSchema,
                StabilityTier stability) {
            Objects.requireNonNull(requestType, "requestType");
            Objects.requireNonNull(responseType, "responseType");
            return new ActionEnvelopeEntry(
                    name,
                    requiredScope,
                    streaming,
                    requestType.getFullName(),
                    responseType.getFullName(),
                    extractRequiredFields(inputSchema),
                    describeMessageFields(requestType),
                    describeMessageFields(responseType),
                    stability);
        }
    }
}
