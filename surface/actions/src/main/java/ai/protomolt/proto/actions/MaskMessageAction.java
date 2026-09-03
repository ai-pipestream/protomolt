package ai.protomolt.proto.actions;

import ai.protomolt.proto.http.json.MalformedProtobufJsonException;
import ai.protomolt.proto.meta.SensitivityMasker;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Masks a message's fields by their schema-declared sensitivity classes — the masking
 * policy travels inside the descriptor, so every surface enforces exactly what the schema
 * authors declared.
 */
final class MaskMessageAction implements ProtoAction {

    /**
     * Proto enum values carry their type name, so the wire form of REDACT is
     * MASK_STRATEGY_REDACT. The masker's own enum does not, and the two are otherwise
     * the same vocabulary.
     */
    private static final String STRATEGY_PREFIX = "MASK_STRATEGY_";

    @Override
    public String name() {
        return "mask-message";
    }

    @Override
    public String requiredScope() {
        return Scopes.SCHEMA_READ;
    }

    @Override
    public String description() {
        return "Masks fields whose declared sensitivity class "
                + "(ai.pipestream.proto.meta.v1.field.sensitivity) is in 'classes' — e.g. "
                + "pii, secret. Strategies: 'remove' clears; 'redact' turns strings into "
                + "***; 'encrypt' seals string/bytes values with AES-GCM (reversible only "
                + "with the same key) and clears other types; 'decrypt' reverses encrypt "
                + "and fails loudly on a wrong key. Recurses through nested and repeated "
                + "messages, and into google.protobuf.Any payloads whose type the schema "
                + "carries. Returns the masked message, which field paths were touched, and "
                + "'unresolvedPayloads': packed payloads whose type the schema does not "
                + "describe, which therefore could not be masked.";
    }

    @Override
    public Descriptor requestType() {
        return CatalogContract.request("MaskMessageRequest");
    }

    @Override
    public Descriptor responseType() {
        return CatalogContract.response("MaskMessageResponse");
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        SchemaResolver.ResolvedSchema schema = SchemaResolver.resolve(input, "schema", context);
        Descriptor descriptor = schema.message(named(input, "type"), "/type");
        // The message is a structure: its shape is the named type, not this contract.
        ObjectNode messageNode = Fields.json(input, "message");
        Set<String> classes = new LinkedHashSet<>(Fields.strings(input, "classes"));
        if (classes.isEmpty()) {
            throw Inputs.invalidInput("'classes' must be a non-empty array", "/classes");
        }
        // The contract names the strategy with an enum, so an unknown one is refused
        // before the verb runs and whatever arrives is a value the masker implements.
        String strategyName = Fields.enumName(input, "strategy");
        SensitivityMasker.Strategy strategy =
                strategyName.isEmpty() || strategyName.endsWith("UNSPECIFIED")
                ? SensitivityMasker.Strategy.REMOVE
                : SensitivityMasker.Strategy.of(strategyName.startsWith(STRATEGY_PREFIX)
                        ? strategyName.substring(STRATEGY_PREFIX.length()) : strategyName);
        byte[] key = null;
        String keyText = Fields.string(input, "key");
        if (!keyText.isEmpty()) {
            try {
                key = java.util.Base64.getDecoder().decode(keyText);
            } catch (IllegalArgumentException e) {
                throw Inputs.invalidInput("'key' must be base64", "/key");
            }
        }
        if ((strategy == SensitivityMasker.Strategy.ENCRYPT
                || strategy == SensitivityMasker.Strategy.DECRYPT)
                && (key == null || (key.length != 16 && key.length != 24 && key.length != 32))) {
            throw Inputs.invalidInput("'key' must be an AES key of 16, 24, or 32 bytes for "
                    + strategy.name().toLowerCase(java.util.Locale.ROOT), "/key");
        }
        DynamicMessage message;
        try {
            message = context.transcoder().fromJsonDynamic(messageNode.toString(), descriptor);
        } catch (MalformedProtobufJsonException e) {
            throw Inputs.invalidInput("Message is not valid proto3 JSON for "
                    + descriptor.getFullName(), "/message");
        }
        // Packed payloads resolve against everything this call can see: the schema first,
        // which carries types the root proto never imports, then the registry, which is how a
        // payload type arrives when the schema was given inline.
        SensitivityMasker.PayloadResolver payloads = typeName -> {
            Descriptor found = schema.findMessage(typeName);
            return found != null ? found : context.registry().findDescriptorByFullName(typeName);
        };
        SensitivityMasker.MaskResult result;
        try {
            result = SensitivityMasker.mask(message, classes, strategy, key, payloads);
        } catch (IllegalArgumentException e) {
            // The key is validated above, so what reaches here is about the payload: a value
            // that is not one of our envelopes, or one this key cannot open.
            throw Inputs.invalidInput(e.getMessage(), "/message");
        }
        return Reply.of(responseType())
                .set("message", context.transcoder().toJson(result.message()))
                .addAll("maskedFields", result.maskedPaths())
                // Empty unless it happened: these payloads were not masked, and the caller
                // is the only one who can say whether that is acceptable.
                .addAll("unresolvedPayloads", result.unresolvedPaths())
                .build();
    }

    /** A named type, or null when the caller left the schema's own default to apply. */
    private static String named(Message input, String field) {
        String value = Fields.string(input, field);
        return value.isEmpty() ? null : value;
    }

}
