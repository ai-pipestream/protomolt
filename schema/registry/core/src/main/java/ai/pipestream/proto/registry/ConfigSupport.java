package ai.pipestream.proto.registry;

import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * The registry door's gate for config documents. A document is an
 * envelope — {@code {"messageType": "...", "config": {...}}} — whose
 * config is proto3 JSON of a type the registry already serves: the type
 * resolves against the registered schemas (a config whose schema is not
 * registered refuses naming the type), the JSON parses strictly, and the
 * type's declared rules run against the parsed message, so an invalid
 * document never reaches Git. The consumer re-validates on apply with
 * the same rules; the door is the first belt, never the only one.
 */
public final class ConfigSupport {

    /** The envelope field naming the document's type: {@value}. */
    public static final String MESSAGE_TYPE = "messageType";

    /** The envelope field carrying the proto3-JSON document: {@value}. */
    public static final String CONFIG = "config";

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final ProtoSourceCompiler COMPILER = new ProtoSourceCompiler();
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private ConfigSupport() {
    }

    /**
     * One gated config document.
     *
     * @param messageType the resolved type's full name
     * @param message the parsed, rules-checked document
     */
    public record Gated(String messageType, DynamicMessage message) {
    }

    /**
     * Gates one envelope: envelope shape, type resolution, strict parse,
     * declared rules — refusing loudly at the first failure.
     *
     * @param store the registry the type must be registered in
     * @param envelopeJson the envelope document
     * @return the gated document
     * @throws InvalidConfigException naming exactly what was refused
     */
    public static Gated gate(SchemaRegistryStore store, String envelopeJson)
            throws InvalidConfigException {
        JsonNode envelope;
        try {
            envelope = JSON.readTree(envelopeJson);
        } catch (Exception e) {
            throw new InvalidConfigException(
                    "the envelope is not JSON: " + e.getMessage());
        }
        JsonNode type = envelope.path(MESSAGE_TYPE);
        JsonNode config = envelope.path(CONFIG);
        if (!type.isTextual() || type.asText().isBlank() || !config.isObject()) {
            throw new InvalidConfigException("the envelope must be {\"" + MESSAGE_TYPE
                    + "\": \"<full message name>\", \"" + CONFIG + "\": {...}}");
        }
        Descriptor descriptor = resolveType(store, type.asText())
                .orElseThrow(() -> new InvalidConfigException("type '" + type.asText()
                        + "' is not served by this registry: register its schema first"));
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        try {
            JsonFormat.parser().merge(config.toString(), builder);
        } catch (Exception e) {
            throw new InvalidConfigException("the config is not a valid "
                    + type.asText() + ": " + e.getMessage());
        }
        DynamicMessage message = builder.build();
        ValidationResult validated = VALIDATOR.validate(message);
        if (!validated.valid()) {
            throw new InvalidConfigException("the config violates the type's own"
                    + " declared rules: " + validated.violations().stream()
                            .map(violation -> violation.path() + ": " + violation.message())
                            .collect(Collectors.joining("; ")));
        }
        return new Gated(type.asText(), message);
    }

    /**
     * Resolves a message type against the registered schemas by compiling
     * each subject's latest version (with its resolved references) until
     * one links the type, nested messages included.
     *
     * @param store the registry
     * @param fullName the message type's full name
     * @return the linked descriptor, or empty when no subject serves it
     */
    public static Optional<Descriptor> resolveType(
            SchemaRegistryStore store, String fullName) {
        for (String subject : store.subjects()) {
            Optional<StoredSchema> latest = store.latest(subject);
            if (latest.isEmpty()) {
                continue;
            }
            CompiledProtos compiled;
            try {
                StoredSchemaSources.Resolved resolved = StoredSchemaSources.resolve(
                        store, subject, latest.get().schemaText(),
                        latest.get().references());
                compiled = COMPILER.compile(resolved.sources());
            } catch (Exception e) {
                // A subject that no longer links cannot serve the type;
                // the next subject may.
                continue;
            }
            for (FileDescriptor file : compiled.fileDescriptors()) {
                Descriptor found = find(file, fullName);
                if (found != null) {
                    return Optional.of(found);
                }
            }
        }
        return Optional.empty();
    }

    private static Descriptor find(FileDescriptor file, String fullName) {
        for (Descriptor message : file.getMessageTypes()) {
            Descriptor found = find(message, fullName);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private static Descriptor find(Descriptor message, String fullName) {
        if (message.getFullName().equals(fullName)) {
            return message;
        }
        for (Descriptor nested : message.getNestedTypes()) {
            Descriptor found = find(nested, fullName);
            if (found != null) {
                return found;
            }
        }
        return null;
    }
}
