package ai.protomolt.proto.validate.protovalidate;

import ai.protomolt.proto.validate.RuleCompilationException;
import build.buf.validate.ValidateProto;
import com.google.protobuf.ByteString;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

/**
 * Recovers {@code buf.validate} annotations that a descriptor kept only as unknown fields.
 *
 * <p>A descriptor linked without the {@code buf.validate} extension registry — a
 * {@code FileDescriptorSet} parsed with plain {@code parseFrom}, say — still carries the rule
 * options, but as unknown bytes rather than as recognised extensions. Re-parsing the options
 * against a knowing registry is what keeps those rules from being silently dropped, which is the
 * failure mode that reports success.</p>
 */
final class OptionReparse {

    /** Registry knowing the {@code buf.validate} option extensions, for reparsing options. */
    private static final ExtensionRegistry EXTENSIONS = createExtensionRegistry();

    private OptionReparse() {
    }

    private static ExtensionRegistry createExtensionRegistry() {
        ExtensionRegistry registry = ExtensionRegistry.newInstance();
        ValidateProto.registerAllExtensions(registry);
        return registry;
    }

    /** Parses {@code options}' bytes against the buf.validate registry; failures are schema errors. */
    static <T extends Message> T reparse(T options, OptionsParser<T> parser, String extensionName) {
        try {
            return parser.parse(options.toByteString(), EXTENSIONS);
        } catch (InvalidProtocolBufferException e) {
            throw new RuleCompilationException(
                    "cannot reparse options carrying " + extensionName + ": " + e.getMessage(), e);
        }
    }

    /** Recovers a declared option and rejects occurrences the parser could not consume. */
    static <T extends Message> T recover(T options, int number, OptionsParser<T> parser, String name) {
        if (!options.getUnknownFields().hasField(number)) {
            return options;
        }
        T recovered = reparse(options, parser, name);
        if (recovered.getUnknownFields().hasField(number)) {
            throw new RuleCompilationException("invalid wire encoding for " + name);
        }
        return recovered;
    }

    @FunctionalInterface
    interface OptionsParser<T> {
        T parse(ByteString bytes, ExtensionRegistry registry) throws InvalidProtocolBufferException;
    }
}
