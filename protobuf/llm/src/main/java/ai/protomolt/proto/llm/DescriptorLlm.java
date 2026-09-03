package ai.protomolt.proto.llm;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.ExtensionRegistry;

import java.util.Objects;
import java.util.Optional;

/**
 * Reads {@code (ai.pipestream.proto.llm.v1.field|message)} options from descriptors.
 * Register extensions before parsing descriptor sets that carry them.
 */
public final class DescriptorLlm {

    private DescriptorLlm() {
    }

    public static void registerExtensions(ExtensionRegistry registry) {
        LlmProto.registerAllExtensions(Objects.requireNonNull(registry, "registry"));
    }

    public static Optional<FieldLlm> field(FieldDescriptor field) {
        Objects.requireNonNull(field, "field");
        var options = field.getOptions();
        if (!options.hasExtension(LlmProto.field)) {
            return Optional.empty();
        }
        return Optional.of(options.getExtension(LlmProto.field));
    }

    public static Optional<MessageLlm> message(Descriptor descriptor) {
        Objects.requireNonNull(descriptor, "descriptor");
        var options = descriptor.getOptions();
        if (!options.hasExtension(LlmProto.message)) {
            return Optional.empty();
        }
        return Optional.of(options.getExtension(LlmProto.message));
    }
}
