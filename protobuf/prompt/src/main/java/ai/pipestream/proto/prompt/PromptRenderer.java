package ai.pipestream.proto.prompt;

import ai.pipestream.proto.jsonschema.ProtoJsonSchemaGenerator;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import ai.pipestream.proto.validate.spi.ValidationRuleSource;
import ai.pipestream.proto.validate.spi.ValidationRuleSources;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.util.JsonFormat.TypeRegistry;

import java.util.List;
import java.util.Objects;

/**
 * Renders a protobuf descriptor into a {@link PromptPacket}: the complete briefing for a
 * model asked to fill the form. Pure function over descriptors — no registry I/O, no
 * model calls, no state.
 *
 * <p>The request is validated with the full validation framework before anything is
 * rendered (dogfooding: the packet's own contract is enforced by the same rules the
 * packet teaches), and every failure is loud: invalid requests, target/descriptor
 * mismatches, and unresolvable overrides all throw {@link PromptRenderException}.
 */
public final class PromptRenderer {

    private final List<ValidationRuleSource> sources;
    private final ProtoValidator validator;

    private PromptRenderer(List<ValidationRuleSource> sources) {
        this.sources = List.copyOf(Objects.requireNonNull(sources, "sources"));
        this.validator = ProtoValidator.create(sources);
    }

    /** Uses the default rule-source chain ({@link ValidationRuleSources#defaults()}). */
    public static PromptRenderer create() {
        return new PromptRenderer(ValidationRuleSources.defaults());
    }

    /** As {@link #create()} but with an explicit rule-source chain. */
    public static PromptRenderer create(List<ValidationRuleSource> sources) {
        return new PromptRenderer(sources);
    }

    /**
     * Renders the packet. This overload rejects a request carrying overrides: resolving
     * an {@code Any} into instruction text requires a type registry, and guessing is not
     * a default this class is willing to make.
     */
    public PromptPacket render(Descriptor descriptor, RenderPromptRequest request,
            String descriptorSetRef) {
        return render(descriptor, request, descriptorSetRef, null);
    }

    /**
     * Renders the packet.
     *
     * @param descriptor the descriptor of the message type to fill; its full name must
     *     equal the request's {@code target_type}
     * @param request the render request, validated before use
     * @param descriptorSetRef opaque registry reference echoed into the packet so the
     *     receiver can build a {@code DynamicMessage} without guessing
     * @param typeRegistry resolves the request's {@code overrides} into instruction
     *     text; required exactly when the request carries overrides
     */
    public PromptPacket render(Descriptor descriptor, RenderPromptRequest request,
            String descriptorSetRef, TypeRegistry typeRegistry) {
        Objects.requireNonNull(descriptor, "descriptor");
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(descriptorSetRef, "descriptorSetRef");

        ValidationResult result = validator.validate(request);
        if (!result.valid()) {
            throw PromptRenderException.requestInvalid(result);
        }
        if (!descriptor.getFullName().equals(request.getTargetType())) {
            throw new PromptRenderException("target_type '" + request.getTargetType()
                    + "' does not name the descriptor being rendered ('"
                    + descriptor.getFullName() + "')");
        }
        if (request.hasOverrides() && typeRegistry == null) {
            throw new PromptRenderException("request carries overrides of type '"
                    + request.getOverrides().getTypeUrl()
                    + "' but no TypeRegistry was supplied to resolve them");
        }

        String instructions = InstructionRenderer.render(descriptor, request, sources, typeRegistry);
        String jsonSchema = ProtoJsonSchemaGenerator.create(sources).generateJson(descriptor);

        PromptPacket.Builder packet = PromptPacket.newBuilder()
                .setTargetType(descriptor.getFullName())
                .setDescriptorSetRef(descriptorSetRef)
                .setInstructions(instructions)
                .setResponseJsonSchema(jsonSchema);
        if (request.hasPersona()) {
            packet.setPersona(request.getPersona());
            packet.addAllFewShot(request.getPersona().getFewShotList());
        }
        return packet.build();
    }
}
