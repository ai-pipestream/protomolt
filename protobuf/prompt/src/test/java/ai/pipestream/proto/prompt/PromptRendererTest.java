package ai.pipestream.proto.prompt;

import ai.pipestream.proto.prompt.testdata.DecoratedOpinion;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.util.JsonFormat.TypeRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PromptRendererTest {

    private static final Descriptor FORM = DecoratedOpinion.getDescriptor();

    private static RenderPromptRequest.Builder request() {
        return RenderPromptRequest.newBuilder().setTargetType(FORM.getFullName());
    }

    private static Persona litigator() {
        return Persona.newBuilder()
                .setId("litigator")
                .setVersion("1.0.0")
                .setInstructions("You fill forms as a practicing litigator researching precedent.")
                .addSafeguards("Never speculate about intent.")
                .build();
    }

    @Test
    void rendersTheFullPacketFromEveryAnnotationFamily() {
        PromptPacket packet = PromptRenderer.create()
                .render(FORM, request().build(), "repo://forms/v3");

        assertThat(packet.getTargetType()).isEqualTo(FORM.getFullName());
        assertThat(packet.getDescriptorSetRef()).isEqualTo("repo://forms/v3");
        assertThat(packet.getResponseJsonSchema())
                .contains("$defs")
                .contains("DecoratedOpinion");
        assertThat(packet.hasPersona()).isFalse();
        assertThat(packet.getFewShotList()).isEmpty();

        String instructions = packet.getInstructions();
        assertThat(instructions)
                // task line + meta.v1 message description + llm.v1 message instruction
                .contains("You are filling the form \""
                        + "ai.pipestream.proto.prompt.testdata.v1.DecoratedOpinion\"")
                .contains("Metadata extracted from a court opinion.")
                .contains("Fill this form from the opinion text alone.")
                // message-level CEL rendered with its human message
                .contains("rule court.when.summary: a summary requires a court")
                // field rendering: json name, meta description, llm instruction, required
                .contains("\"court\"")
                .contains("The issuing court, as it appears in the caption.")
                .contains("Instruction: Name the court exactly as it appears in the caption.")
                .contains("must be present and non-empty")
                .contains("must be at most 200 characters")
                // repeated constraints
                .contains("must have at least 1 item(s)")
                .contains("must have at most 10 item(s)")
                .contains("items must be unique")
                // enum constraint
                .contains("must be a defined "
                        + "ai.pipestream.proto.prompt.testdata.v1.Posture value")
                // enum vocabulary: the model cannot fill what it cannot name
                .contains("defined values: POSTURE_UNSPECIFIED (means unknown),"
                        + " POSTURE_AFFIRMED, POSTURE_REVERSED")
                // numeric constraints
                .contains("must be at least 1600")
                .contains("must be at most 2100")
                // safeguards, message and field scope
                .contains("Do not use outside legal knowledge.")
                .contains("Do not abbreviate.")
                // volatility note
                .contains("time-relative")
                // quality dimensions
                .contains("completeness (weight 2.0)")
                // the output contract
                .contains("Respond with exactly one JSON object")
                // no persona in a persona-free render
                .doesNotContain("Persona:");
    }

    @Test
    void rendersPersonaContextAndCarriesFewShot() {
        Any example = Any.pack(DecoratedOpinion.newBuilder()
                .setCourt("Supreme Court of the United States").build());
        Persona persona = Persona.newBuilder(litigator()).addFewShot(example).build();

        PromptPacket packet = PromptRenderer.create()
                .render(FORM, request().setPersona(persona).build(), "repo://forms/v3");

        assertThat(packet.getPersona()).isEqualTo(persona);
        assertThat(packet.getFewShotList()).containsExactly(example);
        assertThat(packet.getInstructions())
                .contains("Persona: litigator (version 1.0.0)")
                .contains("You fill forms as a practicing litigator researching precedent.")
                .contains("Never speculate about intent.");
    }

    @Test
    void rejectsAnInvalidRequestWithViolationsAttached() {
        RenderPromptRequest invalid = RenderPromptRequest.newBuilder().build();

        assertThatThrownBy(() -> PromptRenderer.create().render(FORM, invalid, "repo://forms/v3"))
                .isInstanceOfSatisfying(PromptRenderException.class, e -> {
                    assertThat(e.requestViolations()).isPresent();
                    assertThat(e.getMessage()).contains("target_type");
                });
    }

    @Test
    void rejectsARequestWhosePersonaIsInvalid() {
        RenderPromptRequest invalid = request()
                .setPersona(Persona.newBuilder().setId("litigator"))
                .build();

        assertThatThrownBy(() -> PromptRenderer.create().render(FORM, invalid, "repo://forms/v3"))
                .isInstanceOf(PromptRenderException.class);
    }

    @Test
    void rejectsATargetThatDoesNotNameTheDescriptor() {
        RenderPromptRequest mismatched = RenderPromptRequest.newBuilder()
                .setTargetType("ai.pipestream.proto.prompt.v1.Persona")
                .build();

        assertThatThrownBy(() -> PromptRenderer.create().render(FORM, mismatched, "repo://forms/v3"))
                .isInstanceOfSatisfying(PromptRenderException.class,
                        e -> assertThat(e.getMessage()).contains("does not name"));
    }

    @Test
    void rejectsOverridesWithoutATypeRegistry() {
        RenderPromptRequest withOverrides = request()
                .setOverrides(Any.pack(litigator()))
                .build();

        assertThatThrownBy(() -> PromptRenderer.create()
                .render(FORM, withOverrides, "repo://forms/v3"))
                .isInstanceOfSatisfying(PromptRenderException.class,
                        e -> assertThat(e.getMessage()).contains("TypeRegistry"));
    }

    @Test
    void rendersOverridesThroughTheTypeRegistry() {
        TypeRegistry registry = TypeRegistry.newBuilder()
                .add(FORM)
                .build();
        RenderPromptRequest withOverrides = request()
                .setOverrides(Any.pack(DecoratedOpinion.newBuilder()
                        .setCourt("Supreme Court of the United States")
                        .build()))
                .build();

        PromptPacket packet = PromptRenderer.create()
                .render(FORM, withOverrides, "repo://forms/v3", registry);

        assertThat(packet.getInstructions())
                .contains("Document-specific context")
                .contains("Supreme Court of the United States");
    }

    @Test
    void rejectsOverridesTheRegistryCannotResolve() {
        TypeRegistry registry = TypeRegistry.newBuilder()
                .add(Persona.getDescriptor())
                .build();
        RenderPromptRequest withOverrides = request()
                .setOverrides(Any.pack(DecoratedOpinion.newBuilder()
                        .setCourt("Supreme Court of the United States")
                        .build()))
                .build();

        assertThatThrownBy(() -> PromptRenderer.create()
                .render(FORM, withOverrides, "repo://forms/v3", registry))
                .isInstanceOfSatisfying(PromptRenderException.class,
                        e -> assertThat(e.getMessage()).contains("cannot resolve"));
    }
}
