package ai.protomolt.proto.emit.renderer;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.emit.v1.RendererCapability;
import com.google.protobuf.*;
import com.google.protobuf.Descriptors.Descriptor;
import java.io.OutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class RendererRegistryTest {
    static final RendererRegistry.Authorization ALLOW = (c, i, o) -> {};
    static final RendererProvider.Cancellation ACTIVE = () -> false;
    public static class Echo implements RendererProvider {
        public Echo() {}
        @Override public RendererCapability capability() {
            return RendererCapability.newBuilder().setProviderId("test.echo").setProviderVersion("1")
                    .setSourceProfile("protobuf-bytes").setMediaType("application/x-protobuf")
                    .setExtension("pb").setOptionsType(Empty.getDescriptor().getFullName())
                    .setOptionsFingerprint(DescriptorFingerprints.fingerprintOf(Empty.getDescriptor()))
                    .setLossDescription("No conversion; original serialized message")
                    .setMaxInputBytes(32).setMaxOutputBytes(16).build();
        }
        @Override public Descriptor optionsDescriptor() { return Empty.getDescriptor(); }
        @Override public void check(Message input, Message options) {}
        @Override public void render(Message input, Message options, OutputStream output, Cancellation cancel) throws IOException {
            input.writeTo(output);
        }
    }
    private RendererRegistry registry(RendererProvider p) { return new RendererRegistry(List.of(p), Set.of("test.echo")); }
    @Test void installedProvidersRequireExplicitEnablement() throws Exception {
        var disabled = RendererRegistry.load(getClass().getClassLoader(), Set.of());
        assertThat(disabled.capabilities()).isEmpty();
        var enabled = RendererRegistry.load(getClass().getClassLoader(), Set.of("test.echo"));
        var message = StringValue.of("hello");
        assertThat(enabled.render("test.echo", message, Empty.getDefaultInstance(), ALLOW, ACTIVE)).isEqualTo(message.toByteString());
    }
    @Test void absentAndDuplicateProvidersAreErrors() {
        assertThatThrownBy(() -> new RendererRegistry(List.of(), Set.of("missing"))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RendererRegistry(List.of(new Echo(), new Echo()), Set.of())).hasMessageContaining("Duplicate");
    }
    @Test void disabledProviderCannotRender() {
        var registry = new RendererRegistry(List.of(new Echo()), Set.of());
        assertThatThrownBy(() -> registry.render("test.echo", Empty.getDefaultInstance(), Empty.getDefaultInstance(), ALLOW, ACTIVE))
                .isInstanceOfSatisfying(RenderFailure.class, e -> assertThat(e.kind()).isEqualTo(RenderFailure.Kind.UNAVAILABLE));
    }
    @Test void authorizationRunsBeforeProvider() {
        var invoked = new AtomicBoolean();
        var registry = registry(new Echo() { @Override public void check(Message i, Message o) { invoked.set(true); } });
        assertThatThrownBy(() -> registry.render("test.echo", Empty.getDefaultInstance(), Empty.getDefaultInstance(),
                (c,i,o) -> { throw new RenderFailure(RenderFailure.Kind.DENIED, "denied"); }, ACTIVE))
                .isInstanceOf(RenderFailure.class);
        assertThat(invoked).isFalse();
    }
    @Test void checksInputBudgetBeforeProvider() {
        assertThatThrownBy(() -> registry(new Echo()).render("test.echo", StringValue.of("x".repeat(40)), Empty.getDefaultInstance(), ALLOW, ACTIVE))
                .isInstanceOfSatisfying(RenderFailure.class, e -> assertThat(e.kind()).isEqualTo(RenderFailure.Kind.LIMIT));
    }
    @Test void checksOutputBudgetDuringWriting() {
        var writes = new java.util.concurrent.atomic.AtomicInteger();
        var provider = new Echo() { @Override public void render(Message i, Message o, OutputStream output, Cancellation c) throws IOException {
            for (int x=0; x<1000; x++) { output.write(x); writes.incrementAndGet(); }
        }};
        assertThatThrownBy(() -> registry(provider).render("test.echo", Empty.getDefaultInstance(), Empty.getDefaultInstance(), ALLOW, ACTIVE))
                .isInstanceOfSatisfying(RenderFailure.class, e -> assertThat(e.kind()).isEqualTo(RenderFailure.Kind.LIMIT));
        assertThat(writes.get()).isEqualTo(16);
    }
    @Test void cancellationCannotReturnPartialSuccess() {
        var cancelled = new AtomicBoolean();
        var provider = new Echo() { @Override public void render(Message i, Message o, OutputStream output, Cancellation c) throws IOException {
            output.write(1); cancelled.set(true);
        }};
        assertThatThrownBy(() -> registry(provider).render("test.echo", Empty.getDefaultInstance(), Empty.getDefaultInstance(), ALLOW, cancelled::get))
                .isInstanceOfSatisfying(RenderFailure.class, e -> assertThat(e.kind()).isEqualTo(RenderFailure.Kind.CANCELLED));
    }
    @Test void sameNameDifferentOptionsSchemaIsRefused() throws Exception {
        var original = Empty.getDescriptor().getFile().toProto();
        var altered = original.toBuilder().setMessageType(0, original.getMessageType(0).toBuilder()
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder().setName("forged").setNumber(1)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))).build();
        var type = Descriptors.FileDescriptor.buildFrom(altered, new Descriptors.FileDescriptor[0]).findMessageTypeByName("Empty");
        assertThatThrownBy(() -> registry(new Echo()).render("test.echo", Empty.getDefaultInstance(), DynamicMessage.getDefaultInstance(type), ALLOW, ACTIVE))
                .hasMessageContaining("descriptor differs");
    }
    @Test void unknownOptionsAreRefusedAndInputUnknownBytesSurvive() throws Exception {
        var unknown = UnknownFieldSet.newBuilder().addField(17, UnknownFieldSet.Field.newBuilder().addVarint(42).build()).build();
        var message = Empty.newBuilder().setUnknownFields(unknown).build();
        assertThat(registry(new Echo()).render("test.echo", message, Empty.getDefaultInstance(), ALLOW, ACTIVE)).isEqualTo(message.toByteString());
        assertThatThrownBy(() -> registry(new Echo()).render("test.echo", Empty.getDefaultInstance(), message, ALLOW, ACTIVE)).hasMessageContaining("Unknown renderer option");
    }
    @Test void preservesFailureCause() {
        var cause = new IOException("disk full");
        var provider = new Echo() { @Override public void render(Message i, Message o, OutputStream out, Cancellation c) throws IOException { throw cause; } };
        assertThatThrownBy(() -> registry(provider).render("test.echo", Empty.getDefaultInstance(), Empty.getDefaultInstance(), ALLOW, ACTIVE)).hasCause(cause);
    }
}
