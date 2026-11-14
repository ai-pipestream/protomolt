package ai.protomolt.proto.emit.renderer;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.emit.v1.RendererCapability;
import ai.protomolt.proto.validate.model.FieldConstraints;
import ai.protomolt.proto.validate.model.MessageConstraints;
import ai.protomolt.proto.validate.spi.ValidationRuleSource;
import com.google.protobuf.*;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import java.io.ByteArrayInputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class RendererRegistryTest {
    static final RendererRegistry.Authorization ALLOW = (c, i, o) -> {};
    static final RendererProvider.Cancellation ACTIVE = () -> false;

    public static final class CountingRuleSource implements ValidationRuleSource {
        static final AtomicInteger INSTANCES = new AtomicInteger();
        static final Map<Descriptor, AtomicInteger> COMPILATIONS = new ConcurrentHashMap<>();

        public CountingRuleSource() {
            INSTANCES.incrementAndGet();
        }

        static void reset() {
            INSTANCES.set(0);
            COMPILATIONS.clear();
        }

        @Override public Optional<FieldConstraints> fieldConstraints(FieldDescriptor field) {
            return Optional.empty();
        }

        @Override public Optional<MessageConstraints> messageConstraints(Descriptor message) {
            COMPILATIONS.computeIfAbsent(message, ignored -> new AtomicInteger()).incrementAndGet();
            return Optional.empty();
        }
    }
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

    @Test void rendersEmptyExactBudgetAndMultiChunkPayloadsWithoutCoalescingCopy() throws Exception {
        var emptyProvider = new Echo() {
            @Override public void render(Message i, Message o, OutputStream out, Cancellation c) {}
        };
        ByteString emptyResult = registry(emptyProvider).render(
                "test.echo", Empty.getDefaultInstance(), Empty.getDefaultInstance(), ALLOW, ACTIVE);
        assertThat(emptyResult).isSameAs(ByteString.EMPTY);

        byte[] exact16 = new byte[16];
        for (int i = 0; i < exact16.length; i++) exact16[i] = (byte) (i + 1);
        var exactBudgetProvider = new Echo() {
            @Override public void render(Message i, Message o, OutputStream out, Cancellation c) throws IOException {
                out.write(exact16, 0, exact16.length);
            }
        };
        assertThat(registry(exactBudgetProvider).render(
                "test.echo", Empty.getDefaultInstance(), Empty.getDefaultInstance(), ALLOW, ACTIVE).toByteArray())
                .containsExactly(exact16);

        int totalBytes = 5_000;
        byte[] expected = new byte[totalBytes];
        for (int i = 0; i < totalBytes; i++) expected[i] = (byte) ((i * 31 + 7) & 0xff);
        var multiChunkProvider = new Echo() {
            @Override public RendererCapability capability() {
                return super.capability().toBuilder().setMaxOutputBytes(8_192).build();
            }
            @Override public void render(Message i, Message o, OutputStream out, Cancellation c) throws IOException {
                int cursor = 0;
                // Exercise single-byte writes, offset sub-slices, and multi-buffer writes across ByteString.Output boundaries.
                for (; cursor < 19; cursor++) out.write(expected[cursor] & 0xff);
                byte[] padded = new byte[300];
                System.arraycopy(expected, cursor, padded, 17, 257);
                out.write(padded, 17, 257);
                cursor += 257;
                while (cursor < totalBytes) {
                    int chunk = Math.min(373, totalBytes - cursor);
                    out.write(expected, cursor, chunk);
                    cursor += chunk;
                }
            }
        };
        ByteString rendered = registry(multiChunkProvider).render(
                "test.echo", Empty.getDefaultInstance(), Empty.getDefaultInstance(), ALLOW, ACTIVE);
        assertThat(rendered.size()).isEqualTo(totalBytes);
        assertThat(rendered.toByteArray()).containsExactly(expected);
        // ByteString.Output retains flushed buffers as rope leaves instead of coalescing via toByteArray() + copyFrom().
        assertThat(rendered.asReadOnlyByteBufferList().size()).isGreaterThan(1);
    }

    @Test void reusesCompiledOptionsValidatorAcrossChecksAndRendersAndRejectsInvalidOptions() throws Exception {
        Descriptor constrainedOptions = RendererCapability.getDescriptor();
        String optionsFingerprint = DescriptorFingerprints.fingerprintOf(constrainedOptions);
        RendererProvider first = validatedOptionsProvider("test.validated.first", constrainedOptions, optionsFingerprint);
        RendererProvider second = validatedOptionsProvider("test.validated.second", constrainedOptions, optionsFingerprint);

        CountingRuleSource.reset();
        ClassLoader previousTccl = Thread.currentThread().getContextClassLoader();
        RendererRegistry registry;
        try {
            Thread.currentThread().setContextClassLoader(ruleSourceLoader(previousTccl));
            registry = new RendererRegistry(List.of(first, second), Set.of("test.validated.first", "test.validated.second"));
        } finally {
            Thread.currentThread().setContextClassLoader(previousTccl);
        }

        // Both providers share constrainedOptions, so only one ProtoValidator (and one CountingRuleSource) is built.
        assertThat(CountingRuleSource.INSTANCES.get()).isEqualTo(1);

        RendererCapability validOptions = new Echo().capability();
        for (int i = 0; i < 4; i++) {
            assertThat(registry.check("test.validated.first", StringValue.of("ok"), validOptions, ALLOW, ACTIVE))
                    .isEqualTo(first.capability());
            assertThat(registry.render("test.validated.first", StringValue.of("ok"), validOptions, ALLOW, ACTIVE))
                    .isEqualTo(StringValue.of("ok").toByteString());
            assertThat(registry.render("test.validated.second", StringValue.of("ok2"), validOptions, ALLOW, ACTIVE))
                    .isEqualTo(StringValue.of("ok2").toByteString());
        }

        RendererCapability emptyInvalidOptions = RendererCapability.getDefaultInstance();
        RendererCapability zeroLimitOptions = validOptions.toBuilder().setMaxOutputBytes(0).build();
        for (RendererCapability invalid : List.of(emptyInvalidOptions, zeroLimitOptions)) {
            assertThatThrownBy(() -> registry.check("test.validated.first", Empty.getDefaultInstance(), invalid, ALLOW, ACTIVE))
                    .isInstanceOfSatisfying(RenderFailure.class, failure -> {
                        assertThat(failure.kind()).isEqualTo(RenderFailure.Kind.INVALID);
                        assertThat(failure).hasMessage("Invalid renderer options");
                    });
            assertThatThrownBy(() -> registry.render("test.validated.second", Empty.getDefaultInstance(), invalid, ALLOW, ACTIVE))
                    .isInstanceOfSatisfying(RenderFailure.class, failure -> {
                        assertThat(failure.kind()).isEqualTo(RenderFailure.Kind.INVALID);
                        assertThat(failure).hasMessage("Invalid renderer options");
                    });
        }

        assertThat(CountingRuleSource.INSTANCES.get()).isEqualTo(1);
        assertThat(CountingRuleSource.COMPILATIONS.get(constrainedOptions)).isNotNull();
        assertThat(CountingRuleSource.COMPILATIONS.get(constrainedOptions).get()).isEqualTo(1);
    }

    private static RendererProvider validatedOptionsProvider(String id, Descriptor optionsType, String optionsFingerprint) {
        return new RendererProvider() {
            @Override public RendererCapability capability() {
                return RendererCapability.newBuilder()
                        .setProviderId(id)
                        .setProviderVersion("1")
                        .setSourceProfile("protobuf-bytes")
                        .setMediaType("application/x-protobuf")
                        .setExtension("pb")
                        .setOptionsType(optionsType.getFullName())
                        .setOptionsFingerprint(optionsFingerprint)
                        .setLossDescription("No conversion; original serialized message")
                        .setMaxInputBytes(4_096)
                        .setMaxOutputBytes(128)
                        .build();
            }
            @Override public Descriptor optionsDescriptor() { return optionsType; }
            @Override public void check(Message input, Message options) {}
            @Override public void render(Message input, Message options, OutputStream output, Cancellation cancel) throws IOException {
                input.writeTo(output);
            }
        };
    }

    private static ClassLoader ruleSourceLoader(ClassLoader parent) throws IOException {
        String resourceName = "META-INF/services/" + ValidationRuleSource.class.getName();
        byte[] content = (CountingRuleSource.class.getName() + "\n").getBytes(StandardCharsets.UTF_8);
        URL memoryUrl = URL.of(URI.create("memory:///counting-rule-source"), new URLStreamHandler() {
            @Override protected URLConnection openConnection(URL u) {
                return new URLConnection(u) {
                    @Override public void connect() {}
                    @Override public InputStream getInputStream() {
                        return new ByteArrayInputStream(content);
                    }
                };
            }
        });
        return new ClassLoader(parent) {
            @Override public Enumeration<URL> getResources(String name) throws IOException {
                if (!resourceName.equals(name)) return super.getResources(name);
                List<URL> urls = new ArrayList<>(Collections.list(super.getResources(name)));
                urls.add(memoryUrl);
                return Collections.enumeration(urls);
            }
        };
    }
}
