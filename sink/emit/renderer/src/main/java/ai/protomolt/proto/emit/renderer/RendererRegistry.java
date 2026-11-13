package ai.protomolt.proto.emit.renderer;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.emit.v1.RendererCapability;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;

/** Explicit enablement over ServiceLoader; no implicit default provider or destination. */
public final class RendererRegistry {
    private static final ProtoValidator CAPABILITY_VALIDATOR =
            ProtoValidator.forMessageType(RendererCapability.getDescriptor());

    private record Discovered(RendererProvider provider, RendererCapability capability, Descriptor optionsDescriptor) {}
    private record Entry(
            RendererProvider provider,
            RendererCapability capability,
            Descriptor optionsDescriptor,
            ProtoValidator optionsValidator) {}

    private final Map<String, Entry> enabled;

    /** Discovers installed {@link RendererProvider}s from {@code loader} and enables {@code enabled}. */
    public static RendererRegistry load(ClassLoader loader, Set<String> enabled) {
        return new RendererRegistry(ServiceLoader.load(RendererProvider.class, loader), enabled);
    }

    /** Also accepts host-configured remote adapters. Duplicate ids fail even when disabled. */
    public RendererRegistry(Iterable<RendererProvider> providers, Set<String> enabledIds) {
        Map<String, Discovered> discovered = new LinkedHashMap<>();
        for (RendererProvider provider : providers) {
            RendererCapability c = Objects.requireNonNull(provider.capability());
            Descriptor optionsDescriptor = Objects.requireNonNull(provider.optionsDescriptor());
            if (!CAPABILITY_VALIDATOR.validate(c).valid()
                    || !c.getUnknownFields().asMap().isEmpty()
                    || c.getMaxInputBytes() <= 0 || c.getMaxOutputBytes() <= 0
                    || !c.getOptionsType().equals(optionsDescriptor.getFullName())
                    || !c.getOptionsFingerprint().equals(DescriptorFingerprints.fingerprintOf(optionsDescriptor)))
                throw new IllegalArgumentException("Invalid renderer capability: " + c.getProviderId());
            if (discovered.putIfAbsent(c.getProviderId(), new Discovered(provider, c, optionsDescriptor)) != null)
                throw new IllegalArgumentException("Duplicate renderer id: " + c.getProviderId());
        }
        Map<Descriptor, ProtoValidator> validatorsByOptions = new HashMap<>();
        Map<String, Entry> selected = new LinkedHashMap<>();
        for (String id : enabledIds) {
            Discovered item = discovered.get(id);
            if (item == null) throw new IllegalArgumentException("Enabled renderer is not installed: " + id);
            ProtoValidator optionsValidator = validatorsByOptions.computeIfAbsent(
                    item.optionsDescriptor(), ProtoValidator::forMessageType);
            selected.put(id, new Entry(item.provider(), item.capability(), item.optionsDescriptor(), optionsValidator));
        }
        this.enabled = Map.copyOf(selected);
    }

    /** Returns the capabilities of enabled providers ordered by provider ID. */
    public List<RendererCapability> capabilities() {
        return enabled.values().stream().map(Entry::capability)
                .sorted(Comparator.comparing(RendererCapability::getProviderId)).toList();
    }

    /** Must enforce the current caller's source access and disclosure policy, including options. */
    @FunctionalInterface
    public interface Authorization {
        /** Authorizes {@code input} and {@code options} for {@code capability}, or throws {@link RenderFailure}. */
        void check(RendererCapability capability, Message input, Message options) throws RenderFailure;
    }

    /** Check actual input/options compatibility and current policy without rendering or saving. */
    public RendererCapability check(String id, Message input, Message options, Authorization authorization,
                                    RendererProvider.Cancellation cancellation) throws RenderFailure {
        Objects.requireNonNull(input); Objects.requireNonNull(options);
        Objects.requireNonNull(authorization); Objects.requireNonNull(cancellation).check();
        Entry entry = enabled.get(id);
        if (entry == null) throw new RenderFailure(RenderFailure.Kind.UNAVAILABLE, "Renderer is not enabled: " + id);
        var c = entry.capability();
        authorization.check(c, input, options);
        if ((long) input.getSerializedSize() + options.getSerializedSize() > c.getMaxInputBytes())
            throw new RenderFailure(RenderFailure.Kind.LIMIT, "Renderer input limit exceeded");
        Descriptor candidateOptions = options.getDescriptorForType();
        if (candidateOptions != entry.optionsDescriptor()
                && (!candidateOptions.getFullName().equals(c.getOptionsType())
                        || !DescriptorFingerprints.fingerprintOf(candidateOptions).equals(c.getOptionsFingerprint())))
            throw new RenderFailure(RenderFailure.Kind.INVALID, "Renderer options descriptor differs from capability");
        requireKnownOptions(options);
        if (!entry.optionsValidator().validate(options).valid())
            throw new RenderFailure(RenderFailure.Kind.INVALID, "Invalid renderer options");
        try { entry.provider().check(input, options); }
        catch (RenderFailure e) { throw e; }
        catch (RuntimeException e) { throw new RenderFailure(RenderFailure.Kind.FAILED, "Renderer capability check failed", e); }
        cancellation.check();
        return c;
    }

    /** No bytes escape on failed rendering. Returning an artifact is not repository success. */
    public ByteString render(String id, Message input, Message options, Authorization authorization,
                             RendererProvider.Cancellation cancellation) throws IOException {
        var c = check(id, input, options, authorization, cancellation);
        Entry entry = enabled.get(id);
        ByteString.Output bytes = ByteString.newOutput();
        OutputStream bounded = new OutputStream() {
            private int size;
            private void reserve(int count) throws RenderFailure {
                cancellation.check();
                if (count > c.getMaxOutputBytes() - size)
                    throw new RenderFailure(RenderFailure.Kind.LIMIT, "Renderer output limit exceeded");
                size += count;
            }
            @Override public void write(int b) throws IOException { reserve(1); bytes.write(b); }
            @Override public void write(byte[] b, int offset, int length) throws IOException {
                Objects.checkFromIndexSize(offset, length, b.length);
                reserve(length); bytes.write(b, offset, length);
            }
        };
        try {
            entry.provider().render(input, options, bounded, cancellation);
            cancellation.check();
            return bytes.toByteString();
        } catch (RenderFailure e) { throw e; }
        catch (IOException | RuntimeException e) {
            throw new RenderFailure(RenderFailure.Kind.FAILED, "Renderer failed: " + id, e);
        }
    }

    private static void requireKnownOptions(Message root) throws RenderFailure {
        var queue = new ArrayDeque<Message>(); queue.add(root);
        while (!queue.isEmpty()) {
            Message m = queue.removeFirst();
            if (!m.getUnknownFields().asMap().isEmpty())
                throw new RenderFailure(RenderFailure.Kind.INVALID, "Unknown renderer option fields");
            for (var field : m.getAllFields().entrySet()) {
                if (field.getKey().getJavaType() != FieldDescriptor.JavaType.MESSAGE) continue;
                if (field.getKey().isRepeated())
                    for (Object item : (List<?>) field.getValue()) queue.add((Message) item);
                else queue.add((Message) field.getValue());
            }
        }
    }
}
