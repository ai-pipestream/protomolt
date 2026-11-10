package ai.protomolt.proto.emit.renderer;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.emit.v1.RendererCapability;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;

/** Explicit enablement over ServiceLoader; no implicit default provider or destination. */
public final class RendererRegistry {
    private record Entry(RendererProvider provider, RendererCapability capability) {}
    private final Map<String, Entry> enabled;

    public static RendererRegistry load(ClassLoader loader, Set<String> enabled) {
        return new RendererRegistry(ServiceLoader.load(RendererProvider.class, loader), enabled);
    }

    /** Also accepts host-configured remote adapters. Duplicate ids fail even when disabled. */
    public RendererRegistry(Iterable<RendererProvider> providers, Set<String> enabledIds) {
        Map<String, Entry> discovered = new LinkedHashMap<>();
        for (RendererProvider provider : providers) {
            RendererCapability c = Objects.requireNonNull(provider.capability());
            if (!ProtoValidator.forMessageType(RendererCapability.getDescriptor()).validate(c).valid()
                    || !c.getUnknownFields().asMap().isEmpty()
                    || c.getMaxInputBytes() <= 0 || c.getMaxOutputBytes() <= 0
                    || !c.getOptionsType().equals(provider.optionsDescriptor().getFullName())
                    || !c.getOptionsFingerprint().equals(DescriptorFingerprints.fingerprintOf(provider.optionsDescriptor())))
                throw new IllegalArgumentException("Invalid renderer capability: " + c.getProviderId());
            if (discovered.putIfAbsent(c.getProviderId(), new Entry(provider, c)) != null)
                throw new IllegalArgumentException("Duplicate renderer id: " + c.getProviderId());
        }
        Map<String, Entry> selected = new LinkedHashMap<>();
        for (String id : enabledIds) {
            Entry entry = discovered.get(id);
            if (entry == null) throw new IllegalArgumentException("Enabled renderer is not installed: " + id);
            selected.put(id, entry);
        }
        this.enabled = Map.copyOf(selected);
    }

    public List<RendererCapability> capabilities() {
        return enabled.values().stream().map(Entry::capability)
                .sorted(java.util.Comparator.comparing(RendererCapability::getProviderId)).toList();
    }

    /** Must enforce the current caller's source access and disclosure policy, including options. */
    @FunctionalInterface
    public interface Authorization {
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
        if (!options.getDescriptorForType().getFullName().equals(c.getOptionsType())
                || !DescriptorFingerprints.fingerprintOf(options.getDescriptorForType()).equals(c.getOptionsFingerprint()))
            throw new RenderFailure(RenderFailure.Kind.INVALID, "Renderer options descriptor differs from capability");
        requireKnownOptions(options);
        if (!ProtoValidator.forMessageType(options.getDescriptorForType()).validate(options).valid())
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
        var bytes = new ByteArrayOutputStream();
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
            return ByteString.copyFrom(bytes.toByteArray());
        } catch (RenderFailure e) { throw e; }
        catch (IOException | RuntimeException e) {
            throw new RenderFailure(RenderFailure.Kind.FAILED, "Renderer failed: " + id, e);
        }
    }

    private static void requireKnownOptions(Message root) throws RenderFailure {
        var queue = new java.util.ArrayDeque<Message>(); queue.add(root);
        while (!queue.isEmpty()) {
            Message m = queue.removeFirst();
            if (!m.getUnknownFields().asMap().isEmpty())
                throw new RenderFailure(RenderFailure.Kind.INVALID, "Unknown renderer option fields");
            for (var field : m.getAllFields().entrySet()) {
                if (field.getKey().getJavaType() != com.google.protobuf.Descriptors.FieldDescriptor.JavaType.MESSAGE) continue;
                if (field.getKey().isRepeated())
                    for (Object item : (List<?>) field.getValue()) queue.add((Message) item);
                else queue.add((Message) field.getValue());
            }
        }
    }
}
