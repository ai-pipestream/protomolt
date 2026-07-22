package ai.pipestream.proto.gather;

import ai.pipestream.proto.descriptors.DescriptorLoader;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoCompilationException;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A {@link DescriptorLoader} backed by a {@link ProtoGatherer}: sources are gathered and
 * compiled lazily on first use, and the compiled result is cached until {@link #refresh()}.
 *
 * <p>This is the bridge between the gathering layer and the descriptor SPI — wire it into a
 * {@code DescriptorRegistry} via {@code addLoader} and gathered types resolve on demand.
 * {@link #loadDescriptors()} returns descriptors for the gathered files only, not the
 * {@code google/protobuf/*} imports the compiler supplies. Instances are thread-safe.</p>
 */
public final class GatheringDescriptorLoader implements DescriptorLoader, AutoCloseable {

    private final ProtoGatherer gatherer;
    private final ProtoSourceCompiler compiler;

    private volatile Compiled cached;

    private record Compiled(ProtoSourceSet sources, CompiledProtos compiled) {
    }

    /** Wraps the gatherer with a fresh {@link ProtoSourceCompiler}. */
    public GatheringDescriptorLoader(ProtoGatherer gatherer) {
        this(gatherer, new ProtoSourceCompiler());
    }

    public GatheringDescriptorLoader(ProtoGatherer gatherer, ProtoSourceCompiler compiler) {
        this.gatherer = Objects.requireNonNull(gatherer, "gatherer");
        this.compiler = Objects.requireNonNull(compiler, "compiler");
    }

    private Compiled compiled() throws DescriptorLoadException {
        Compiled current = cached;
        if (current != null) {
            return current;
        }
        synchronized (this) {
            if (cached == null) {
                try {
                    ProtoSourceSet sources = gatherer.gather();
                    cached = new Compiled(sources, compiler.compile(sources));
                } catch (GatherException e) {
                    throw new DescriptorLoadException(
                            "Failed to gather protos from " + gatherer.origin(), e);
                } catch (ProtoCompilationException e) {
                    throw new DescriptorLoadException(
                            "Failed to compile protos gathered from " + gatherer.origin(), e);
                }
            }
            return cached;
        }
    }

    /** Descriptors for the gathered files, in gathering order (no compiler-supplied extras). */
    @Override
    public List<FileDescriptor> loadDescriptors() throws DescriptorLoadException {
        Compiled state = compiled();
        List<FileDescriptor> descriptors = new ArrayList<>(state.sources().size());
        for (String path : state.sources().paths()) {
            state.compiled().descriptorFor(path).ifPresent(descriptors::add);
        }
        return descriptors;
    }

    /** Looks up a compiled file by import path, e.g. {@code common/v1/a.proto}. */
    @Override
    public FileDescriptor loadDescriptor(String fileName) throws DescriptorLoadException {
        return compiled().compiled().descriptorFor(fileName).orElse(null);
    }

    /**
     * Finds the gathered file defining the given message type: first by fully qualified name,
     * then by simple name, nested types included.
     */
    @Override
    public FileDescriptor loadDescriptorForType(String fullTypeName) throws DescriptorLoadException {
        List<FileDescriptor> descriptors = loadDescriptors();
        for (FileDescriptor descriptor : descriptors) {
            if (containsMessage(descriptor.getMessageTypes(), fullTypeName, true)) {
                return descriptor;
            }
        }
        for (FileDescriptor descriptor : descriptors) {
            if (containsMessage(descriptor.getMessageTypes(), fullTypeName, false)) {
                return descriptor;
            }
        }
        return null;
    }

    private static boolean containsMessage(List<Descriptor> messages, String name, boolean fullyQualified) {
        for (Descriptor message : messages) {
            String candidate = fullyQualified ? message.getFullName() : message.getName();
            if (candidate.equals(name)
                    || containsMessage(message.getNestedTypes(), name, fullyQualified)) {
                return true;
            }
        }
        return false;
    }

    /** Drops the cached compilation; the next load gathers and compiles again. */
    public synchronized void refresh() {
        cached = null;
    }

    @Override
    public void close() {
        refresh();
    }

    @Override
    public boolean isAvailable() {
        return gatherer.isAvailable();
    }

    @Override
    public String getLoaderType() {
        return "Proto Gatherer (" + gatherer.origin() + ")";
    }
}
