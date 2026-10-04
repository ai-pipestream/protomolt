package ai.protomolt.proto.descriptors;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Links a self-contained descriptor artifact without registry or classpath imports.
 * Message full names must be unique across the artifact for unambiguous Any lookup.
 * This proves import closure and protobuf linkage, not interpretation of custom
 * options, validation-rule support, schema identity, or archival persistence.
 */
public final class ClosedDescriptorSet {
    private ClosedDescriptorSet() {}

    /** Resource policy chosen by the caller; byte limits apply before parsing. */
    public record Limits(int maxBytes, int maxFiles, int maxDependencies, int maxImportDepth) {
        public Limits {
            if (maxBytes < 1 || maxFiles < 1 || maxDependencies < 1
                    || maxImportDepth < 1 || maxImportDepth > 100) {
                throw new IllegalArgumentException("positive limits required; import depth must be at most 100");
            }
        }
    }

    /** Returns immutable dependency-first descriptors, using only the supplied bytes. */
    public static List<FileDescriptor> load(ByteString bytes, Limits limits) {
        Objects.requireNonNull(bytes, "bytes");
        Objects.requireNonNull(limits, "limits");
        if (bytes.isEmpty() || bytes.size() > limits.maxBytes()) {
            throw new IllegalArgumentException("descriptor artifact is empty or exceeds byte limit");
        }
        final FileDescriptorSet set;
        try {
            set = FileDescriptorSet.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("invalid descriptor artifact", e);
        }
        if (set.getFileCount() == 0 || set.getFileCount() > limits.maxFiles()) {
            throw new IllegalArgumentException("descriptor file count is empty or exceeds limit");
        }
        Map<String, FileDescriptorProto> files = new LinkedHashMap<>();
        long edges = 0;
        for (FileDescriptorProto file : set.getFileList()) {
            if (file.getName().isBlank() || files.putIfAbsent(file.getName(), file) != null) {
                throw new IllegalArgumentException("missing or duplicate descriptor filename: " + file.getName());
            }
            edges += file.getDependencyCount();
            if (edges > limits.maxDependencies()) {
                throw new IllegalArgumentException("descriptor dependencies exceed limit");
            }
        }
        Map<String, List<String>> dependents = new HashMap<>();
        Map<String, Integer> remaining = new HashMap<>();
        ArrayDeque<String> ready = new ArrayDeque<>();
        for (FileDescriptorProto file : files.values()) {
            HashSet<String> distinct = new HashSet<>();
            for (String dependency : file.getDependencyList()) {
                if (!files.containsKey(dependency)) {
                    throw new IllegalArgumentException("missing import " + dependency + " in " + file.getName());
                }
                if (!distinct.add(dependency)) {
                    throw new IllegalArgumentException("duplicate import " + dependency + " in " + file.getName());
                }
                dependents.computeIfAbsent(dependency, ignored -> new ArrayList<>()).add(file.getName());
            }
            remaining.put(file.getName(), distinct.size());
            if (distinct.isEmpty()) ready.addLast(file.getName());
        }
        Map<String, FileDescriptor> linked = new LinkedHashMap<>();
        Map<String, Integer> depths = new HashMap<>();
        HashSet<String> messageNames = new HashSet<>();
        while (!ready.isEmpty()) {
            String name = ready.removeFirst();
            FileDescriptorProto file = files.get(name);
            FileDescriptor[] imports = new FileDescriptor[file.getDependencyCount()];
            int depth = 1;
            for (int i = 0; i < imports.length; i++) {
                String dependency = file.getDependency(i);
                imports[i] = linked.get(dependency);
                depth = Math.max(depth, depths.get(dependency) + 1);
            }
            if (depth > limits.maxImportDepth()) {
                throw new IllegalArgumentException("descriptor import depth exceeds limit: " + name);
            }
            try {
                FileDescriptor descriptor = FileDescriptor.buildFrom(file, imports);
                requireUniqueMessageNames(descriptor, messageNames);
                linked.put(name, descriptor);
            } catch (DescriptorValidationException e) {
                throw new IllegalArgumentException("invalid descriptor: " + name, e);
            }
            depths.put(name, depth);
            for (String dependent : dependents.getOrDefault(name, List.of())) {
                int count = remaining.compute(dependent, (ignored, previous) -> previous - 1);
                if (count == 0) ready.addLast(dependent);
            }
        }
        if (linked.size() != files.size()) {
            throw new IllegalArgumentException("descriptor import cycle");
        }
        return List.copyOf(linked.values());
    }

    private static void requireUniqueMessageNames(FileDescriptor file, HashSet<String> names) {
        ArrayDeque<Descriptor> pending = new ArrayDeque<>(file.getMessageTypes());
        while (!pending.isEmpty()) {
            Descriptor message = pending.removeFirst();
            if (!names.add(message.getFullName())) {
                throw new IllegalArgumentException("duplicate message name: " + message.getFullName());
            }
            pending.addAll(message.getNestedTypes());
        }
    }
}
