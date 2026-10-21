package ai.protomolt.proto.workflow;

import ai.protomolt.proto.grpc.workflow.v1.Workflow;
import ai.protomolt.proto.grpc.workflow.v1.WorkflowStep;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.util.JsonFormat.TypeRegistry;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Type resolution against the descriptors a recorded run was checked against. Replay
 * never consults a live registry: a name that is not in the replay schema is reported
 * as such, never resolved somewhere else.
 */
final class ReplaySchema {

    private ReplaySchema() {
    }

    /** The message with the given fully qualified name, nested types included; null when absent. */
    static Descriptor findMessage(List<FileDescriptor> schema, String fullName) {
        for (FileDescriptor file : schema) {
            Descriptor found = findIn(file.getMessageTypes(), fullName);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private static Descriptor findIn(List<Descriptor> messages, String fullName) {
        for (Descriptor message : messages) {
            if (message.getFullName().equals(fullName)) {
                return message;
            }
            Descriptor nested = findIn(message.getNestedTypes(), fullName);
            if (nested != null) {
                return nested;
            }
        }
        return null;
    }

    /** The workflow's declared output type; refused when the schema lacks it. */
    static Descriptor outputType(List<FileDescriptor> schema, Workflow workflow) {
        Descriptor type = findMessage(schema, workflow.getOutput().getType());
        if (type == null) {
            throw new IllegalArgumentException("output type " + workflow.getOutput().getType()
                    + " not in the replay schema");
        }
        return type;
    }

    /** The collect type of a fanned-out step; refused when the schema lacks it. */
    static Descriptor collectType(List<FileDescriptor> schema, WorkflowStep step) {
        Descriptor type = findMessage(schema, step.getFanOut().getCollectType());
        if (type == null) {
            throw new IllegalArgumentException("collect type "
                    + step.getFanOut().getCollectType() + " not in the replay schema");
        }
        return type;
    }

    /** Every message type visible from the replay schema, for grounding resolution. */
    static TypeRegistry typeRegistry(List<FileDescriptor> schema) {
        TypeRegistry.Builder builder = TypeRegistry.newBuilder();
        Set<String> seen = new HashSet<>();
        for (FileDescriptor file : schema) {
            collectFileTypes(file, builder, seen);
        }
        return builder.build();
    }

    /**
     * A memoized supplier of the replay schema's {@link TypeRegistry}: the registry is
     * built on the first {@code get()} and the same instance answers every later call,
     * so one replay traverses the descriptor graph at most once no matter how many
     * grounded structured steps it carries. Not thread-safe; replay is sequential.
     */
    static Supplier<TypeRegistry> lazyTypeRegistry(List<FileDescriptor> schema) {
        return new Supplier<>() {
            private TypeRegistry registry;

            @Override public TypeRegistry get() {
                if (registry == null) {
                    registry = typeRegistry(schema);
                }
                return registry;
            }
        };
    }

    private static void collectFileTypes(FileDescriptor file, TypeRegistry.Builder out,
                                         Set<String> seen) {
        if (!seen.add(file.getFullName())) {
            return;
        }
        for (Descriptor message : file.getMessageTypes()) {
            out.add(message);
        }
        for (FileDescriptor dependency : file.getDependencies()) {
            collectFileTypes(dependency, out, seen);
        }
    }
}
