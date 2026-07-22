package ai.pipestream.proto.compat;

import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;

import java.util.List;
import java.util.function.Consumer;

/**
 * Test fixtures: compiles {@code .proto} text through the real {@link ProtoSourceCompiler} and
 * patches in the descriptor details Wire's encoder drops (reserved ranges/names, explicit
 * {@code json_name}), so every rule can be exercised from readable source pairs.
 */
final class TestSchemas {

    private static final ProtoSourceCompiler COMPILER = new ProtoSourceCompiler();

    private TestSchemas() {
    }

    /** Compiles a single file named {@code schema.proto}. */
    static FileDescriptorSet compile(String source) throws Exception {
        return COMPILER.compile(sourceSet(source)).descriptorSet();
    }

    /** Compiles alternating {@code path, content} pairs. */
    static FileDescriptorSet compileFiles(String... pathContentPairs) throws Exception {
        ProtoSourceSet.Builder builder = ProtoSourceSet.builder();
        for (int i = 0; i < pathContentPairs.length; i += 2) {
            builder.add(pathContentPairs[i], pathContentPairs[i + 1], "test");
        }
        return COMPILER.compile(builder.build()).descriptorSet();
    }

    /** A single-file source set named {@code schema.proto}. */
    static ProtoSourceSet sourceSet(String source) {
        return ProtoSourceSet.builder().add("schema.proto", source, "test").build();
    }

    /** Compiles a single file and returns its linked {@link FileDescriptor}. */
    static FileDescriptor link(String source) throws Exception {
        CompiledProtos compiled = COMPILER.compile(sourceSet(source));
        return compiled.descriptorFor("schema.proto").orElseThrow();
    }

    /** Diffs two single-file schemas. */
    static List<SchemaChange> diff(String oldSource, String newSource) throws Exception {
        return SchemaDiff.diff(compile(oldSource), compile(newSource));
    }

    /**
     * Adds {@code reserved n;} declarations to the named top-level message. Wire's encoder
     * drops reserved ranges from compiled descriptors, so tests patch them in.
     */
    static FileDescriptorSet reserveNumbers(FileDescriptorSet set, String messageName,
                                            int... numbers) {
        return transformMessage(set, messageName, message -> {
            for (int number : numbers) {
                message.addReservedRange(DescriptorProto.ReservedRange.newBuilder()
                        .setStart(number).setEnd(number + 1));
            }
        });
    }

    /** Adds one {@code reserved start to end;} range (end exclusive) to the named message. */
    static FileDescriptorSet reserveRange(FileDescriptorSet set, String messageName,
                                          int start, int endExclusive) {
        return transformMessage(set, messageName,
                message -> message.addReservedRange(DescriptorProto.ReservedRange.newBuilder()
                        .setStart(start).setEnd(endExclusive)));
    }

    /** Adds {@code reserved "name";} declarations to the named top-level message. */
    static FileDescriptorSet reserveNames(FileDescriptorSet set, String messageName,
                                          String... names) {
        return transformMessage(set, messageName,
                message -> List.of(names).forEach(message::addReservedName));
    }

    /**
     * Sets an explicit {@code json_name} on a field. Wire's encoder drops the option from
     * compiled descriptors, so tests patch it in.
     */
    static FileDescriptorSet withJsonName(FileDescriptorSet set, String messageName,
                                          String fieldName, String jsonName) {
        return transformMessage(set, messageName, message -> {
            for (FieldDescriptorProto.Builder field : message.getFieldBuilderList()) {
                if (field.getName().equals(fieldName)) {
                    field.setJsonName(jsonName);
                }
            }
        });
    }

    private static FileDescriptorSet transformMessage(FileDescriptorSet set, String messageName,
                                                      Consumer<DescriptorProto.Builder> edit) {
        FileDescriptorSet.Builder out = set.toBuilder();
        for (FileDescriptorProto.Builder file : out.getFileBuilderList()) {
            for (DescriptorProto.Builder message : file.getMessageTypeBuilderList()) {
                if (message.getName().equals(messageName)) {
                    edit.accept(message);
                }
            }
        }
        return out.build();
    }

    /** The single change with the given rule id; fails when absent or ambiguous. */
    static SchemaChange single(List<SchemaChange> changes, String ruleId) {
        List<SchemaChange> matches = all(changes, ruleId);
        if (matches.size() != 1) {
            throw new AssertionError("Expected exactly one " + ruleId + " but found "
                    + matches.size() + " in " + changes);
        }
        return matches.get(0);
    }

    static List<SchemaChange> all(List<SchemaChange> changes, String ruleId) {
        return changes.stream().filter(change -> change.ruleId().equals(ruleId)).toList();
    }
}
