package ai.protomolt.proto.compat;

import com.google.protobuf.DescriptorProtos.FileDescriptorSet;

import java.util.ArrayList;
import java.util.List;

/**
 * The diff engine: compares two {@link FileDescriptorSet}s and reports every difference as a
 * {@link SchemaChange} tagged with the {@link Impact}s it carries. Policy — which changes are
 * acceptable under which compatibility mode — lives in {@link CompatibilityChecker}; this class
 * only observes.
 *
 * <p>Matching is by identity that survives refactoring: messages, enums and services are matched
 * by fully-qualified name across the <em>whole</em> set, so moving a type between files (with an
 * unchanged shape) produces no changes. Within a message, fields are matched by number — the
 * wire identity. Within an enum, values are matched by number, with a separate name-keyed pass
 * to distinguish a renumbered value from a removal plus an addition. Service methods are matched
 * by name within their service.</p>
 *
 * <p>Synthetic map-entry messages ({@code options.map_entry}) are never diffed as messages;
 * their key/value fields are diffed at the map field's site with paths like
 * {@code example.Doc.attrs (map value)}. Synthetic proto3-optional oneofs are likewise invisible
 * to the oneof rules.</p>
 *
 * <p>The engine diffs exactly the files it is given: if the old set contains a file (say a
 * well-known import) that the new set lacks, its types are reported as removed. Callers should
 * hand both sides the same dependency closure — the {@link CompatibilityChecker} overloads do.</p>
 *
 * <p>The rules themselves are grouped by what they observe: {@link MessageDiff} for messages and
 * fields (delegating to {@link FieldTypeDiff}, {@link OneofDiff} and {@link ReservedRanges}),
 * {@link EnumDiff} for enums, {@link ServiceDiff} for services. {@link DescriptorIndex} supplies
 * the FQN-keyed view they all match against and {@link DescriptorSnippets} renders the
 * declaration text a change carries.</p>
 */
public final class SchemaDiff {

    private SchemaDiff() {
    }

    /**
     * Diffs {@code oldSet} against {@code newSet} and returns every change, informational ones
     * included, in a stable order (types first, then fields, oneofs, reserved declarations,
     * enums, services).
     */
    public static List<SchemaChange> diff(FileDescriptorSet oldSet, FileDescriptorSet newSet) {
        DescriptorIndex oldIndex = DescriptorIndex.of(oldSet);
        DescriptorIndex newIndex = DescriptorIndex.of(newSet);
        List<SchemaChange> changes = new ArrayList<>();
        MessageDiff.diffAll(oldIndex, newIndex, changes);
        EnumDiff.diffAll(oldIndex, newIndex, changes);
        ServiceDiff.diffAll(oldIndex, newIndex, changes);
        return List.copyOf(changes);
    }
}
