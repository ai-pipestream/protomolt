package ai.pipestream.proto.compat;

import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoCompilationException;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The policy layer over {@link SchemaDiff}: evaluates a diff against a
 * {@link CompatibilityMode} and decides which changes are violations. This is the write-gate
 * for registry servers, CI checks and pre-publish hooks.
 *
 * <p>Direction semantics (see {@link Impact} for the exact reader/writer phrasing):
 * {@link CompatibilityMode#BACKWARD} means the new schema must read old data, so a change
 * violates it when it carries {@link Impact#WIRE_BACKWARD} (plus {@link Impact#JSON_BACKWARD}
 * when JSON rules are enabled). {@link CompatibilityMode#FORWARD} mirrors that with the
 * {@code *_FORWARD} impacts; {@link CompatibilityMode#FULL} is the union;
 * {@link CompatibilityMode#NONE} never produces violations, though every change is still
 * reported via {@link CompatibilityResult#changes()}.</p>
 *
 * <p>The default configuration ({@link #create()}) checks binary wire compatibility only —
 * Confluent parity. {@linkplain Builder#includeJsonRules(boolean) JSON rules} additionally
 * treat canonical proto3 JSON payloads (field names and enum value names are the payload;
 * default parsers reject unknown JSON fields) as part of the contract — relevant when a
 * JSON/REST gateway serves the schema. {@linkplain Builder#includeSourceRules(boolean) Source
 * rules} turn every {@link Impact#SOURCE} change into a violation in every mode except
 * {@code NONE}, gating the generated-code and gRPC surface.</p>
 *
 * <p>Instances are immutable and thread-safe.</p>
 */
public final class CompatibilityChecker {

    private final boolean includeJsonRules;
    private final boolean includeSourceRules;
    private final ProtoSourceCompiler compiler = new ProtoSourceCompiler();

    private CompatibilityChecker(Builder builder) {
        this.includeJsonRules = builder.includeJsonRules;
        this.includeSourceRules = builder.includeSourceRules;
    }

    /** A checker enforcing binary wire rules only — the Confluent-parity default. */
    public static CompatibilityChecker create() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Checks {@code newSet} against {@code oldSet} under {@code mode}. */
    public CompatibilityResult check(FileDescriptorSet oldSet, FileDescriptorSet newSet,
                                     CompatibilityMode mode) {
        return toResult(SchemaDiff.diff(oldSet, newSet), mode);
    }

    /**
     * Checks two linked files. Each side's transitive imports are included, so both sides see
     * the same kind of closure.
     */
    public CompatibilityResult check(FileDescriptor oldFile, FileDescriptor newFile,
                                     CompatibilityMode mode) {
        return check(toDescriptorSet(List.of(oldFile)), toDescriptorSet(List.of(newFile)), mode);
    }

    /**
     * Compiles both source sets with {@link ProtoSourceCompiler} and checks the results.
     *
     * @throws CompatibilityException when either side fails to compile
     */
    public CompatibilityResult check(ProtoSourceSet oldSources, ProtoSourceSet newSources,
                                     CompatibilityMode mode) throws CompatibilityException {
        return check(compile(oldSources, "old"), compile(newSources, "new"), mode);
    }

    /**
     * Checks freshly authored sources against descriptors already loaded from a registry: the
     * old side is linked descriptors (transitive imports included), the new side is compiled.
     *
     * @throws CompatibilityException when the new sources fail to compile
     */
    public CompatibilityResult check(List<FileDescriptor> oldFiles, ProtoSourceSet newSources,
                                     CompatibilityMode mode) throws CompatibilityException {
        return check(toDescriptorSet(oldFiles), compile(newSources, "new"), mode);
    }

    /**
     * Checks {@code newSet} against a version history, oldest first. Non-transitive modes check
     * only the latest entry; {@code _TRANSITIVE} modes check every entry. Aggregated changes
     * and violations are labeled with the history index they were found against
     * ({@code history[i]: ...} in the message).
     *
     * @throws IllegalArgumentException when {@code history} is empty
     */
    public CompatibilityResult checkAgainstHistory(List<FileDescriptorSet> history,
                                                   FileDescriptorSet newSet,
                                                   CompatibilityMode mode) {
        if (history.isEmpty()) {
            throw new IllegalArgumentException("history must contain at least one version");
        }
        int first = mode.isTransitive() ? 0 : history.size() - 1;
        List<SchemaChange> changes = new ArrayList<>();
        List<SchemaChange> violations = new ArrayList<>();
        for (int i = first; i < history.size(); i++) {
            for (SchemaChange change : SchemaDiff.diff(history.get(i), newSet)) {
                SchemaChange labeled = new SchemaChange(change.ruleId(), change.path(),
                        change.before(), change.after(),
                        "history[" + i + "]: " + change.message(), change.impacts());
                changes.add(labeled);
                if (isViolation(labeled, mode)) {
                    violations.add(labeled);
                }
            }
        }
        return new CompatibilityResult(mode, changes, violations);
    }

    private CompatibilityResult toResult(List<SchemaChange> changes, CompatibilityMode mode) {
        List<SchemaChange> violations = changes.stream()
                .filter(change -> isViolation(change, mode))
                .toList();
        return new CompatibilityResult(mode, changes, violations);
    }

    private boolean isViolation(SchemaChange change, CompatibilityMode mode) {
        if (mode == CompatibilityMode.NONE) {
            return false;
        }
        Set<Impact> impacts = change.impacts();
        if (includeSourceRules && impacts.contains(Impact.SOURCE)) {
            return true;
        }
        if (mode.checksBackward() && (impacts.contains(Impact.WIRE_BACKWARD)
                || (includeJsonRules && impacts.contains(Impact.JSON_BACKWARD)))) {
            return true;
        }
        return mode.checksForward() && (impacts.contains(Impact.WIRE_FORWARD)
                || (includeJsonRules && impacts.contains(Impact.JSON_FORWARD)));
    }

    private FileDescriptorSet compile(ProtoSourceSet sources, String side)
            throws CompatibilityException {
        try {
            CompiledProtos compiled = compiler.compile(sources);
            return compiled.descriptorSet();
        } catch (ProtoCompilationException e) {
            throw new CompatibilityException(
                    "Failed to compile " + side + " schema sources: " + e.getMessage(), e);
        }
    }

    /** The given files plus their transitive imports, as a descriptor set. */
    private static FileDescriptorSet toDescriptorSet(List<FileDescriptor> files) {
        Map<String, FileDescriptorProto> byName = new LinkedHashMap<>();
        for (FileDescriptor file : files) {
            collect(file, byName);
        }
        return FileDescriptorSet.newBuilder().addAllFile(byName.values()).build();
    }

    private static void collect(FileDescriptor file, Map<String, FileDescriptorProto> out) {
        if (out.putIfAbsent(file.getName(), file.toProto()) != null) {
            return;
        }
        for (FileDescriptor dependency : file.getDependencies()) {
            collect(dependency, out);
        }
    }

    /** Configures which rule families count as violations; see the class javadoc. */
    public static final class Builder {

        private boolean includeJsonRules;
        private boolean includeSourceRules;

        private Builder() {
        }

        /**
         * When {@code true}, {@link Impact#JSON_BACKWARD}/{@link Impact#JSON_FORWARD} impacts
         * count as violations in the corresponding directions. Default {@code false}.
         */
        public Builder includeJsonRules(boolean includeJsonRules) {
            this.includeJsonRules = includeJsonRules;
            return this;
        }

        /**
         * When {@code true}, {@link Impact#SOURCE} impacts count as violations in every mode
         * except {@link CompatibilityMode#NONE}. Default {@code false}.
         */
        public Builder includeSourceRules(boolean includeSourceRules) {
            this.includeSourceRules = includeSourceRules;
            return this;
        }

        public CompatibilityChecker build() {
            return new CompatibilityChecker(this);
        }
    }
}
