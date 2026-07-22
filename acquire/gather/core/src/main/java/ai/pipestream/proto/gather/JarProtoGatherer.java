package ai.pipestream.proto.gather;

import ai.pipestream.proto.sources.ProtoSource;
import ai.pipestream.proto.sources.ProtoSourceSet;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Gathers {@code .proto} entries from explicit jar files, preserving each entry's in-jar path
 * as its import path (so {@code common/v1/a.proto} inside the jar imports as
 * {@code common/v1/a.proto}).
 *
 * <p>{@code google/protobuf/**} entries are skipped by default because the compiler supplies
 * the well-known types; see {@link JarProtoExtraction}. Entry include/exclude glob filters
 * apply to the in-jar path. The same entry path in multiple jars is tolerated when the content
 * is identical; differing content is a {@link GatherException} naming both jars.</p>
 */
public final class JarProtoGatherer implements ProtoGatherer {

    private final List<Path> jars;
    private final JarProtoExtraction.Options options;

    private JarProtoGatherer(Builder builder) {
        this.jars = List.copyOf(builder.jars);
        this.options = new JarProtoExtraction.Options(
                builder.includeGoogleWellKnownTypes, builder.includeGlobs, builder.excludeGlobs);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public ProtoSourceSet gather() throws GatherException {
        ProtoSourceSet.Builder builder = ProtoSourceSet.builder();
        for (Path jar : jars) {
            if (!Files.isRegularFile(jar)) {
                throw new GatherException("Jar file does not exist: " + jar);
            }
            List<ProtoSource> extracted;
            try {
                extracted = JarProtoExtraction.extract(jar, "jar:" + jar.getFileName(), options);
            } catch (IOException e) {
                throw new GatherException("Failed reading jar " + jar, e);
            }
            try {
                extracted.forEach(builder::add);
            } catch (IllegalStateException e) {
                throw new GatherException("Conflicting proto sources: " + e.getMessage(), e);
            }
        }
        return builder.build();
    }

    @Override
    public String origin() {
        return jars.stream()
                .map(jar -> jar.getFileName().toString())
                .collect(Collectors.joining(",", "jar:[", "]"));
    }

    /** Builder for {@link JarProtoGatherer}; at least one jar is required. */
    public static final class Builder {

        private final List<Path> jars = new ArrayList<>();
        private final List<String> includeGlobs = new ArrayList<>();
        private final List<String> excludeGlobs = new ArrayList<>();
        private boolean includeGoogleWellKnownTypes = false;

        private Builder() {
        }

        /** Adds a jar file to extract from. */
        public Builder jar(Path jar) {
            jars.add(Objects.requireNonNull(jar, "jar"));
            return this;
        }

        /** Adds jar files to extract from. */
        public Builder jars(Collection<Path> jars) {
            Objects.requireNonNull(jars, "jars").forEach(this::jar);
            return this;
        }

        /**
         * Whether to extract {@code google/protobuf/**} entries. Off by default: the compiler
         * supplies the well-known types, so bundled copies are redundant and risk conflicts.
         */
        public Builder includeGoogleWellKnownTypes(boolean include) {
            this.includeGoogleWellKnownTypes = include;
            return this;
        }

        /** Only entries matching at least one of these globs are extracted (when any are set). */
        public Builder includeEntries(String... globs) {
            includeGlobs.addAll(List.of(globs));
            return this;
        }

        /** Entries matching any of these globs are skipped. */
        public Builder excludeEntries(String... globs) {
            excludeGlobs.addAll(List.of(globs));
            return this;
        }

        public JarProtoGatherer build() {
            if (jars.isEmpty()) {
                throw new IllegalStateException("At least one jar is required");
            }
            return new JarProtoGatherer(this);
        }
    }
}
