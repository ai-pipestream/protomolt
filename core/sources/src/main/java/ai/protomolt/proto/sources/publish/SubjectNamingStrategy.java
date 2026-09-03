package ai.protomolt.proto.sources.publish;

/**
 * Maps a proto import path to the subject (Confluent) or artifact ID (Apicurio) it is
 * registered under.
 *
 * <p>The default, {@link #importPath()}, uses the import path itself. This is the convention
 * that makes reference resolution round-trip: Schema Registry references carry the import path
 * as their {@code name}, so a subject named after the import path is discoverable from the
 * reference alone.</p>
 */
@FunctionalInterface
public interface SubjectNamingStrategy {

    /** Subject/artifact name for the file at the given import path. */
    String subjectFor(String importPath);

    /** The import path itself, e.g. {@code common/v1/core.proto}. Round-trips with references. */
    static SubjectNamingStrategy importPath() {
        return path -> path;
    }

    /** The file's base name without the {@code .proto} suffix, e.g. {@code core}. */
    static SubjectNamingStrategy baseName() {
        return path -> {
            String name = path.substring(path.lastIndexOf('/') + 1);
            return name.endsWith(".proto") ? name.substring(0, name.length() - ".proto".length()) : name;
        };
    }

    /** A fixed prefix in front of {@link #importPath()}, e.g. {@code schemas/}. */
    static SubjectNamingStrategy prefixed(String prefix) {
        return path -> prefix + path;
    }
}
