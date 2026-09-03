package ai.protomolt.proto.sources.publish;

import java.util.Objects;

/**
 * Options shared by every {@link SchemaPublisher}.
 *
 * @param naming subject/artifact naming strategy
 * @param dryRun when {@code true}, the publisher performs every read (existence and content
 *               checks) but no write, reporting what it would have done
 */
public record PublishOptions(SubjectNamingStrategy naming, boolean dryRun) {

    public PublishOptions {
        Objects.requireNonNull(naming, "naming");
    }

    /** Import-path naming, writes enabled. */
    public static PublishOptions defaults() {
        return new PublishOptions(SubjectNamingStrategy.importPath(), false);
    }

    public static PublishOptions dryRunDefaults() {
        return new PublishOptions(SubjectNamingStrategy.importPath(), true);
    }

    public PublishOptions withNaming(SubjectNamingStrategy naming) {
        return new PublishOptions(naming, dryRun);
    }
}
