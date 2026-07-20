/**
 * The schema-publisher SPI: registering proto sources with a schema registry.
 *
 * <p>{@link SchemaPublisher} is the extension point and the write-side
 * counterpart of {@link ai.pipestream.proto.descriptors.DescriptorLoader}. It
 * takes an {@link ai.pipestream.proto.sources.ProtoSourceSet} — however it was
 * gathered — and registers its files in reverse-topological import order, so
 * every reference exists before the file that imports it. Implementations for
 * Confluent-compatible registries and Apicurio Registry v3 live in their own
 * modules.</p>
 *
 * <p>{@link PublishOptions} carries the {@link SubjectNamingStrategy} that maps
 * an import path to the subject or artifact ID it is registered under, plus a
 * dry-run flag that performs every read but no write.
 * {@link PublishResult} reports one
 * {@link PublishResult.FileOutcome} per file. Publishing is idempotent:
 * identical content reports {@link PublishResult.Action#UNCHANGED} rather than
 * minting a version. Compatibility enforcement belongs to the registry, so a
 * server-side rejection appears as a {@link PublishResult.Action#FAILED}
 * outcome for that file; {@link SchemaPublishException} is reserved for
 * registry-level failures such as an unreachable endpoint.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/publishing.md">Publishing
 * schemas to registries guide</a>.</p>
 */
package ai.pipestream.proto.sources.publish;
