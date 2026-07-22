/**
 * Build-time half of the Quarkus extension for the ProtoMolt protobuf tools.
 *
 * <p>{@link ai.pipestream.proto.quarkus.deployment.ProtoToolsProcessor} declares the build steps
 * Quarkus runs during augmentation: it registers the {@code protomolt} feature, adds
 * {@link ai.pipestream.proto.quarkus.ProtoToolsProducer} as an unremovable bean — extension jars
 * are not bean archives, so the producer would otherwise go undiscovered — and records it for
 * reflection so the producers resolve in a native image.
 *
 * <p>Nothing in this package is present at runtime. The beans it registers, and the configuration
 * they honor, are documented on the runtime package {@link ai.pipestream.proto.quarkus}.
 *
 * <p>See the
 * <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/framework-integrations.md">Framework
 * integrations</a> guide.
 */
package ai.pipestream.proto.quarkus.deployment;
