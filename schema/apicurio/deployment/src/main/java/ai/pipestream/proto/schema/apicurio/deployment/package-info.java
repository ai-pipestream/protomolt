/**
 * Build-time half of the Apicurio Registry Quarkus extension.
 *
 * <p>{@link ProtoToolsApicurioProcessor} declares the extension feature and registers the
 * runtime CDI beans — the registry client producer and the startup installer — as unremovable
 * additional beans, so an application picks up the loader without declaring the beans itself.</p>
 *
 * <p>Nothing in this package is on the application runtime classpath; the loader, publisher and
 * configuration types it refers to live in {@code ai.pipestream.proto.schema.apicurio}.</p>
 */
package ai.pipestream.proto.schema.apicurio.deployment;
