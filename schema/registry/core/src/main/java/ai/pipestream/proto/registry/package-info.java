/**
 * Subject/version schema storage for the protobuf schema registry.
 *
 * <p>{@link SchemaRegistryStore} is the storage SPI: subjects holding ascending 1-based
 * versions, global IDs unique across the store, content-identity lookup, and per-subject
 * compatibility configuration falling back to a global default. Two implementations ship —
 * {@link GitSchemaRegistryStore}, where a Git repository is the storage and every registration
 * is a commit, and {@link InMemorySchemaRegistryStore} for tests and embedding. Both run the
 * same registration pipeline: reference verification, write gate, compile verification.</p>
 *
 * <p>{@link SchemaRegistryStore.WriteGate} is the extension point on the write path.
 * {@link CompatibilityWriteGate} implements it over {@code protomolt-compat}, checking a
 * candidate against the subject's history under its effective mode; modes themselves are opaque
 * validated strings from the Confluent vocabulary in {@link CompatibilityModes}. Rejections are
 * typed: {@link IncompatibleRegistrationException}, {@link InvalidSchemaException},
 * {@link ReferenceNotFoundException} and {@link ReferenceConflictException}, all extending
 * {@link RegistryStoreException}.</p>
 *
 * <p>Stored state is described by {@link StoredSchema} and {@link SchemaReference}, with content
 * identity computed by {@link SchemaContents}. {@link StoredSchemaSources} resolves a stored
 * schema plus its transitive references into a self-contained
 * {@link ai.pipestream.proto.sources.ProtoSourceSet}, which is how both register-time
 * verification and descriptor-set serving obtain compilable input. The HTTP surface over this
 * package lives in the {@code protomolt-registry-server} module.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/registry.md">registry
 * guide</a> for usage.</p>
 */
package ai.pipestream.proto.registry;
