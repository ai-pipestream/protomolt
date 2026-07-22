/**
 * Compatibility checking between two versions of a protobuf schema.
 *
 * <p>The package separates observation from policy. {@link SchemaDiff} compares two
 * {@link com.google.protobuf.DescriptorProtos.FileDescriptorSet}s and reports every difference
 * as a {@link SchemaChange} carrying the {@link Impact}s it breaks;
 * {@link CompatibilityChecker} evaluates those changes against a {@link CompatibilityMode} and
 * returns a {@link CompatibilityResult} separating the full diff from the subset that violates
 * the mode. Rule identifiers are stable strings declared in {@link ChangeRules}, so gates and
 * tooling can match on them.</p>
 *
 * <p>Modes and direction semantics follow Confluent Schema Registry. Wire, canonical proto3
 * JSON and generated-source impacts are tracked separately; the checker's builder decides which
 * layers participate in the verdict. A check that cannot be carried out — typically a
 * {@link ai.pipestream.proto.sources.ProtoSourceSet} that fails to compile — raises
 * {@link CompatibilityException}, distinct from {@link IncompatibleSchemaException}, which
 * reports a check that ran and found violations.</p>
 *
 * <p>The package holds no persistence and no transport: it is consumed by the registry write
 * path, by CI checks and by pre-publish guards, and compilation of schema text is delegated to
 * {@link ai.pipestream.proto.sources.ProtoSourceCompiler}.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/compatibility.md">compatibility
 * checking guide</a> for usage.</p>
 */
package ai.pipestream.proto.compat;
