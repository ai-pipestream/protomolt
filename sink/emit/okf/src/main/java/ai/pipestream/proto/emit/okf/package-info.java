/**
 * Renders protobuf schemas as Open Knowledge Format (OKF v0.1) bundles.
 *
 * <p>{@link OkfRenderer} converts a set of file descriptors into markdown concept documents with
 * YAML frontmatter — one per message, enum, and service — cross-linked into the knowledge graph
 * agents and data catalogs consume. The {@code ai.pipestream.proto.meta.v1} annotations carried by
 * the contract supply frontmatter and schema-table columns, so descriptions and sensitivity
 * classes come from the schema rather than a separate document.
 * {@link OkfRegistryBundles} renders a whole
 * {@link ai.pipestream.proto.registry.SchemaRegistryStore} the same way, adding a concept per
 * subject with its version table.</p>
 *
 * <p>{@link EmitOkfAction} exposes the renderer as the {@code emit-okf} verb, an
 * {@link ai.pipestream.proto.actions.ProtoAction} that returns the bundle inline. Delivery is a
 * separate step through {@link ai.pipestream.proto.emit.BundleSink}; no destination rides in the
 * request.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/emitting.md">Emitting
 * bundles guide</a> for the bundle layout and the sink options.</p>
 */
package ai.pipestream.proto.emit.okf;
