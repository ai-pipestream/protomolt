/**
 * The action catalog: JSON-in/JSON-out verbs over the toolkit, mountable as HTTP endpoints or
 * MCP tools.
 *
 * <p>{@link ai.pipestream.proto.actions.ProtoAction} is the extension point. Each action
 * declares a kebab-case name, a description written for tool use, and a JSON Schema for its
 * input envelope, so a catalog listing is a self-describing tool manifest.
 * {@link ai.pipestream.proto.actions.ActionCatalog} registers the built-in actions and is the
 * single dispatch point; {@link ai.pipestream.proto.actions.ActionContext} carries the
 * {@link ai.pipestream.proto.descriptors.DescriptorRegistry} and JSON machinery that actions
 * share. Failures are {@link ai.pipestream.proto.actions.ActionException}s with stable codes
 * rather than free-form messages.</p>
 *
 * <p>Actions that produce results incrementally implement
 * {@link ai.pipestream.proto.actions.StreamingAction} and write to a
 * {@link ai.pipestream.proto.actions.StreamEmitter}; the unary contract still holds, so fronts
 * that collect results need no special handling. Wherever an action takes a schema it follows
 * the convention implemented by {@link ai.pipestream.proto.actions.SchemaResolver}: a registered
 * type name, inline {@code .proto} sources compiled per call, or a serialized descriptor set.</p>
 *
 * <p>JSON is confined to this layer. Every action wraps a descriptor-native library from a
 * sibling module, and machine-to-machine callers should prefer the binary endpoints.
 * See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/actions.md">Actions
 * guide</a> for the catalog of verbs and their envelopes.</p>
 */
package ai.pipestream.proto.actions;
