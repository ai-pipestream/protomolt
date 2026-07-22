/**
 * JSON Schema (draft 2020-12) generation from protobuf descriptors and their validation rules.
 *
 * <p>{@link ProtoJsonSchemaGenerator} renders a message type as a schema describing its
 * canonical proto3 JSON encoding. Declared validation rules are read through the
 * {@link ai.pipestream.proto.validate.spi.ValidationRuleSource} chain and mapped onto the
 * corresponding JSON Schema keywords. Because the generator consumes the neutral constraint
 * model rather than a specific annotation dialect, any rule source contributes — including
 * protovalidate annotations. Rules with no JSON Schema equivalent are not dropped silently:
 * CEL expressions are surfaced verbatim under the {@code x-pipestream-cel} vendor keyword.
 *
 * <p>The rule-source chain is the extension point: {@link ProtoJsonSchemaGenerator#create()}
 * uses {@link ai.pipestream.proto.validate.spi.ValidationRuleSources#defaults()}, and the
 * overload taking an explicit list lets a caller narrow or extend the dialects consulted. The
 * result is returned as an ordered map or as JSON text.
 *
 * <p>This package describes message types; {@code ai.pipestream.proto.openapi} describes
 * service surfaces, and {@code ai.pipestream.proto.validate} enforces the same rules at
 * runtime. See the
 * <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/json-schema.md">JSON Schema
 * guide</a>.
 */
package ai.pipestream.proto.jsonschema;
