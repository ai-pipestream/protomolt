/**
 * Utilities for working with protobuf payloads, descriptors and schema hygiene.
 *
 * <p>For payloads, {@link AnyHandler} packs, unpacks and inspects
 * {@code google.protobuf.Any} values including type-URL handling,
 * {@link TypeConverter} converts among messages, primitives,
 * {@code Struct}/{@code Value} and plain Java types, and {@link PayloadCodec}
 * combines the two for pipelines that move loosely-typed payloads. Type
 * resolution during unpacking goes through
 * {@link ai.pipestream.proto.descriptors.DescriptorRegistry}.</p>
 *
 * <p>For descriptors, {@link MappingHelper} enumerates message fields as dotted
 * paths with type metadata and {@link MappingHelperJsonSupport} exports that
 * information as JSON, both intended for mapping and schema-browsing user
 * interfaces; {@link MessageDiff} reports field-level differences between two
 * messages of the same type.</p>
 *
 * <p>For schema hygiene, {@link ProtoFqnConflictDetector} rejects
 * cross-file redefinitions of a fully-qualified name that disagree on wire
 * shape, and {@link BinaryProtobufIdentifierValidator} rejects illegal
 * identifiers inside binary descriptors before they reach a registry or
 * compiler. Both report failures as {@link ProtoSchemaValidationException}.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/helpers.md">Core
 * utilities guide</a>.</p>
 */
package ai.pipestream.proto.helpers;
