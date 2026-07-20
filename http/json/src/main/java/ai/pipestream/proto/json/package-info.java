/**
 * Conversion between protobuf messages and their canonical proto3 JSON form.
 *
 * <p>{@link ProtobufJsonTranscoder} is the entry point. It converts in both directions and
 * resolves message types through an {@link ai.pipestream.proto.descriptors.DescriptorRegistry},
 * so types loaded at runtime from a schema registry are handled as
 * {@link com.google.protobuf.DynamicMessage}s alongside generated classes, and the type
 * registry follows the descriptor registry as it grows.
 *
 * <p>Failures surface as {@link ProtobufJsonException}, with
 * {@link MalformedProtobufJsonException} distinguishing input that cannot be merged into the
 * target message from internal conversion faults. The transcoder carries no HTTP or framework
 * dependencies; {@code ai.pipestream.proto.rest} builds the JSON/REST gateway on top of it.
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/rest-gateway.md">REST
 * gateway guide</a> for how the transcoder is wired into a server.
 */
package ai.pipestream.proto.json;
