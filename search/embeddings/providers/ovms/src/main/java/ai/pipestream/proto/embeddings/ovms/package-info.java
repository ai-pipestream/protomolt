/**
 * OpenVINO Model Server (OVMS) provider.
 *
 * <p>{@link ai.pipestream.proto.embeddings.ovms.OvmsEmbeddingProvider} registers under the id
 * {@code ovms} and embeds through the KServe v2 gRPC prediction protocol: texts go out as a
 * BYTES tensor and come back as an FP32 tensor, with tokenization server side. Construct it
 * with a {@code host:port} target and a servable name, or let the ServiceLoader constructor
 * resolve them from the {@code protomolt.embeddings.ovms.target} and
 * {@code protomolt.embeddings.ovms.model} system properties or the
 * {@code PROTOMOLT_OVMS_TARGET} and {@code PROTOMOLT_OVMS_MODEL} environment variables.
 */
package ai.pipestream.proto.embeddings.ovms;
