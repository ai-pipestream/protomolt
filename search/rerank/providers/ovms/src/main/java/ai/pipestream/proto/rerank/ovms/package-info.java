/**
 * OpenVINO Model Server (OVMS) rerank provider.
 *
 * <p>{@link ai.pipestream.proto.rerank.ovms.OvmsRerankProvider} registers under the id
 * {@code ovms} and reranks through an OVMS rerank servable's REST endpoint
 * ({@code POST {base}/v3/rerank}): the servable's graph expects an HTTP payload packet, so
 * the gRPC ModelInfer path does not serve it. Construct it with a base URL and model name, or
 * let the ServiceLoader constructor resolve them from the
 * {@code protomolt.rerank.ovms.url} and {@code protomolt.rerank.ovms.model} system properties
 * or the {@code PROTOMOLT_RERANK_OVMS_URL} and {@code PROTOMOLT_RERANK_OVMS_MODEL}
 * environment variables.
 */
package ai.pipestream.proto.rerank.ovms;
