/**
 * gRPC interceptors that enforce a schema's declared rules at the call boundary.
 *
 * <p>{@link ValidatingServerInterceptor} validates every inbound request message — unary or
 * streamed — before the handler sees it, and refuses a violation with
 * {@code INVALID_ARGUMENT}. It also measures requests against the quality dimensions their
 * schema declares, so a configured floor turns
 * {@link ai.pipestream.proto.quality.QualityReport} into admission criteria.
 * {@link ValidatingClientInterceptor} is the outbound half: an invalid request fails locally
 * rather than crossing the network to be told what the descriptor already said.</p>
 *
 * <p>{@link GrpcValidationMetricsListener} is the extension point. Implementations are
 * discovered with {@link java.util.ServiceLoader} and hear every validation, rejection and
 * quality score; a listener that throws is logged once and never fails a call. The
 * {@code ai.pipestream.proto.grpc.validate.micrometer} package ships the Micrometer binding.</p>
 *
 * <p>The rules and dimensions themselves come from sibling modules — the engine is
 * {@link ai.pipestream.proto.validate.ProtoValidator} and the scorer is
 * {@link ai.pipestream.proto.quality.QualityScorer} — so this package holds only the call-path
 * wiring, and the same contract governs a topic and a service.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/validation.md">Validation
 * guide</a> for the rule dialects and the gRPC boundary.</p>
 */
package ai.pipestream.proto.grpc.validate;
