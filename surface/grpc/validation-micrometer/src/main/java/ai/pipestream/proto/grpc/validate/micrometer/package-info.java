/**
 * Micrometer binding for the gRPC validation metrics SPI.
 *
 * <p>{@link MicrometerGrpcValidationMetrics} implements
 * {@link ai.pipestream.proto.grpc.validate.GrpcValidationMetricsListener} and records the
 * interceptors' events as counters and distribution summaries. Discovery is
 * {@link java.util.ServiceLoader}: with this module on the classpath the no-arg constructor
 * binds to {@link io.micrometer.core.instrument.Metrics#globalRegistry} and every interceptor
 * reports without configuration. Callers holding their own registry construct the listener
 * directly.</p>
 *
 * <p>The meters are named under {@code protomolt.grpc.validation.*} and
 * {@code protomolt.grpc.quality.*}, tagged by side, method, type, and — for violations and
 * dimensions — the schema's own rule and dimension ids. This package is the gRPC counterpart of
 * the serde's Micrometer module; the interceptors themselves live in
 * {@link ai.pipestream.proto.grpc.validate}.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/validation.md">Validation
 * guide</a> for the meter list.</p>
 */
package ai.pipestream.proto.grpc.validate.micrometer;
