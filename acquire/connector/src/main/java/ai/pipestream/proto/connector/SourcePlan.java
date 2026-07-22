package ai.pipestream.proto.connector;

/**
 * Marker for the typed plan a {@link StreamSource} opens from. Each source kind declares
 * its own plan record carrying exactly what it needs (a channel and method for gRPC, a
 * topic and consumer config for Kafka), so plans stay type-safe instead of collapsing
 * into a property bag.
 */
public interface SourcePlan {
}
