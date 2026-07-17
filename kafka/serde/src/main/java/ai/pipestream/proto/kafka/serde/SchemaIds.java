package ai.pipestream.proto.kafka.serde;

import ai.pipestream.proto.descriptors.DescriptorLoader.DescriptorLoadException;
import ai.pipestream.proto.schema.confluent.ConfluentSchemaRegistryLoader;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Schema ids and schemas from a Confluent-compatible registry, with the packaged descriptor set
 * underneath as a floor.
 *
 * <p>This is the part worth having. A serde that resolves schemas from a registry stops working
 * when the registry does, which turns a metadata service into a hard runtime dependency of every
 * producer and consumer on the cluster. But a serde that only reads a packaged descriptor set
 * cannot follow a topic whose writers evolve. So: consult the registry, and when it cannot answer,
 * fall back to the schema this deployment already packages rather than fail. The fallback is
 * announced once per serde, not per record, because a warning on every message is a second
 * outage.</p>
 *
 * <p>The registry is also not retried per record. Answers are cached: a resolved subject id or
 * schema is kept for the life of the serde (an id in a Confluent-compatible registry names one
 * exact schema forever), and a lookup that failed is not attempted again until the retry backoff
 * expires. Without the backoff, an outage would cost every record a connection attempt — and a
 * registry that recovers would still be answering for a serde that stopped asking.</p>
 *
 * <p>Correctness is not traded away for any of it. The fallback only supplies a schema; the
 * message is still validated against the rules that schema declares, and a frame carrying a type
 * other than the configured one is still refused rather than parsed as the wrong message.</p>
 */
final class SchemaIds implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaIds.class);
    /** Bound on the retry maps; frames carrying garbage ids must not grow them without limit. */
    private static final int MAX_TRACKED_FAILURES = 1024;

    private final ConfluentSchemaRegistryLoader registry;
    private final long retryBackoffNanos;
    private final SerdeMetricsListener metrics;
    private final AtomicBoolean warnedFallback = new AtomicBoolean();
    // Resolved subject ids, kept for the serde's lifetime; the frame is stamped once per subject.
    private final ConcurrentMap<String, OptionalInt> idsBySubject = new ConcurrentHashMap<>();
    // Failed lookups by key, each holding the System.nanoTime() after which to ask again.
    private final ConcurrentMap<String, Long> subjectRetryAt = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, Long> schemaRetryAt = new ConcurrentHashMap<>();

    private SchemaIds(ConfluentSchemaRegistryLoader registry, long retryBackoffMillis,
                      SerdeMetricsListener metrics) {
        this.registry = registry;
        this.retryBackoffNanos = TimeUnit.MILLISECONDS.toNanos(retryBackoffMillis);
        this.metrics = metrics;
    }

    /** @return null when no registry is configured, which is a supported way to run */
    static SchemaIds create(String registryUrl, long retryBackoffMillis,
                            SerdeMetricsListener metrics) {
        return registryUrl == null || registryUrl.isBlank()
                ? null
                : new SchemaIds(new ConfluentSchemaRegistryLoader(URI.create(registryUrl.trim())),
                        retryBackoffMillis, metrics);
    }

    /**
     * The id registered for a subject, or empty when the registry cannot say. Empty is not an
     * error: the caller stamps its configured id and carries on. A subject that resolved once is
     * never asked about again; one that did not is asked again after the backoff, so a registry
     * that comes up (or a subject registered later) repairs the stamped id without a restart.
     */
    OptionalInt idForSubject(String subject) {
        OptionalInt resolved = idsBySubject.get(subject);
        if (resolved != null) {
            return resolved;
        }
        if (inBackoff(subjectRetryAt, subject)) {
            return OptionalInt.empty();
        }
        try {
            OptionalInt id = registry.idForSubject(subject);
            if (id.isPresent()) {
                idsBySubject.put(subject, id);
            } else {
                couldNotAnswer(subjectRetryAt, subject, "subject " + subject + " is not registered");
            }
            return id;
        } catch (DescriptorLoadException e) {
            couldNotAnswer(subjectRetryAt, subject,
                    "looking up subject " + subject + " failed: " + e.getMessage());
            return OptionalInt.empty();
        }
    }

    /**
     * The message a frame's id and index path name, or null when the registry cannot say.
     *
     * <p>A resolved schema that does not contain the index path is treated as unresolved rather
     * than as an error: the caller falls back to its configured type, which then either matches
     * the frame or is refused. Resolved schemas are cached by the registry client for the life of
     * the serde; an id that failed to resolve is retried only after the backoff.</p>
     */
    Descriptor messageFor(int schemaId, List<Integer> indexPath) {
        if (inBackoff(schemaRetryAt, schemaId)) {
            return null;
        }
        try {
            FileDescriptor file = registry.schemaById(schemaId);
            Descriptor message = ConfluentWireFormat.messageAt(file, indexPath);
            if (message == null) {
                fellBack("schema id " + schemaId + " has no message at index path " + indexPath);
            }
            return message;
        } catch (DescriptorLoadException e) {
            couldNotAnswer(schemaRetryAt, schemaId,
                    "resolving schema id " + schemaId + " failed: " + e.getMessage());
            return null;
        }
    }

    /**
     * The type named {@code fullName} within the schema {@code schemaId} names, or null when the
     * registry cannot say — or when the registered schema does not declare the type at all.
     *
     * <p>This is the serializer's half of type identity. The frame's message-index array is a
     * position in the schema the frame's id names, so a serializer that stamps a registry id must
     * compute the index against the <em>registry's</em> file, not its packaged one: the two can
     * declare the same type at different positions, and a consumer following the id would land on
     * the wrong message. A null here tells the caller to stamp the configured id and the packaged
     * index instead — a pair that is at least consistent with itself.</p>
     */
    Descriptor typeInSchema(int schemaId, String fullName) {
        if (inBackoff(schemaRetryAt, schemaId)) {
            return null;
        }
        try {
            FileDescriptor file = registry.schemaById(schemaId);
            Descriptor type = SerdeDescriptors.findMessage(file, fullName);
            if (type == null) {
                fellBack("the schema registered under id " + schemaId + " does not declare "
                        + fullName);
            }
            return type;
        } catch (DescriptorLoadException e) {
            couldNotAnswer(schemaRetryAt, schemaId,
                    "resolving schema id " + schemaId + " failed: " + e.getMessage());
            return null;
        }
    }

    /** Whether {@code key} failed recently enough that asking again would just repeat the answer. */
    private <K> boolean inBackoff(ConcurrentMap<K, Long> retryAt, K key) {
        Long deadline = retryAt.get(key);
        if (deadline == null) {
            return false;
        }
        if (System.nanoTime() - deadline < 0) {
            return true;
        }
        retryAt.remove(key);
        return false;
    }

    private <K> void couldNotAnswer(ConcurrentMap<K, Long> retryAt, K key, String what) {
        fellBack(what);
        if (retryAt.size() >= MAX_TRACKED_FAILURES) {
            retryAt.clear();
        }
        retryAt.put(key, System.nanoTime() + retryBackoffNanos);
    }

    /** Every fallback event reaches metrics; the log line fires once (see warnOnce). */
    private void fellBack(String what) {
        warnOnce(what);
        metrics.onRegistryFallback();
    }

    /** Once per serde: a per-record warning during an outage is its own incident. */
    private void warnOnce(String what) {
        if (warnedFallback.compareAndSet(false, true)) {
            LOG.warn("Falling back to the packaged descriptor set because {}. Records are still "
                    + "validated against the packaged schema. This is logged once.", what);
        }
    }

    @Override
    public void close() {
        registry.close();
    }
}
