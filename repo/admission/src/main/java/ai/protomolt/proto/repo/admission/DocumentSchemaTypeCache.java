package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.repo.v1.PublicationSchemaCondition;
import com.google.protobuf.Descriptors.Descriptor;

import java.util.LinkedHashMap;
import java.util.Objects;

/**
 * Linked schema types that {@link DocumentSchemaBinding} already verified, keyed by
 * the exact condition (type name and descriptor fingerprint), the artifact's SHA-256
 * and the closure limits it was loaded under. A hit is only possible for a caller that
 * holds the same verified bytes, so it discloses nothing and grants nothing; it saves
 * relinking and refingerprinting the closure, and it hands back the same
 * {@link Descriptor} instance, so descriptor-keyed memo maps hit across requests.
 * Failures are never cached. The byte bound counts serialized artifacts, not the
 * linked descriptors' heap.
 */
final class DocumentSchemaTypeCache {
    record Limits(int maxEntries, long maxArtifactBytes) {
        Limits {
            if (maxEntries < 1 || maxArtifactBytes < 1)
                throw new IllegalArgumentException("positive schema type cache limits required");
        }
    }

    record Key(ClosedDescriptorSet.Limits limits, PublicationSchemaCondition condition, String artifactSha256) {
        Key {
            Objects.requireNonNull(limits, "limits");
            Objects.requireNonNull(condition, "condition");
            Objects.requireNonNull(artifactSha256, "artifactSha256");
        }
    }

    private record Entry(Descriptor type, long artifactBytes) {
    }

    /** The host-wide cache: retained reads of one revision link the same closures on every call. */
    static final DocumentSchemaTypeCache SHARED = new DocumentSchemaTypeCache(new Limits(256, 64L * 1024 * 1024));

    private final Limits limits;
    private final LinkedHashMap<Key, Entry> entries = new LinkedHashMap<>(16, .75f, true);
    private long artifactBytes;

    DocumentSchemaTypeCache(Limits limits) {
        this.limits = Objects.requireNonNull(limits, "limits");
    }

    synchronized Descriptor find(Key key) {
        var entry = entries.get(Objects.requireNonNull(key, "key"));
        return entry == null ? null : entry.type();
    }

    /** Keeps the first type stored under {@code key}; an artifact larger than the byte bound is not kept. */
    synchronized Descriptor put(Key key, Descriptor type, long size) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(type, "type");
        var existing = entries.get(key);
        if (existing != null) return existing.type();
        if (size > limits.maxArtifactBytes()) return type;
        var iterator = entries.values().iterator();
        while ((entries.size() >= limits.maxEntries() || size > limits.maxArtifactBytes() - artifactBytes)
                && iterator.hasNext()) {
            artifactBytes -= iterator.next().artifactBytes();
            iterator.remove();
        }
        entries.put(key, new Entry(type, size));
        artifactBytes += size;
        return type;
    }

    synchronized int size() {
        return entries.size();
    }

    synchronized long artifactBytes() {
        return artifactBytes;
    }
}
