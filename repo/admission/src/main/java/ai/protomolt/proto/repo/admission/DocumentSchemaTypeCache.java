package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.repo.v1.PublicationSchemaCondition;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;

import java.util.LinkedHashMap;
import java.util.Objects;

/**
 * Linked schema types that {@link DocumentSchemaBinding} already verified, keyed by
 * the exact condition (type name and descriptor fingerprint) and the closure limits
 * it was loaded under. Each entry keeps a private copy of the artifact it verified,
 * and a lookup hits only when the presented bytes equal that copy, so a hit discloses
 * nothing and grants nothing. A hit skips relinking, refingerprinting and rehashing
 * the closure, and it hands back the same {@link Descriptor} instance, so
 * descriptor-keyed memo maps hit across requests. Different bytes with the same
 * condition (an equivalent closure in another file order) are verified in full and
 * then share the entry's type. Failures are never cached. The byte bound counts the
 * retained artifact copies, not the linked descriptors' heap.
 */
final class DocumentSchemaTypeCache {
    record Limits(int maxEntries, long maxArtifactBytes) {
        Limits {
            if (maxEntries < 1 || maxArtifactBytes < 1)
                throw new IllegalArgumentException("positive schema type cache limits required");
        }
    }

    record Key(ClosedDescriptorSet.Limits limits, PublicationSchemaCondition condition) {
        Key {
            Objects.requireNonNull(limits, "limits");
            Objects.requireNonNull(condition, "condition");
        }
    }

    /** A verified type, the exact artifact it was linked from and that artifact's SHA-256. */
    record Linked(Descriptor type, ByteString artifact, String artifactSha256) {
        Linked {
            Objects.requireNonNull(type, "type");
            Objects.requireNonNull(artifact, "artifact");
            Objects.requireNonNull(artifactSha256, "artifactSha256");
        }
    }

    /** The host-wide cache: retained reads of one revision link the same closures on every call. */
    static final DocumentSchemaTypeCache SHARED = new DocumentSchemaTypeCache(new Limits(256, 64L * 1024 * 1024));

    private final Limits limits;
    private final LinkedHashMap<Key, Linked> entries = new LinkedHashMap<>(16, .75f, true);
    private long artifactBytes;

    DocumentSchemaTypeCache(Limits limits) {
        this.limits = Objects.requireNonNull(limits, "limits");
    }

    /** The entry for {@code key} when it was linked from exactly {@code artifact}; otherwise {@code null}. */
    synchronized Linked find(Key key, ByteString artifact) {
        Objects.requireNonNull(artifact, "artifact");
        var entry = entries.get(Objects.requireNonNull(key, "key"));
        return entry != null && entry.artifact().equals(artifact) ? entry : null;
    }

    /**
     * Records a type verified from {@code artifact} and returns the type to use: the
     * entry's existing type when {@code key} is already present, so equivalent closures
     * share one instance. An artifact larger than the byte bound is not kept.
     */
    synchronized Descriptor put(Key key, Descriptor type, ByteString artifact, String artifactSha256) {
        Objects.requireNonNull(key, "key");
        var linked = new Linked(type, artifact, artifactSha256);
        var existing = entries.remove(key);
        if (existing != null) {
            artifactBytes -= existing.artifact().size();
            type = existing.type();
        }
        if (artifact.size() > limits.maxArtifactBytes()) return type;
        var iterator = entries.values().iterator();
        while ((entries.size() >= limits.maxEntries() || artifact.size() > limits.maxArtifactBytes() - artifactBytes)
                && iterator.hasNext()) {
            artifactBytes -= iterator.next().artifact().size();
            iterator.remove();
        }
        // Keep a compact copy: the presented bytes may be a slice of a larger buffer.
        entries.put(key, new Linked(type, ByteString.copyFrom(linked.artifact().toByteArray()), artifactSha256));
        artifactBytes += artifact.size();
        return type;
    }

    synchronized int size() {
        return entries.size();
    }

    synchronized long artifactBytes() {
        return artifactBytes;
    }
}
