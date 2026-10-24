package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.repo.v1.PublicationSchemaCondition;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import java.util.LinkedHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Read-coordinator-scoped descriptor linkage, keyed by canonical schema fingerprint
 * and type. Entries also retain their exact verified artifact: a hit never substitutes
 * one artifact for different supplied bytes. Metadata, retained-store integrity and
 * access checks remain the caller's responsibility on every request.
 * Limits bound entry count and serialized descriptor weight, not total JVM heap.
 */
public final class DocumentDescriptorCache {
    private record Key(PublicationSchemaCondition condition, ClosedDescriptorSet.Limits limits) {}
    private final LinkedHashMap<Key, DocumentSchemaBinding> entries = new LinkedHashMap<>(16, .75f, true);
    private final ReentrantLock lock = new ReentrantLock();
    private final int maxEntries;
    private final long maxBytes;
    private long bytes;

    /**
     * @param maxEntries maximum linked closures retained by this coordinator
     * @param maxBytes maximum serialized descriptor weight retained
     */
    public DocumentDescriptorCache(int maxEntries, long maxBytes) {
        if (maxEntries < 1 || maxBytes < 1) throw new IllegalArgumentException("positive cache limits required");
        this.maxEntries = maxEntries;
        this.maxBytes = maxBytes;
    }

    /**
     * Binds the definition's own exact artifact and metadata, reusing verified linkage.
     * @param definition the verified retained definition
     * @return its message descriptor
     */
    public Descriptor type(DocumentSchemaAdmission.Definition definition) {
        return DocumentSchemaAssetBinding.bind(definition.metadata(), definition.descriptors(),
                new ClosedDescriptorSet.Limits(DocumentRetainedDefinitions.MAX_DESCRIPTOR_BYTES, 256, 4096, 64),
                null, () -> {}, this).schema().type();
    }

    DocumentSchemaBinding find(PublicationSchemaCondition condition, ByteString artifact,
            ClosedDescriptorSet.Limits limits) {
        lock.lock();
        try {
            var cached = entries.get(new Key(condition, limits));
            // Exact byte equality matters even if untrusted metadata claims the cached fingerprint.
            return cached != null && cached.artifact().equals(artifact) ? cached : null;
        } finally {
            lock.unlock();
        }
    }

    DocumentSchemaBinding remember(DocumentSchemaBinding binding, ClosedDescriptorSet.Limits limits) {
        if (binding.artifact().size() > maxBytes) return binding;
        lock.lock();
        try {
            var key = new Key(binding.condition(), limits);
            var old = entries.get(key);
            if (old != null) {
                // Both artifacts were independently verified to have this canonical fingerprint.
                binding = new DocumentSchemaBinding(binding.condition(), binding.artifact(),
                        binding.artifactSha256(), old.type());
                bytes -= old.artifact().size();
            }
            entries.put(key, binding);
            bytes += binding.artifact().size();
            while (entries.size() > maxEntries || bytes > maxBytes) {
                var oldest = entries.firstEntry();
                bytes -= oldest.getValue().artifact().size();
                entries.remove(oldest.getKey());
            }
            return binding;
        } finally {
            lock.unlock();
        }
    }
}
