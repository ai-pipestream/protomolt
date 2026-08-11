package ai.pipestream.proto.inference.spi;

import ai.pipestream.proto.inference.v1.ModelEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The model catalog: the in-memory registry of {@link ModelEntry}s that makes
 * "add a model" a data operation instead of a code change.
 *
 * <p>Registration validates the entry (id, provider, endpoint present) and
 * rejects duplicates; every mutation increments the generation counter that
 * list-models reports, so consumers can tell a stale view from a current one.
 * Instances are thread-safe.</p>
 */
public final class InferenceCatalog {

    private final ConcurrentMap<String, ModelEntry> entries = new ConcurrentHashMap<>();
    private final AtomicLong generation = new AtomicLong();

    /**
     * Registers one model entry.
     *
     * @param entry the entry to register
     * @throws InferenceException on an incomplete entry or a duplicate id
     */
    public void register(ModelEntry entry) {
        if (entry.getId().isEmpty()) {
            // ModelEntry now contains a secret-sensitive credential reference;
            // never stringify the whole entry into a failure message.
            throw new InferenceException("catalog entry has no id");
        }
        if (entry.getProvider().isEmpty()) {
            throw new InferenceException("catalog entry " + entry.getId() + " has no provider");
        }
        if (entry.getEndpoint().isEmpty()) {
            throw new InferenceException("catalog entry " + entry.getId() + " has no endpoint");
        }
        if (entries.putIfAbsent(entry.getId(), entry) != null) {
            throw new InferenceException("duplicate catalog model id: " + entry.getId());
        }
        generation.incrementAndGet();
    }

    /**
     * Removes one model entry.
     *
     * @param id the catalog id to remove
     * @throws InferenceException when the id is not registered
     */
    public void remove(String id) {
        if (entries.remove(id) == null) {
            throw new UnknownModelException("unknown catalog model id: " + id);
        }
        generation.incrementAndGet();
    }

    /**
     * Looks up one model entry.
     *
     * @param id the catalog id
     * @return the entry
     * @throws UnknownModelException when the id is not registered
     */
    public ModelEntry get(String id) {
        ModelEntry entry = entries.get(id);
        if (entry == null) {
            throw new UnknownModelException("unknown catalog model id: " + id);
        }
        return entry;
    }

    /**
     * Lists entries, optionally restricted to one provider, ordered by id for
     * a stable wire shape.
     *
     * @param provider provider id to filter by, or empty for all entries
     * @return the matching entries
     */
    public List<ModelEntry> list(String provider) {
        List<ModelEntry> result = new ArrayList<>();
        for (ModelEntry entry : entries.values()) {
            if (provider.isEmpty() || entry.getProvider().equals(provider)) {
                result.add(entry);
            }
        }
        result.sort((a, b) -> a.getId().compareTo(b.getId()));
        return result;
    }

    /**
     * The catalog generation: a mutation counter reported by list-models.
     *
     * @return the current generation
     */
    public long generation() {
        return generation.get();
    }
}
