package ai.protomolt.proto.repo.archive;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe in-memory {@link RepositorySinkJobStore} for unit tests and embedded archive hosts.
 */
public final class InMemoryRepositorySinkJobStore implements RepositorySinkJobStore {
    private record Key(String accountId, String operationId) {}

    private final ConcurrentHashMap<Key, StoredJob> jobs = new ConcurrentHashMap<>();

    /** Creates an empty in-memory job store. */
    public InMemoryRepositorySinkJobStore() {}

    @Override
    public Optional<StoredJob> find(String accountId, String operationId) {
        Objects.requireNonNull(accountId, "accountId");
        Objects.requireNonNull(operationId, "operationId");
        return Optional.ofNullable(jobs.get(new Key(accountId, operationId)));
    }

    @Override
    public StoredJob insertOrGet(StoredJob candidate) {
        Objects.requireNonNull(candidate, "candidate");
        Key key = new Key(candidate.job().getAccountId(), candidate.job().getOperationId());
        StoredJob existing = jobs.putIfAbsent(key, candidate);
        return existing != null ? existing : candidate;
    }

    @Override
    public void save(StoredJob updated) {
        Objects.requireNonNull(updated, "updated");
        Key key = new Key(updated.job().getAccountId(), updated.job().getOperationId());
        jobs.put(key, updated);
    }
}
