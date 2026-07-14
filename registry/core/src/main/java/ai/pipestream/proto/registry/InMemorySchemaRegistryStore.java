package ai.pipestream.proto.registry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Heap-only {@link SchemaRegistryStore} for tests and embedding. Runs the same registration
 * pipeline as the git store (reference verification, write gate, compile verification); state
 * is guarded by a single {@link ReentrantLock}.
 */
public final class InMemorySchemaRegistryStore implements SchemaRegistryStore {

    private final ReentrantLock lock = new ReentrantLock();
    private final Map<String, Subject> subjects = new TreeMap<>();
    private final Map<Integer, StoredSchema> byGlobalId = new HashMap<>();
    private final WriteGate writeGate;
    private String globalMode = CompatibilityModes.DEFAULT_GLOBAL;
    private int nextGlobalId = 1;

    private static final class Subject {
        final List<StoredSchema> versions = new ArrayList<>();
        String mode;
    }

    public InMemorySchemaRegistryStore() {
        this(null);
    }

    /** Creates a store enforcing compatibility through the given gate. */
    public InMemorySchemaRegistryStore(WriteGate writeGate) {
        this.writeGate = writeGate;
    }

    @Override
    public List<String> subjects() {
        lock.lock();
        try {
            return subjects.entrySet().stream()
                    .filter(entry -> !entry.getValue().versions.isEmpty())
                    .map(Map.Entry::getKey)
                    .toList();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<Integer> versions(String subject) {
        lock.lock();
        try {
            Subject state = subjects.get(subject);
            if (state == null) {
                return List.of();
            }
            return state.versions.stream().map(StoredSchema::version).toList();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<StoredSchema> version(String subject, int version) {
        lock.lock();
        try {
            Subject state = subjects.get(subject);
            if (state == null || version < 1 || version > state.versions.size()) {
                return Optional.empty();
            }
            return Optional.of(state.versions.get(version - 1));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<StoredSchema> latest(String subject) {
        lock.lock();
        try {
            Subject state = subjects.get(subject);
            if (state == null || state.versions.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(state.versions.getLast());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<StoredSchema> byGlobalId(int globalId) {
        lock.lock();
        try {
            return Optional.ofNullable(byGlobalId.get(globalId));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<StoredSchema> findByContent(String subject, String schemaText,
                                                List<SchemaReference> references) {
        List<SchemaReference> refs = references == null ? List.of() : List.copyOf(references);
        lock.lock();
        try {
            Subject state = subjects.get(subject);
            if (state == null) {
                return Optional.empty();
            }
            return state.versions.stream()
                    .filter(stored -> SchemaContents.sameContent(stored, schemaText, refs))
                    .findFirst();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public StoredSchema register(String subject, String schemaText, List<SchemaReference> references)
            throws RegistryStoreException {
        RegistrationSupport.requireSubject(subject);
        List<SchemaReference> refs = references == null ? List.of() : List.copyOf(references);
        lock.lock();
        try {
            RegistrationSupport.verifyReferences(this, refs);
            Optional<StoredSchema> existing = findByContent(subject, schemaText, refs);
            if (existing.isPresent()) {
                return existing.get();
            }
            List<StoredSchema> history = RegistrationSupport.history(this, subject);
            RegistrationSupport.enforceWriteGate(writeGate, this, subject, history, schemaText, refs);
            RegistrationSupport.compileCandidate(this, subject, schemaText, refs);

            Subject state = subjects.computeIfAbsent(subject, name -> new Subject());
            StoredSchema stored = new StoredSchema(subject, state.versions.size() + 1, nextGlobalId++,
                    schemaText, refs, SchemaContents.contentHash(schemaText, refs));
            state.versions.add(stored);
            byGlobalId.put(stored.globalId(), stored);
            return stored;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<String> compatibilityMode(String subject) {
        lock.lock();
        try {
            Subject state = subjects.get(subject);
            return state == null ? Optional.empty() : Optional.ofNullable(state.mode);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void setCompatibilityMode(String subject, String mode) {
        RegistrationSupport.requireSubject(subject);
        CompatibilityModes.requireValid(mode);
        lock.lock();
        try {
            subjects.computeIfAbsent(subject, name -> new Subject()).mode = mode;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public String globalCompatibilityMode() {
        lock.lock();
        try {
            return globalMode;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void setGlobalCompatibilityMode(String mode) {
        CompatibilityModes.requireValid(mode);
        lock.lock();
        try {
            globalMode = mode;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        // nothing to release
    }
}
