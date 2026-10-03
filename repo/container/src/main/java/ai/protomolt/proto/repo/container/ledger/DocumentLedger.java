package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.v1.NodeAddress;
import jakarta.persistence.LockModeType;
import jakarta.persistence.TypedQuery;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

/**
 * Row-level operations on the {@code documents} table — every method is one
 * unit of work through {@link Tx} (see Tx's Javadoc for the usage contract:
 * virtual threads, one EntityManager per call, detached results).
 * <p>
 * This class deliberately contains no business policy: dedupe/revive
 * decisions, key derivation and object-key layout live in the calling
 * service. The ledger only guarantees the identity rules the database
 * enforces (unique storage identity, row-kind/graph shape) surface as
 * intact {@code PersistenceException}s.
 */
public final class DocumentLedger {

    private static final String REFERENCE_WHERE =
            "d.docId = :docId AND d.graphAddressId = :graphAddressId"
            + " AND d.accountId = :accountId AND d.graphId = :graphId";

    private final Tx tx;

    /**
     * @param tx the transactional EntityManager wrapper shared by this service
     */
    public DocumentLedger(Tx tx) {
        this.tx = tx;
    }

    /** Uses the same persistence unit as document publication and its outbox. */
    public RawObjectLedger rawObjects() { return new RawObjectLedger(tx); }

    /**
     * Reserve every legacy PUT/COPY destination before provider I/O. These
     * reservations survive failed saves and row deletion; they grant no cleanup
     * authority and cannot be converted into managed attempts.
     */
    public void reserveLegacyPartKeys(List<String> keys) {
        List<String> immutableKeys = List.copyOf(keys);
        tx.inTransaction(em -> { DocumentKeyReservations.reserve(em, immutableKeys, null); });
    }

    /**
     * Insert-or-update by {@code node_id}. The service pre-populates the
     * record (including the caller-minted nodeId); an existing row with the
     * same nodeId is overwritten with the supplied state. For a body rewrite,
     * the caller is responsible for setting {@code updatedAt} explicitly —
     * nothing here bumps it (see the staleness guard on
     * {@link DocumentRecord}).
     *
     * @param record the row state to store
     * @return the stored row (detached)
     */
    public DocumentRecord save(DocumentRecord record) {
        // Cast disambiguates the Function overload (merge's return value also
        // matches the Consumer overload's expression-lambda shape).
        return tx.inTransaction((Function<jakarta.persistence.EntityManager, DocumentRecord>)
                em -> em.merge(record));
    }

    /** A candidate was prepared against a different row revision or existence state. */
    public static final class RevisionConflictException extends RuntimeException {
        public RevisionConflictException() { super("Document changed while the candidate was being prepared"); }
    }

    /**
     * Publishes only against the expected revision (null means absent). The callback
     * runs after flush/refresh in the same transaction, for the transactional outbox.
     * No object-store work belongs in this transaction.
     */
    public DocumentRecord saveIfRevision(DocumentRecord candidate, Long expectedRevision,
            java.util.function.BiConsumer<jakarta.persistence.EntityManager, DocumentRecord> committed) {
        return saveIfRevision(candidate, expectedRevision, Map.of(), committed);
    }

    /**
     * Also requires every copy source to retain its sampled revision until commit.
     * Lock ordering is shared with ordinary guarded saves, including missing rows.
     * Dependencies must exist; they are never updated by this operation.
     */
    public DocumentRecord saveIfRevision(DocumentRecord candidate, Long expectedRevision,
            Map<UUID, Long> sourceRevisions,
            java.util.function.BiConsumer<jakarta.persistence.EntityManager, DocumentRecord> committed) {
        return saveGuarded(candidate, expectedRevision, sourceRevisions, (em, prior) -> {}, committed);
    }

    /** Internal until provider writes, reads and recovery use the same attempt boundary. */
    DocumentRecord saveVerifiedAttempt(DocumentRecord candidate, Long expectedRevision,
            Map<UUID, Long> sourceRevisions, UUID attemptId, UUID token, DocumentPublicationTarget target,
            java.util.function.BiConsumer<jakarta.persistence.EntityManager, DocumentRecord> committed) {
        java.util.Objects.requireNonNull(committed, "committed");
        Map<UUID, Long> sources = Map.copyOf(sourceRevisions);
        return saveGuarded(candidate, expectedRevision, sources, (em, prior) -> {
            target.lock(em);
            var attempt = DocumentPartAttemptLedger.requirePublishable(em, attemptId, token, candidate, expectedRevision, sources);
            target.requireMatches(em, candidate, attempt);
            var previousManifest = prior == null ? null : prior.readManifest();
            if (prior != null && (previousManifest == null || previousManifest.getDocVersion() <= 0))
                throw new DocumentPartAttemptLedger.FenceException("Existing document requires an explicit versioned manifest");
            long previousVersion = prior == null ? 0 : previousManifest.getDocVersion();
            long nextVersion = Math.addExact(previousVersion, 1);
            if (candidate.readManifest().getDocVersion() != nextVersion)
                throw new DocumentPartAttemptLedger.FenceException("Document manifest version is not the next locked version");
        }, (em, row) -> {
            em.createNativeQuery("""
                    INSERT INTO document_part_publication_history(attempt_id,node_id,publication_revision,body)
                    SELECT :attempt,node_id,mutation_revision,document_publication_body(documents)
                    FROM documents WHERE node_id=:node
                    """).setParameter("attempt", attemptId).setParameter("node", row.nodeId).executeUpdate();
            em.createNativeQuery("""
                    INSERT INTO document_part_publications(node_id,attempt_id) VALUES (:node,:attempt)
                    ON CONFLICT(node_id) DO UPDATE SET attempt_id=EXCLUDED.attempt_id
                    """).setParameter("node", row.nodeId).setParameter("attempt", attemptId).executeUpdate();
            committed.accept(em, row);
        });
    }

    /** Whether this document has an active managed part publication. */
    public boolean hasPartPublication(UUID nodeId) {
        return tx.readOnly(em -> !em.createNativeQuery("SELECT 1 FROM document_part_publications WHERE node_id=:id")
                .setParameter("id", nodeId).getResultList().isEmpty());
    }

    private DocumentRecord saveGuarded(DocumentRecord candidate, Long expectedRevision,
            Map<UUID, Long> sourceRevisions,
            java.util.function.BiConsumer<jakarta.persistence.EntityManager, DocumentRecord> beforeMerge,
            java.util.function.BiConsumer<jakarta.persistence.EntityManager, DocumentRecord> committed) {
        java.util.Objects.requireNonNull(committed, "committed");
        Map<UUID, Long> sources = Map.copyOf(sourceRevisions);
        var identities = new java.util.TreeSet<>(sources.keySet());
        identities.add(candidate.nodeId);
        return tx.inTransaction(em -> {
            // A row lock cannot serialize two first writes to a missing identity.
            // Hash collisions only serialize unrelated writers; they cannot grant access.
            // Sort the actual keys too: UUID ordering alone is insufficient when
            // unrelated UUIDs alias the same advisory lock.
            identities.stream().mapToLong(id -> id.getMostSignificantBits() ^ id.getLeastSignificantBits())
                    .distinct().sorted().forEach(key ->
                            em.createNativeQuery("SELECT 1 FROM pg_advisory_xact_lock(:key)", Integer.class)
                                    .setParameter("key", key).getSingleResult());
            Map<UUID, DocumentRecord> locked = new HashMap<>();
            for (UUID id : identities) {
                DocumentRecord row = em.find(DocumentRecord.class, id, LockModeType.PESSIMISTIC_WRITE);
                locked.put(id, row);
                Long sourceRevision = sources.get(id);
                if (sourceRevision != null && (row == null || row.mutationRevision != sourceRevision.longValue()))
                    throw new RevisionConflictException();
            }
            DocumentRecord current = locked.get(candidate.nodeId);
            if (expectedRevision == null ? current != null
                    : current == null || current.mutationRevision != expectedRevision.longValue())
                throw new RevisionConflictException();
            beforeMerge.accept(em, current);
            DocumentRecord merged = em.merge(candidate);
            em.flush();
            em.refresh(merged);
            committed.accept(em, merged);
            return merged;
        });
    }

    /**
     * Look up a row by primary key.
     *
     * @param nodeId the row's node id
     * @return the row, or empty
     */
    public Optional<DocumentRecord> findByNodeId(UUID nodeId) {
        return tx.readOnly(em -> Optional.ofNullable(em.find(DocumentRecord.class, nodeId)));
    }

    /**
     * Look up a row by primary key with a {@code PESSIMISTIC_WRITE} lock — the
     * purger's staleness-guard re-read. NOTE: the lock is released when this
     * call's transaction commits, so a read-decide-write sequence spread
     * across ledger calls is NOT serialized by it; the purger re-verifies
     * under a fresh lock before removing the row.
     *
     * @param nodeId the row's node id
     * @return the row, or empty (detached)
     */
    public Optional<DocumentRecord> findByNodeIdForUpdate(UUID nodeId) {
        // Cast disambiguates the Function overload (see DocumentLedger.save).
        return tx.inTransaction((java.util.function.Function<jakarta.persistence.EntityManager, Optional<DocumentRecord>>)
                em -> Optional.ofNullable(
                        em.find(DocumentRecord.class, nodeId, LockModeType.PESSIMISTIC_WRITE)));
    }

    /**
     * The first page of rows in one storage status, in stable
     * {@code (created_at, node_id)} order — the sweeper's PENDING_PURGE scan
     * and the coherence probe's AVAILABLE sample.
     *
     * @param status the {@link DocumentStatus} value to scan
     * @param limit  the page size
     * @return the page's rows (detached), possibly empty
     */
    public List<DocumentRecord> listByStatus(String status, int limit) {
        return tx.readOnly(em -> em.createQuery(
                        "SELECT d FROM DocumentRecord d WHERE d.status = :status"
                                + " ORDER BY d.createdAt ASC, d.nodeId ASC",
                        DocumentRecord.class)
                .setParameter("status", status)
                .setMaxResults(limit)
                .getResultList());
    }

    /**
     * Keyset page over ALL rows ordered by primary key — the reconciler's
     * owned-key harvest. Keyset (not offset) pagination keeps pages stable
     * under concurrent writes: an offset page that skips a row would leave
     * its keys out of the owned set and mis-flag live objects as orphans.
     *
     * @param afterNodeId exclusive lower bound (null = first page)
     * @param limit       the page size
     * @return the page's rows (detached) in node_id order
     */
    public List<DocumentRecord> listPage(UUID afterNodeId, int limit) {
        return tx.readOnly(em -> {
            var query = em.createQuery(
                    "SELECT d FROM DocumentRecord d"
                            + (afterNodeId == null ? "" : " WHERE d.nodeId > :after")
                            + " ORDER BY d.nodeId ASC",
                    DocumentRecord.class);
            if (afterNodeId != null) {
                query.setParameter("after", afterNodeId);
            }
            return query.setMaxResults(limit).getResultList();
        });
    }

    /**
     * Look up a row by its canonical storage address (the
     * {@code uq_documents_identity} unique key).
     *
     * @param address the row's canonical storage address
     * @return the row, or empty
     */
    public Optional<DocumentRecord> findByReference(NodeAddress address) {
        return tx.readOnly(em -> referenceQuery(em, address)
                .getResultStream()
                .findFirst());
    }

    /**
     * {@link #findByReference} with a {@code PESSIMISTIC_WRITE} row lock.
     * <p>
     * NOTE: the lock is released when this call's transaction commits, so a
     * read-decide-write sequence spread across ledger calls is NOT
     * serialized by it. Use {@link #withLockedReference} to run the whole
     * decision inside the lock's lifetime.
     *
     * @param address the row's canonical storage address
     * @return the row, or empty (detached)
     */
    public Optional<DocumentRecord> findByReferenceForUpdate(NodeAddress address) {
        return tx.inTransaction(em -> {
            TypedQuery<DocumentRecord> query = referenceQuery(em, address);
            query.setLockMode(LockModeType.PESSIMISTIC_WRITE);
            return query.getResultStream().findFirst();
        });
    }

    /**
     * Lock the row for a storage address ({@code FOR UPDATE}) and run
     * {@code work} against it — and against the SAME EntityManager — inside
     * the lock's lifetime. This is the save path's dedupe/revive primitive:
     * the checksum comparison, the revive decision and the resulting write
     * all happen while no other writer can touch the row.
     *
     * @param address the row's canonical storage address
     * @param work the decision to run against the locked row (empty when no
     *             row exists for the identity — the caller then holds no lock
     *             and races are settled by the unique constraint instead)
     * @param <T>  result type
     * @return the work's result
     */
    public <T> T withLockedReference(NodeAddress address,
            Function<Optional<DocumentRecord>, T> work) {
        return withLockedReference(address, (em, row) -> work.apply(row));
    }

    /** Transaction-aware variant for reference bindings and other atomic commit obligations. */
    public <T> T withLockedReference(NodeAddress address,
            java.util.function.BiFunction<jakarta.persistence.EntityManager, Optional<DocumentRecord>, T> work) {
        return tx.inTransaction(em -> {
            TypedQuery<DocumentRecord> query = referenceQuery(em, address);
            query.setLockMode(LockModeType.PESSIMISTIC_WRITE);
            return work.apply(em, query.getResultStream().findFirst());
        });
    }

    /**
     * Record a dedupe hit: {@code reprocess_count + 1} and
     * {@code last_reprocessed_at = when}. Does NOT touch {@code updatedAt} —
     * the body was not rewritten, only the save was elided, and the
     * staleness guard must not move for bookkeeping.
     *
     * @param nodeId the row's node id
     * @param when   when the dedupe hit happened
     * @return the updated row (detached), or empty if no such row
     */
    public Optional<DocumentRecord> markReprocessed(UUID nodeId, Instant when) {
        return tx.inTransaction(em -> {
            DocumentRecord record = em.find(DocumentRecord.class, nodeId);
            if (record == null) {
                return Optional.empty();
            }
            record.reprocessCount = record.reprocessCount + 1;
            record.lastReprocessedAt = when;
            return Optional.of(record);
        });
    }

    /**
     * Soft-delete a row: status → {@link DocumentStatus#PENDING_PURGE}. Does
     * NOT touch {@code updatedAt} — a reclaim/purge compares {@code updatedAt}
     * against the reclaim's requested_at to void stale purges, and that only
     * works if the tombstone itself cannot move the timestamp.
     *
     * @param nodeId the row's node id
     * @return the tombstoned row (detached), or empty if no such row
     */
    public Optional<DocumentRecord> tombstone(UUID nodeId) {
        return tx.inTransaction(em -> {
            DocumentRecord record = em.find(DocumentRecord.class, nodeId);
            if (record == null) {
                return Optional.empty();
            }
            record.status = DocumentStatus.PENDING_PURGE;
            return Optional.of(record);
        });
    }

    /**
     * List rows matching the filter, plus the total match count. See
     * {@link ListDocumentsFilter} for the (deliberately simple) pagination
     * semantics.
     *
     * @param filter conjunctive filter + page window
     * @return one page and the total count across all pages
     */
    public ListDocumentsResult list(ListDocumentsFilter filter) {
        return list(filter, null, null);
    }

    /** Evaluates visibility before count and offset, within one database cursor transaction. */
    public ListDocumentsResult listVisible(ListDocumentsFilter filter, java.util.Set<String> accountIds,
            java.util.function.Predicate<DocumentRecord> visible) {
        var accounts = java.util.Set.copyOf(accountIds);
        java.util.Objects.requireNonNull(visible, "visible");
        if (accounts.size() > 1024) throw new IllegalArgumentException("At most 1024 account bindings are supported per listing");
        if (accounts.isEmpty()) return new ListDocumentsResult(List.of(), 0);
        return list(filter, accounts, visible);
    }

    private ListDocumentsResult list(ListDocumentsFilter filter, java.util.Set<String> accountIds,
            java.util.function.Predicate<DocumentRecord> visible) {
        Function<jakarta.persistence.EntityManager, ListDocumentsResult> query = em -> {
            // The listing serves "the documents": rows tombstoned for purge
            // (or stuck in PURGE_FAILED) are logically deleted and must not
            // be re-discovered by listers — a replay resubmitting a
            // tombstoned row would resurrect a deleted document's index
            // entry.
            StringBuilder where = new StringBuilder("WHERE d.status = :status");
            Map<String, Object> params = new HashMap<>();
            params.put("status", DocumentStatus.AVAILABLE);
            if (filter.driveName() != null) {
                where.append(" AND d.driveName = :driveName");
                params.put("driveName", filter.driveName());
            }
            if (filter.connectorId() != null) {
                where.append(" AND d.connectorId = :connectorId");
                params.put("connectorId", filter.connectorId());
            }
            if (filter.crawlId() != null) {
                where.append(" AND d.crawlId = :crawlId");
                params.put("crawlId", filter.crawlId());
            }
            if (filter.accountId() != null) {
                where.append(" AND d.accountId = :accountId");
                params.put("accountId", filter.accountId());
            }

            if (accountIds != null) {
                where.append(" AND d.accountId IN :permittedAccounts");
                params.put("permittedAccounts", accountIds);
                TypedQuery<DocumentRecord> scan = em.createQuery(
                        "SELECT d FROM DocumentRecord d " + where
                                + " ORDER BY d.createdAt ASC, d.nodeId ASC", DocumentRecord.class);
                params.forEach(scan::setParameter);
                scan.setHint("org.hibernate.fetchSize", 256);
                scan.setHint("org.hibernate.readOnly", true);
                var rows = new ArrayList<DocumentRecord>();
                long total = 0;
                try (var stream = scan.getResultStream()) {
                    var iterator = stream.iterator();
                    while (iterator.hasNext()) {
                        DocumentRecord row = iterator.next();
                        boolean allowed = visible.test(row);
                        em.detach(row);
                        if (!allowed) continue;
                        if (total >= filter.offset() && rows.size() < filter.effectiveLimit()) rows.add(row);
                        total++;
                    }
                }
                return new ListDocumentsResult(List.copyOf(rows), total);
            }

            TypedQuery<Long> count = em.createQuery(
                    "SELECT COUNT(d) FROM DocumentRecord d " + where, Long.class);
            params.forEach(count::setParameter);
            long total = count.getSingleResult();

            TypedQuery<DocumentRecord> page = em.createQuery(
                    "SELECT d FROM DocumentRecord d " + where
                            + " ORDER BY d.createdAt ASC, d.nodeId ASC",
                    DocumentRecord.class);
            params.forEach(page::setParameter);
            page.setMaxResults(filter.effectiveLimit());
            if (filter.offset() > 0) {
                page.setFirstResult((int) Math.min(filter.offset(), Integer.MAX_VALUE));
            }
            return new ListDocumentsResult(page.getResultList(), total);
        };
        return accountIds == null ? tx.readOnly(query) : tx.inTransaction(query);
    }

    /**
     * Mark a row's purge as permanently failed: status →
     * {@link DocumentStatus#PURGE_FAILED}, but ONLY from PENDING_PURGE (a
     * revived row keeps its live state). Does NOT touch {@code updatedAt} —
     * status-only transitions never move the staleness guard. This is the
     * DLQ landing: the sweeper scans only PENDING_PURGE rows, so a
     * PURGE_FAILED row is out of every automatic retry path (operator
     * territory).
     *
     * @param nodeId the row's node id
     * @return the updated row (detached), or empty if no such row or it was
     *         not PENDING_PURGE
     */
    public Optional<DocumentRecord> markPurgeFailed(UUID nodeId) {
        return tx.inTransaction(em -> {
            DocumentRecord record = em.find(DocumentRecord.class, nodeId);
            if (record == null || !DocumentStatus.PENDING_PURGE.equals(record.status)) {
                return Optional.empty();
            }
            record.status = DocumentStatus.PURGE_FAILED;
            return Optional.of(record);
        });
    }

    /**
     * Hard-delete a row by primary key.
     *
     * @param nodeId the row's node id
     * @return the removed row (detached; its {@code objectKey} is what the
     *         service needs for the storage purge), or empty if no such row
     */
    public Optional<DocumentRecord> deleteByNodeId(UUID nodeId) {
        return tx.inTransaction(em -> {
            DocumentRecord record = em.find(DocumentRecord.class, nodeId);
            if (record == null) {
                return Optional.empty();
            }
            em.remove(record);
            return Optional.of(record);
        });
    }

    /**
     * Hard-delete the row for a storage address.
     *
     * @param address the row's canonical storage address
     * @return the removed row (detached), or empty if no such row
     */
    public Optional<DocumentRecord> deleteByReference(NodeAddress address) {
        return tx.inTransaction(em -> {
            Optional<DocumentRecord> record =
                    referenceQuery(em, address)
                            .getResultStream()
                            .findFirst();
            record.ifPresent(em::remove);
            return record;
        });
    }

    /**
     * Hard-delete EVERY row of one logical document — all storage addresses
     * and graphs for {@code (doc_id, account_id, datasource_id)}. This is the
     * RTBF/forget-me shape: the caller gets the removed rows back because it
     * needs their object keys to purge the part objects from storage.
     *
     * @return the removed rows (detached), possibly empty
     */
    public List<DocumentRecord> deleteLogical(String docId, String accountId, String datasourceId) {
        return tx.inTransaction(em -> {
            List<DocumentRecord> records = new ArrayList<>(em.createQuery(
                            "SELECT d FROM DocumentRecord d WHERE d.docId = :docId"
                                    + " AND d.accountId = :accountId"
                                    + " AND d.datasourceId = :datasourceId",
                            DocumentRecord.class)
                    .setParameter("docId", docId)
                    .setParameter("accountId", accountId)
                    .setParameter("datasourceId", datasourceId)
                    .getResultList());
            records.forEach(em::remove);
            return records;
        });
    }

    private static TypedQuery<DocumentRecord> referenceQuery(
            jakarta.persistence.EntityManager em, NodeAddress address) {
        return em.createQuery(
                        "SELECT d FROM DocumentRecord d WHERE " + REFERENCE_WHERE,
                        DocumentRecord.class)
                .setParameter("docId", address.getDocId())
                .setParameter("graphAddressId", address.getGraphAddressId())
                .setParameter("accountId", address.getAccountId())
                .setParameter("graphId", address.getGraphId());
    }
}
