package ai.protomolt.proto.repo.container.ledger;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Row-level operations on the {@code drives} table — every method is one
 * unit of work through {@link Tx} (see Tx's Javadoc for the usage contract).
 */
public final class DriveLedger {

    private final Tx tx;
    private final java.util.function.Consumer<DriveRecord> readGate;

    /**
     * @param tx the transactional EntityManager wrapper shared by this service
     */
    public DriveLedger(Tx tx) {
        this(tx, record -> {});
    }

    /** A composition may reject incompatible drive metadata before returning it to storage callers. */
    public DriveLedger(Tx tx, java.util.function.Consumer<DriveRecord> readGate) {
        this.tx = tx;
        this.readGate = java.util.Objects.requireNonNull(readGate);
    }

    /**
     * Insert a new drive. A duplicate {@code (account_id, name)} surfaces as
     * an intact {@code PersistenceException} (constraint violation) — the
     * ledger does not pre-check, the database is the arbiter.
     *
     * @param record the drive to insert
     * @return the inserted drive (detached)
     */
    public DriveRecord insert(DriveRecord record) {
        return tx.inTransaction(em -> {
            em.persist(record);
            return record;
        });
    }

    /**
     * Look up a drive by primary key.
     *
     * @param driveId the drive's id
     * @return the drive, or empty
     */
    public Optional<DriveRecord> findById(UUID driveId) {
        return tx.readOnly(em -> Optional.ofNullable(em.find(DriveRecord.class, driveId))).map(this::checked);
    }

    /**
     * Resolve a drive by its account-scoped name — the lookup document rows
     * depend on, since they reference their drive by bare name.
     *
     * @return the drive, or empty
     */
    public Optional<DriveRecord> findByName(String accountId, String name) {
        return tx.readOnly(em -> em.createQuery(
                        "SELECT d FROM DriveRecord d WHERE d.accountId = :accountId"
                                + " AND d.name = :name",
                        DriveRecord.class)
                .setParameter("accountId", accountId)
                .setParameter("name", name)
                .getResultStream()
                .findFirst()).map(this::checked);
    }

    /**
     * List an account's drives, ordered by name.
     * <p>
     * Continuation is keyset-style over the unique {@code (account_id, name)}
     * ordering: the token is simply the LAST name of the previous page
     * (null/blank = first page). Names are unique per account, so pages are
     * stable under concurrent inserts — no offset drift.
     *
     * @param accountId         owning account
     * @param limit             page size; values &lt;= 0 fall back to 100
     * @param continuationToken last name of the previous page, or null
     * @return the page's drives (detached), possibly empty
     */
    public List<DriveRecord> listByAccount(String accountId, int limit, String continuationToken) {
        int effectiveLimit = limit > 0 ? limit : 100;
        return checkedList(tx.readOnly(em -> {
            var query = em.createQuery(
                    "SELECT d FROM DriveRecord d WHERE d.accountId = :accountId"
                            + (continuationToken == null || continuationToken.isBlank()
                                    ? "" : " AND d.name > :afterName")
                            + " ORDER BY d.name ASC",
                    DriveRecord.class)
                    .setParameter("accountId", accountId)
                    .setMaxResults(effectiveLimit);
            if (continuationToken != null && !continuationToken.isBlank()) {
                query.setParameter("afterName", continuationToken);
            }
            return query.getResultList();
        }));
    }

    /**
     * List every drive across all accounts, ordered by
     * {@code (account_id, name)}, bounded — the periodic reconcile loop's
     * drive enumeration (drive counts are operator-scale, not tenant-scale).
     *
     * @param limit the bound; values &lt;= 0 fall back to 1000
     * @return the drives (detached), possibly empty
     */
    public List<DriveRecord> listAll(int limit) {
        int effectiveLimit = limit > 0 ? limit : 1000;
        return checkedList(tx.readOnly(em -> em.createQuery(
                        "SELECT d FROM DriveRecord d ORDER BY d.accountId ASC, d.name ASC",
                        DriveRecord.class)
                .setMaxResults(effectiveLimit)
                .getResultList()));
    }

    private DriveRecord checked(DriveRecord record) {
        readGate.accept(record);
        return record;
    }

    /** Legacy blob coordinates omit the account; never choose arbitrarily between tenants. */
    public Optional<DriveRecord> findUniqueByName(String name) {
        List<DriveRecord> matches = tx.readOnly(em -> em.createQuery(
                "SELECT d FROM DriveRecord d WHERE d.name = :name", DriveRecord.class)
                .setParameter("name", name).setMaxResults(2).getResultList());
        if (matches.size() > 1) throw new IllegalArgumentException("Drive name is ambiguous across accounts");
        return matches.stream().findFirst().map(this::checked);
    }

    private List<DriveRecord> checkedList(List<DriveRecord> records) {
        records.forEach(readGate);
        return records;
    }
}
