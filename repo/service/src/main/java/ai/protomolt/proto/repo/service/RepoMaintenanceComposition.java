package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.accession.CatalogEntryRebuild;
import ai.protomolt.proto.repo.accession.DocumentReads;
import ai.protomolt.proto.repo.blob.spi.BlobStores;
import ai.protomolt.proto.repo.catalog.CatalogConfig;
import ai.protomolt.proto.repo.catalog.PostgresCatalog;
import ai.protomolt.proto.repo.container.ledger.DriveLedger;
import ai.protomolt.proto.repo.container.ledger.LedgerDatabase;
import ai.protomolt.proto.repo.container.ledger.Tx;
import java.time.Clock;
import java.util.Objects;
import java.util.UUID;

/**
 * The catalog-and-storage composition the local maintenance commands open: the
 * ledger database, the drive ledger, the selected blob store, the parts catalog,
 * the per-account drive resolution and verified reads under the configured read
 * budget. Nothing else from {@link RepoServices} is built: no concept-scheme
 * loading, no saves or accession service, no internal step credential
 * registration, no in-process or network listener, no archive projection and no
 * Kafka publisher. Under qualified managed storage it registers and fences its
 * own reader incarnation exactly as a serving host does, because verified reads
 * over managed storage are admitted per incarnation.
 */
final class RepoMaintenanceComposition implements AutoCloseable {
    /** Schema the parts catalog lives in; the same one {@link RepoServices} migrates. */
    static final String CATALOG_SCHEMA = "repo_catalog";

    private final OwnedResources owned = new OwnedResources();
    private final Tx tx;
    private final UUID readerIncarnation;
    private final CatalogEntryRebuild rebuild;
    private boolean closed;

    private RepoMaintenanceComposition(RepoServiceConfig config, BlobStores providers) {
        if (RepoServiceConfig.BLOB_STORE_REPO.equals(config.blobStore())
                || RepoServiceConfig.BLOB_STORE_REPO_INPROCESS.equals(config.blobStore()))
            throw new IllegalArgumentException("Remote repository blob storage is retired: " + config.blobStore());
        UUID startingReader = null;
        Tx openedTx = null;
        try {
            var database = owned.add(new LedgerDatabase(config.ledger()));
            openedTx = new Tx(database.entityManagerFactory());
            var driveLedger = new DriveLedger(openedTx, new SelectedDriveBackend(config));
            var storage = RepoStorageWiring.wire(config, providers, false, owned, openedTx, null);
            startingReader = storage.readerIncarnation();
            var catalog = owned.add(new PostgresCatalog(
                    new CatalogConfig(config.ledger().jdbcUrl(),
                            config.ledger().username(), config.ledger().password(), config.ledger().maxPoolSize()),
                    CATALOG_SCHEMA));
            String selectedDriveProvider = RepoServiceConfig.BLOB_STORE_S3_REDIS_CACHE.equals(config.blobStore())
                    ? "s3" : config.blobStore();
            var drives = RepoV2Wiring.drives(config, catalog, driveLedger, storage.blobStore(), selectedDriveProvider);
            var reads = new DocumentReads(catalog, drives, ai.protomolt.proto.repo.admission.DocumentSchemaAdmission.Limits.SERVICE_DEFAULTS,
                    new ReadByteBudget(config.readBudgetBytes()), Clock.systemUTC());
            this.rebuild = new CatalogEntryRebuild(catalog, reads);
        } catch (RuntimeException | Error failure) {
            if (startingReader != null) {
                try {
                    fenceReader(openedTx, startingReader);
                } catch (RuntimeException | Error cleanup) {
                    if (cleanup != failure) failure.addSuppressed(cleanup);
                }
            }
            try { owned.close(); } catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
            throw failure;
        }
        this.tx = openedTx;
        this.readerIncarnation = startingReader;
    }

    /**
     * Opens the catalog and the configured stores for one local command.
     *
     * @param config the resolved service configuration
     * @return the composition; the caller closes it after the command
     */
    static RepoMaintenanceComposition open(RepoServiceConfig config) {
        return new RepoMaintenanceComposition(Objects.requireNonNull(config), BlobStores.discover());
    }

    /** Verification and repair over this composition's catalog and verified reads. */
    CatalogEntryRebuild rebuild() {
        if (closed) throw new IllegalStateException("Maintenance composition is closed");
        return rebuild;
    }

    /** Fences the reader incarnation, when one was registered, then releases the stores and the ledger. */
    @Override public synchronized void close() {
        if (closed) return;
        closed = true;
        try {
            if (readerIncarnation != null) fenceReader(tx, readerIncarnation);
        } finally {
            owned.close();
        }
    }

    private static void fenceReader(Tx transaction, UUID incarnation) {
        transaction.inTransaction(em -> {
            if (!Boolean.TRUE.equals(em.createNativeQuery("SELECT fence_repository_reader(:id)")
                    .setParameter("id", incarnation).getSingleResult()))
                throw new IllegalStateException("Reader fence was not acknowledged");
        });
        transaction.inTransaction(em -> {
            if (!Boolean.TRUE.equals(em.createNativeQuery("SELECT attest_local_reader_quiescence(:id)")
                    .setParameter("id", incarnation).getSingleResult()))
                throw new IllegalStateException("Reader quiescence was not acknowledged");
        });
    }
}
