package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.blob.cache.CachingBlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobCapability;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobStores;
import ai.protomolt.proto.repo.blob.spi.NamespaceProvisioner;
import ai.protomolt.proto.repo.container.archive.ArchiveLedger;
import ai.protomolt.proto.repo.container.ledger.ManagedBackendLedger;
import ai.protomolt.proto.repo.container.ledger.Tx;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Builds the storage backing: the blob-provider selection (S3, Redis, or S3
 * with a Redis cache), the namespace provisioner, the archive ledger and,
 * under qualified managed storage, the managed archive services. A builder
 * only: every opened provider registers with the caller's
 * {@link OwnedResources} in the same order as before, and the caller keeps
 * ownership of construction-failure cleanup and the close order.
 */
final class RepoStorageWiring {

    private RepoStorageWiring() {
    }

    /** What the storage selection produced; {@code managedArchive} is null when unqualified. */
    record Wired(BlobStore blobStore, NamespaceProvisioner namespaces,
            ArchiveLedger archiveLedger, ManagedArchiveServices managedArchive) {
    }

    /**
     * Selects and opens the blob backing, then wires managed archive services
     * when the configuration is retention-qualified. The
     * {@link ManagedArchiveServices} construction is the last step, so a
     * failure anywhere in here never leaves an unreachable started reader.
     */
    static Wired wire(RepoServiceConfig config, BlobStores providers, boolean bounded,
            OwnedResources owned, Tx tx, ReaderHostOptions readerHost) {
        BlobStore blobStore;
        NamespaceProvisioner namespaces;
        SelectedBlobBacking selectedBacking;
        switch (config.blobStore()) {
            case RepoServiceConfig.BLOB_STORE_S3 -> {
                selectedBacking = SelectedBlobBacking.open(providers, "s3", s3Options(config),
                        config.managedStorage().retentionQualified(), owned);
                var selected = selectedBacking.handle();
                blobStore = selected.store();
                namespaces = selected::ensureNamespace;
            }
            case RepoServiceConfig.BLOB_STORE_REDIS -> {
                selectedBacking = SelectedBlobBacking.open(providers, "redis", redisOptions(config, bounded),
                        config.managedStorage().retentionQualified(), owned);
                var selected = selectedBacking.handle();
                blobStore = selected.store();
                namespaces = selected::ensureNamespace;
            }
            case RepoServiceConfig.BLOB_STORE_S3_REDIS_CACHE -> {
                selectedBacking = SelectedBlobBacking.open(providers, "s3", s3Options(config),
                        config.managedStorage().retentionQualified(), owned);
                var backing = selectedBacking.handle();
                var cache = owned.add(providers.open("redis", redisOptions(config)));
                blobStore = new CachingBlobStore(backing.store(), cache.store(),
                        config.redisTtlSeconds(), config.redisMaxObjectBytes());
                selectedBacking = selectedBacking.withReclaimer(
                        ((CachingBlobStore) blobStore).reclaimer(backing.reclaimer()));
                namespaces = backing::ensureNamespace;
            }
            default -> throw new IllegalArgumentException(
                    "Remote repository blob storage is retired: " + config.blobStore());
        }
        String generation = config.managedStorage().retentionQualified()
                ? config.managedStorage().backendGeneration() : null;
        var archiveLedger = new ArchiveLedger(tx);
        ManagedArchiveServices managedArchive = null;
        if (generation != null) {
            var backing = Objects.requireNonNull(selectedBacking, "Managed storage requires a selected backing");
            var managedCapabilities = backing.handle().capabilities();
            if (!managedCapabilities.containsAll(Set.of(
                    BlobCapability.NON_EXPIRING_WRITES, BlobCapability.PHYSICAL_RECLAMATION)))
                throw new IllegalArgumentException("Selected backing provider cannot support managed ingestion and reclamation");
            if (!bounded && !managedCapabilities.contains(BlobCapability.STREAMING_WRITE))
                throw new IllegalArgumentException("Selected backing provider cannot support managed streaming ingestion");
            var profiles = new ManagedBackendLedger(tx);
            var profile = new ManagedBackendLedger.Profile(
                    backing.requireManagedIdentity(), config.managedStorage().storageRealm());
            profiles.bind(generation, profile);
            var reclaimer = backing.reclaimer();
            managedArchive = new ManagedArchiveServices(tx, archiveLedger, blobStore,
                    managedCapabilities, generation, profile, reclaimer,
                    readerHost == null ? null : readerHost.execution());
        }
        return new Wired(blobStore, namespaces, archiveLedger, managedArchive);
    }

    private static Map<String, String> redisOptions(RepoServiceConfig config, boolean bounded) {
        var options = new HashMap<>(redisOptions(config));
        options.put("write-policy", bounded ? "create-only" : "replace");
        return Map.copyOf(options);
    }

    private static Map<String, String> redisOptions(RepoServiceConfig config) {
        return Map.of("uri", config.redisUri(), "ttl-seconds", Integer.toString(config.redisTtlSeconds()),
                "max-object-bytes", Long.toString(config.redisMaxObjectBytes()), "key-prefix", "",
                "write-policy", "replace");
    }

    private static Map<String, String> s3Options(RepoServiceConfig config) {
        var options = new HashMap<String, String>();
        options.put("endpoint", config.s3Endpoint() == null ? "" : config.s3Endpoint());
        options.put("region", config.s3Region());
        options.put("path-style", Boolean.toString(config.s3Endpoint() != null));
        options.put("conditional-writes", Boolean.toString(config.s3ConditionalWrites()));
        options.put("credentials-mode", config.hasStaticCredentials() ? "static" : "default-chain");
        // The host chooses its S3 time budget explicitly; the provider has no defaults.
        options.put("api-call-timeout-ms", "300000");
        options.put("api-attempt-timeout-ms", "60000");
        options.put("connection-timeout-ms", "10000");
        options.put("socket-timeout-ms", "60000");
        if (config.hasStaticCredentials()) {
            options.put("access-key", config.s3AccessKey());
            options.put("secret-key", config.s3SecretKey());
        }
        return options;
    }
}
