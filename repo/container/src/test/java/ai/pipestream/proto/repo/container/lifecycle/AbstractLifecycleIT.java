package ai.pipestream.proto.repo.container.lifecycle;

import ai.pipestream.proto.repo.container.blob.BlobStore;
import ai.pipestream.proto.repo.container.blob.S3BlobStore;
import ai.pipestream.proto.repo.container.ledger.DocumentLedger;
import ai.pipestream.proto.repo.container.ledger.DocumentPurgeRecord;
import ai.pipestream.proto.repo.container.ledger.DocumentRecord;
import ai.pipestream.proto.repo.container.ledger.DocumentRowKind;
import ai.pipestream.proto.repo.container.ledger.DriveLedger;
import ai.pipestream.proto.repo.container.ledger.DriveRecord;
import ai.pipestream.proto.repo.container.ledger.LedgerConfig;
import ai.pipestream.proto.repo.container.ledger.LedgerDatabase;
import ai.pipestream.proto.repo.container.ledger.Tx;
import ai.pipestream.proto.repo.v1.DocumentManifest;
import ai.pipestream.proto.repo.v1.DocumentPart;
import ai.pipestream.proto.repo.v1.NodeAddress;
import ai.pipestream.proto.repo.v1.PartManifestEntry;
import ai.pipestream.proto.repo.v1.PartState;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Shared harness for the lifecycle ITs: real testcontainers PostgreSQL 17
 * (Flyway-migrated ledger) and LocalStack S3, with the ledgers, purge queue
 * and lifecycle workers wired exactly as the service wires them. Subclasses
 * get {@code @Testcontainers} semantics from their own annotation; the
 * containers here are static and shared per subclass.
 */
abstract class AbstractLifecycleIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    static LedgerDatabase database;
    static Tx tx;
    static DocumentLedger documents;
    static DriveLedger drives;
    static JdbcPurgeQueue queue;
    static S3Client client;
    static S3BlobStore store;
    static S3Purger purger;
    static PurgeSweeper sweeper;
    static StorageReconciler reconciler;
    static CoherenceProbe probe;

    @BeforeAll
    static void bootLifecycle() {
        database = new LedgerDatabase(new LedgerConfig(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory());
        documents = new DocumentLedger(tx);
        drives = new DriveLedger(tx);
        queue = new JdbcPurgeQueue(tx);
        client = S3Client.builder()
                .endpointOverride(LOCALSTACK.getEndpoint())
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials
                        .create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
                .region(Region.of(LOCALSTACK.getRegion()))
                .httpClient(UrlConnectionHttpClient.create())
                .forcePathStyle(true)
                .build();
        store = new S3BlobStore(client);
        purger = new S3Purger(tx, documents, drives, queue);
        sweeper = new PurgeSweeper(tx, documents, drives, queue);
        reconciler = new StorageReconciler(documents);
        probe = new CoherenceProbe(documents, drives);
    }

    @AfterAll
    static void stopLifecycle() {
        client.close();
        database.close();
    }

    /** Insert a drive row AND create its bucket. */
    static DriveRecord createDrive(String accountId, String name, String bucket, String prefix) {
        DriveRecord drive = new DriveRecord();
        drive.driveId = UUID.randomUUID();
        drive.accountId = accountId;
        drive.name = name;
        drive.driveType = "INTAKE";
        drive.bucket = bucket;
        drive.prefix = prefix;
        drives.insert(drive);
        client.createBucket(b -> b.bucket(bucket));
        return drive;
    }

    /** An INTAKE document row (unsaved) with a two-entry manifest. */
    static DocumentRecord intakeRow(UUID nodeId, String accountId, String docId,
            String datasourceId, String driveName, List<String> partKeys) {
        DocumentRecord record = new DocumentRecord();
        record.nodeId = nodeId;
        record.docId = docId;
        record.graphAddressId = datasourceId;
        record.graphId = "intake:" + accountId;
        record.rowKind = DocumentRowKind.INTAKE;
        record.accountId = accountId;
        record.datasourceId = datasourceId;
        record.checksum = "sha256:" + UUID.randomUUID();
        record.driveName = driveName;
        record.objectKey = "documents/" + accountId + "/" + nodeId;
        record.etag = "etag-" + docId;
        record.sizeBytes = 1234L;
        DocumentManifest.Builder manifest = DocumentManifest.newBuilder()
                .setAddress(NodeAddress.newBuilder()
                        .setDocId(docId)
                        .setGraphAddressId(datasourceId)
                        .setAccountId(accountId)
                        .setGraphId(record.graphId))
                .setDocVersion(1);
        for (String key : partKeys) {
            manifest.addParts(PartManifestEntry.newBuilder()
                    .setPart(DocumentPart.DOCUMENT_PART_CHUNKS)
                    .setState(PartState.PART_STATE_PRESENT)
                    .setObjectKey(key)
                    .setSubKey("set-" + key.hashCode())
                    .setSha256("sha256:" + key.hashCode())
                    .setSizeBytes(10));
        }
        record.writeManifest(manifest.build());
        return record;
    }

    /** Enqueue a purge record for {@code row} (snapshot from its manifest), in one tx. */
    static DocumentPurgeRecord enqueuePurge(DocumentRecord row, String drivePrefix,
            Instant requestedAt) {
        DocumentPurgeRecord record = new DocumentPurgeRecord();
        record.purgeId = UUID.randomUUID();
        record.nodeId = row.nodeId;
        record.docId = row.docId;
        record.graphAddressId = row.graphAddressId;
        record.accountId = row.accountId;
        record.graphId = row.graphId;
        record.driveName = row.driveName;
        record.writeObjectKeys(PurgeSnapshots.objectKeysOf(row, drivePrefix));
        record.requestedAt = requestedAt;
        tx.inTransaction(em -> {
            queue.enqueue(em, record);
        });
        return record;
    }

    static Optional<DocumentPurgeRecord> findPurge(UUID purgeId) {
        return tx.readOnly(em -> Optional.ofNullable(em.find(DocumentPurgeRecord.class, purgeId)));
    }

    static void putObject(String bucket, String key) {
        store.put(new BlobStore.PutSpec(bucket, key, "application/octet-stream", null, null),
                ("body-of-" + key).getBytes(StandardCharsets.UTF_8));
    }

    static boolean objectExists(String bucket, String key) {
        try {
            store.headObject(bucket, key);
            return true;
        } catch (BlobStore.BlobNotFoundException e) {
            return false;
        }
    }

    static Map<String, Long> queueCounts() {
        return queue.countByStatus();
    }
}
