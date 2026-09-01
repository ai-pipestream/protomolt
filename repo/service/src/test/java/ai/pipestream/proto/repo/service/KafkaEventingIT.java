package ai.pipestream.proto.repo.service;

import ai.pipestream.proto.kafka.serde.ProtoMoltProtobufDeserializer;
import ai.pipestream.proto.kafka.serde.ProtoMoltSerdeConfig;
import ai.pipestream.proto.repo.container.ledger.DocumentEventRecord;
import ai.pipestream.proto.repo.container.ledger.LedgerConfig;
import ai.pipestream.proto.repo.container.lifecycle.DocumentEventFactory;
import ai.pipestream.proto.repo.v1.Blob;
import ai.pipestream.proto.repo.v1.BlobBag;
import ai.pipestream.proto.repo.v1.CreateDriveRequest;
import ai.pipestream.proto.repo.v1.DeleteDocumentByReferenceCommand;
import ai.pipestream.proto.repo.v1.DeleteDocumentRequest;
import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.repo.v1.DocumentEvent;
import ai.pipestream.proto.repo.v1.DocumentServiceGrpc;
import ai.pipestream.proto.repo.v1.DriveServiceGrpc;
import ai.pipestream.proto.repo.v1.DriveType;
import ai.pipestream.proto.repo.v1.NodeAddress;
import ai.pipestream.proto.repo.v1.OwnershipContext;
import ai.pipestream.proto.repo.v1.SaveDocumentRequest;
import ai.pipestream.proto.repo.v1.SaveDocumentResponse;
import ai.pipestream.proto.repo.v1.SearchMetadata;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Kafka eventing end to end: the full service stack booted through
 * {@link RepoServices} against real testcontainers PostgreSQL 17, LocalStack
 * S3 and Redpanda, with {@code DOCUMENT_PLATFORM_KAFKA_BOOTSTRAP_SERVERS}
 * set. Proves the outbox writes land IN THE SAME TRANSACTION as the save /
 * delete commit points (a failed save leaves no row), and that the relay
 * publishes them to the topic where a consumer using
 * {@link ProtoMoltProtobufDeserializer} reads and revalidates them.
 */
@Testcontainers(disabledWithoutDocker = true)
class KafkaEventingIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    // Same baseline image as the serde lane (testcontainers' own pinned tag).
    @Container
    static final RedpandaContainer REDPANDA = new RedpandaContainer(
            DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v22.2.1"));

    static final String TOPIC = "document-events-it-" + Long.toUnsignedString(System.nanoTime(), 36);

    static RepoServices services;
    static ManagedChannel channel;
    static DocumentServiceGrpc.DocumentServiceBlockingStub documents;
    static DriveServiceGrpc.DriveServiceBlockingStub drives;

    @BeforeAll
    static void boot() {
        RepoServiceConfig config = new RepoServiceConfig(
                0, // unused on the in-process transport
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                LOCALSTACK.getEndpoint().toString(),
                LOCALSTACK.getRegion(),
                LOCALSTACK.getAccessKey(),
                LOCALSTACK.getSecretKey(),
                "it-docs",
                0, // HTTP upload route not exercised by this IT
                null, null, null, null, 0, 0L, // blob store: the default direct-S3 path
                true, 5000L, 60000L, false, true, 3600000L, // lifecycle defaults
                REDPANDA.getBootstrapServers(), TOPIC);
        services = RepoServices.build(config);
        services.startInProcess("kafka-eventing-it");
        channel = InProcessChannelBuilder.forName("kafka-eventing-it").build();
        documents = DocumentServiceGrpc.newBlockingStub(channel);
        drives = DriveServiceGrpc.newBlockingStub(channel);
    }

    @AfterAll
    static void tearDown() {
        channel.shutdownNow();
        services.close();
    }

    @Test
    void saveWritesDocumentSavedAtomically() {
        String account = "acct-kafka-save";
        createDrive("kafka-save", account);
        SaveDocumentResponse saved = save("doc-kafka-saved", account, "kafka-save");

        DocumentEventRecord row = pendingFor("doc-kafka-saved");
        assertThat(row.eventType).isEqualTo(DocumentEventRecord.TYPE_SAVED);
        assertThat(row.kafkaKey).isEqualTo("doc-kafka-saved");
        DocumentEvent event = parse(row);
        assertThat(event.getEventId()).isEqualTo(row.eventId.toString());
        assertThat(event.getSaved().getAddress())
                .isEqualTo(address("doc-kafka-saved", "ds-1", account, "intake:" + account));
        assertThat(event.getSaved().getChecksum()).isEqualTo(saved.getChecksum());
        assertThat(event.getSaved().getSizeBytes()).isEqualTo(saved.getSizeBytes());
        assertThat(event.getSaved().getDocVersion()).isEqualTo(1);

        // A failed save commits nothing: no document row, no outbox row. The
        // unknown drive fails before any ledger write, so the outbox must
        // carry no event for the rejected document (the passing save's event
        // above was already asserted present).
        assertThatThrownBy(() -> documents.saveDocument(SaveDocumentRequest.newBuilder()
                .setDocument(fixture("doc-kafka-nosave", account))
                .setDrive("no-such-drive")
                .setUseDatasourceId(true)
                .setGraphId("intake:" + account)
                .build()))
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(e -> assertThat(((StatusRuntimeException) e).getStatus().getCode())
                        .isEqualTo(Status.Code.NOT_FOUND));
        assertThat(services.eventOutbox().claimBatch(1000))
                .noneMatch(r -> r.kafkaKey.equals("doc-kafka-nosave"));
    }

    @Test
    void deleteWritesPurgeRequestedAndDocumentDeleted() {
        String account = "acct-kafka-delete";
        createDrive("kafka-delete", account);
        save("doc-kafka-tombstoned", account, "kafka-delete");
        save("doc-kafka-harddeleted", account, "kafka-delete");

        // Soft delete (purge_storage=false): tombstone + purge record +
        // PurgeRequested, one transaction.
        documents.deleteDocument(DeleteDocumentRequest.newBuilder()
                .setByReference(DeleteDocumentByReferenceCommand.newBuilder()
                        .setAddress(address("doc-kafka-tombstoned", "ds-1", account,
                                "intake:" + account)))
                .build());
        DocumentEventRecord requested = pendingFor("doc-kafka-tombstoned", "PurgeRequested");
        DocumentEvent purgeRequested = parse(requested);
        assertThat(purgeRequested.getPurgeRequested().getPurgeId()).isNotBlank();
        assertThat(purgeRequested.getPurgeRequested().getAddress().getDocId())
                .isEqualTo("doc-kafka-tombstoned");
        assertThat(purgeRequested.getPurgeRequested().getRequestedAt().getSeconds()).isPositive();

        // Hard delete (purge_storage=true): row removal + DocumentDeleted,
        // one transaction.
        documents.deleteDocument(DeleteDocumentRequest.newBuilder()
                .setByReference(DeleteDocumentByReferenceCommand.newBuilder()
                        .setAddress(address("doc-kafka-harddeleted", "ds-1", account,
                                "intake:" + account)))
                .setPurgeStorage(true)
                .build());
        DocumentEvent deleted = parse(pendingFor("doc-kafka-harddeleted", "DocumentDeleted"));
        assertThat(deleted.getDeleted().getAddress().getDocId()).isEqualTo("doc-kafka-harddeleted");
        assertThat(deleted.getDeleted().getDeletedAt().getSeconds()).isPositive();
    }

    @Test
    void relayPublishesToKafkaAndTheConsumerRevalidates() {
        String account = "acct-kafka-relay";
        createDrive("kafka-relay", account);
        SaveDocumentResponse saved = save("doc-kafka-published", account, "kafka-relay");
        DocumentEventRecord row = pendingFor("doc-kafka-published");

        int published = services.eventRelay().relayOnce(services.eventProducer(), TOPIC, 100);
        assertThat(published).isGreaterThanOrEqualTo(1);
        DocumentEventRecord after = services.eventOutbox().findById(row.eventId).orElseThrow();
        assertThat(after.status).isEqualTo(DocumentEventRecord.STATUS_PUBLISHED);
        assertThat(after.publishedAt).isNotNull();

        try (KafkaConsumer<String, Message> consumer = newConsumer()) {
            consumer.subscribe(List.of(TOPIC));
            DocumentEvent event = pollFor(consumer, "doc-kafka-published");
            assertThat(event.getEventId()).isEqualTo(row.eventId.toString());
            assertThat(event.getSaved().getAddress().getDocId()).isEqualTo("doc-kafka-published");
            assertThat(event.getSaved().getChecksum()).isEqualTo(saved.getChecksum());
        }
    }

    // ------------------------------------------------------------------ helpers

    private static void createDrive(String name, String accountId) {
        drives.createDrive(CreateDriveRequest.newBuilder()
                .setName(name)
                .setAccountId(accountId)
                .setDriveType(DriveType.DRIVE_TYPE_INTAKE)
                .build());
    }

    private static SaveDocumentResponse save(String docId, String accountId, String drive) {
        return documents.saveDocument(SaveDocumentRequest.newBuilder()
                .setDocument(fixture(docId, accountId))
                .setDrive(drive)
                .setUseDatasourceId(true)
                .setGraphId("intake:" + accountId)
                .build());
    }

    /** The PENDING outbox row for {@code docId} (any event type). */
    private static DocumentEventRecord pendingFor(String docId) {
        return services.eventOutbox().claimBatch(1000).stream()
                .filter(r -> r.kafkaKey.equals(docId))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no PENDING outbox row for " + docId));
    }

    /** The PENDING outbox row of one event type for {@code docId}. */
    private static DocumentEventRecord pendingFor(String docId, String eventType) {
        return services.eventOutbox().claimBatch(1000).stream()
                .filter(r -> r.kafkaKey.equals(docId) && r.eventType.equals(eventType))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "no PENDING " + eventType + " outbox row for " + docId));
    }

    private static DocumentEvent parse(DocumentEventRecord record) {
        try {
            return DocumentEvent.parseFrom(record.payload);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new AssertionError("outbox payload did not parse", e);
        }
    }

    /** A consumer pinned to DocumentEvent that revalidates every record on read. */
    private static KafkaConsumer<String, Message> newConsumer() {
        ProtoMoltProtobufDeserializer deserializer = new ProtoMoltProtobufDeserializer();
        deserializer.configure(Map.of(
                ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64, DocumentEventFactory.descriptorSetBase64(),
                ProtoMoltSerdeConfig.MESSAGE_TYPE, DocumentEvent.getDescriptor().getFullName(),
                ProtoMoltSerdeConfig.VALIDATE_ON_READ, true), false);
        Properties props = new Properties();
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                REDPANDA.getBootstrapServers());
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG,
                "kafka-eventing-it-" + UUID.randomUUID());
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest");
        return new KafkaConsumer<>(props, new StringDeserializer(), deserializer);
    }

    /** Poll until the DocumentSaved for {@code docId} shows up (the topic is
     *  shared across this class's tests; other documents' records are skipped). */
    private static DocumentEvent pollFor(KafkaConsumer<String, Message> consumer, String docId) {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            ConsumerRecords<String, Message> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, Message> record : records) {
                Message value = record.value();
                assertThat(value).isInstanceOf(DocumentEvent.class);
                DocumentEvent event = (DocumentEvent) value;
                if (event.hasSaved() && event.getSaved().getAddress().getDocId().equals(docId)) {
                    // The doc_id is the record key: partition-ordered per document.
                    assertThat(record.key()).isEqualTo(docId);
                    return event;
                }
            }
        }
        throw new AssertionError("no DocumentSaved for " + docId + " within 60s");
    }

    private static Document fixture(String docId, String accountId) {
        return Document.newBuilder()
                .setDocId(docId)
                .setOwnership(OwnershipContext.newBuilder()
                        .setAccountId(accountId)
                        .setDatasourceId("ds-1"))
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle("Eventing fixture")
                        .setBody("body of " + docId))
                .setBlobBag(BlobBag.newBuilder().setBlob(Blob.newBuilder()
                        .setBlobId("blob-1")
                        .setData(ByteString.copyFromUtf8("raw-bytes-of-" + docId))))
                .build();
    }

    private static NodeAddress address(String docId, String graphAddressId,
            String accountId, String graphId) {
        return NodeAddress.newBuilder()
                .setDocId(docId)
                .setGraphAddressId(graphAddressId)
                .setAccountId(accountId)
                .setGraphId(graphId)
                .build();
    }
}
