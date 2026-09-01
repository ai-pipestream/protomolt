package ai.pipestream.proto.repo.service;

import ai.pipestream.proto.kafka.wire.ConfluentWireFormat;
import ai.pipestream.proto.registry.CompatibilityWriteGate;
import ai.pipestream.proto.registry.InMemorySchemaRegistryStore;
import ai.pipestream.proto.registry.service.SchemaRegistryServer;
import ai.pipestream.proto.registry.service.SchemaRegistryServerConfig;
import ai.pipestream.proto.repo.container.ledger.DocumentEventRecord;
import ai.pipestream.proto.repo.container.ledger.LedgerConfig;
import ai.pipestream.proto.repo.v1.Blob;
import ai.pipestream.proto.repo.v1.BlobBag;
import ai.pipestream.proto.repo.v1.CreateDriveRequest;
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
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
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

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The end-to-end interop cherry: the repo service relays a real {@code DocumentSaved} event and
 * a standard Confluent consumer ({@link KafkaProtobufDeserializer}) reads it.
 *
 * <p>What this proves, and what it pins down as the gap:</p>
 * <ul>
 *   <li>The bytes the shipped relay writes are the Confluent wire format exactly: magic byte,
 *     message index, and a payload byte-identical to what the reference serializer emits for the
 *     same message.</li>
 *   <li>The shipped relay is pinned and registry-free, so its frames carry schema id 0
 *     ({@code use.schema.id}'s default). Id 0 names no schema in any registry: Confluent's
 *     deserializer always resolves the frame's id first, so as shipped the records are readable
 *     by protomolt consumers (which read the payload against the packaged contract) but not by
 *     standard tooling.</li>
 *   <li>The remedy is configuration, shipped as {@code DOCUMENT_PLATFORM_SCHEMA_REGISTRY_URL}:
 *     a service booted with it relays through the same serde pointed at the registry, frames
 *     carry the registry-assigned id, and the reference consumer reads the relayed event through
 *     our own {@link SchemaRegistryServer}. The second test boots {@link RepoServices} with
 *     exactly that config, so the shipped wiring ({@code RepoServiceConfig} ->
 *     {@code RepoServices} -> {@code EventRelay.newProducer}) is exercised end to end.</li>
 * </ul>
 */
@Testcontainers(disabledWithoutDocker = true)
class KafkaEventingConfluentInteropIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    // Same baseline image as the other broker lanes (testcontainers' own pinned tag).
    @Container
    static final RedpandaContainer REDPANDA = new RedpandaContainer(
            DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v22.2.1"));

    static final String TOPIC =
            "document-events-confluent-it-" + Long.toUnsignedString(System.nanoTime(), 36);

    /**
     * Confluent registers the non-well-known import as a referenced subject named by the import
     * path; well-known types ({@code google/protobuf/*}) never become subjects.
     */
    private static final String ADDRESS_SUBJECT = "ai/pipestream/proto/repo/v1/address.proto";
    /** DocumentEvent is the fifth top-level message in document_events.proto. */
    private static final List<Integer> DOCUMENT_EVENT_INDEX = List.of(4);
    /** 5-byte frame prefix: magic byte plus the 4-byte schema id. */
    private static final int FRAME_PREFIX_BYTES = 5;

    private static final HttpClient HTTP = HttpClient.newHttpClient();

    static RepoServices services;
    static ManagedChannel channel;
    static DocumentServiceGrpc.DocumentServiceBlockingStub documents;
    static DriveServiceGrpc.DriveServiceBlockingStub drives;

    /** The same stack booted with {@code DOCUMENT_PLATFORM_SCHEMA_REGISTRY_URL} set. */
    static RepoServices registryServices;
    static ManagedChannel registryChannel;
    static DocumentServiceGrpc.DocumentServiceBlockingStub registryDocuments;
    static DriveServiceGrpc.DriveServiceBlockingStub registryDrives;

    static InMemorySchemaRegistryStore store;
    static SchemaRegistryServer registryServer;
    static String registryUrl;

    @BeforeAll
    static void boot() {
        store = new InMemorySchemaRegistryStore(new CompatibilityWriteGate());
        registryServer = new SchemaRegistryServer(
                SchemaRegistryServerConfig.defaults().withHost("127.0.0.1").withPort(0), store);
        registryServer.start();
        registryUrl = "http://127.0.0.1:" + registryServer.actualPort();

        services = RepoServices.build(serviceConfig(null));
        services.startInProcess("kafka-eventing-confluent-it");
        channel = InProcessChannelBuilder.forName("kafka-eventing-confluent-it").build();
        documents = DocumentServiceGrpc.newBlockingStub(channel);
        drives = DriveServiceGrpc.newBlockingStub(channel);

        registryServices = RepoServices.build(serviceConfig(registryUrl));
        registryServices.startInProcess("kafka-eventing-confluent-it-registry");
        registryChannel = InProcessChannelBuilder
                .forName("kafka-eventing-confluent-it-registry").build();
        registryDocuments = DocumentServiceGrpc.newBlockingStub(registryChannel);
        registryDrives = DriveServiceGrpc.newBlockingStub(registryChannel);
    }

    private static RepoServiceConfig serviceConfig(String schemaRegistryUrl) {
        return new RepoServiceConfig(
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
                REDPANDA.getBootstrapServers(), TOPIC, schemaRegistryUrl);
    }

    @AfterAll
    static void tearDown() {
        registryChannel.shutdownNow();
        registryServices.close();
        channel.shutdownNow();
        services.close();
        registryServer.close();
    }

    /**
     * The shipped producer, exactly as configured by {@code EventRelay.newProducer}: the frame
     * is the Confluent wire format byte for byte after the id, but the id is 0, which no
     * registry can resolve - the gap standard tooling hits as shipped.
     */
    @Test
    void theShippedRelayWritesConfluentFramesWithAnUnresolvableId() throws Exception {
        String topic = TOPIC + "-shipped";
        String account = "acct-confluent-shipped";
        createDrive(drives, "confluent-shipped", account);
        save(documents, "doc-confluent-shipped", account, "confluent-shipped");
        DocumentEventRecord row = pendingFor(services, "doc-confluent-shipped");
        DocumentEvent event = parse(row);

        int published = services.eventRelay().relayOnce(services.eventProducer(), topic, 100);
        assertThat(published).isGreaterThanOrEqualTo(1);

        byte[] frame = pollRaw(topic, "doc-confluent-shipped");
        assertThat(ConfluentWireFormat.schemaId(frame))
                .as("the pinned, registry-free producer stamps use.schema.id's default")
                .isEqualTo(0);
        assertThat(ConfluentWireFormat.messageIndex(frame)).isEqualTo(DOCUMENT_EVENT_INDEX);
        assertThat(DocumentEvent.parseFrom(ConfluentWireFormat.payload(frame))).isEqualTo(event);

        // Everything after the 5-byte prefix is byte-identical to the reference serializer's
        // frame for the same record; only the stamped id differs. (This call also registers the
        // DocumentEvent subject against our registry, the way production would.)
        try (KafkaProtobufSerializer<DocumentEvent> reference = confluentSerializer()) {
            byte[] referenceFrame = reference.serialize(topic, event);
            assertThat(ConfluentWireFormat.messageIndex(referenceFrame))
                    .isEqualTo(DOCUMENT_EVENT_INDEX);
            assertThat(Arrays.copyOfRange(frame, FRAME_PREFIX_BYTES, frame.length))
                    .isEqualTo(Arrays.copyOfRange(referenceFrame, FRAME_PREFIX_BYTES,
                            referenceFrame.length));
        }
        assertThat(store.latest(topic + "-value")).isPresent();

        // And still the record is unreadable by the reference consumer: it resolves the frame's
        // id first, and id 0 names no schema. This is the finding, pinned: the remedy is the
        // registry-configured topology proven by the next test.
        try (KafkaProtobufDeserializer<DocumentEvent> standard = confluentDeserializer()) {
            assertThatThrownBy(() -> standard.deserialize(topic, frame))
                    .isInstanceOf(SerializationException.class)
                    .hasMessageContaining("Error retrieving Protobuf schema for id")
                    .hasMessageContaining("id=0");
        }
    }

    /**
     * The production remedy through the shipped wiring: a service booted with
     * {@code DOCUMENT_PLATFORM_SCHEMA_REGISTRY_URL} set, the DocumentEvent subject registered
     * against our registry server, and a standard Confluent consumer reconstructing the relayed
     * event - no hand-built producer anywhere.
     */
    @Test
    void aRegistryConfiguredRelayIsReadableByStandardTooling() throws Exception {
        String topic = TOPIC + "-registered";
        registerDocumentEventSubject(topic);
        String account = "acct-confluent-registered";
        createDrive(registryDrives, "confluent-registered", account);
        save(registryDocuments, "doc-confluent-registered", account, "confluent-registered");
        DocumentEventRecord row = pendingFor(registryServices, "doc-confluent-registered");
        DocumentEvent event = parse(row);

        int published = registryServices.eventRelay()
                .relayOnce(registryServices.eventProducer(), topic, 100);
        assertThat(published).isGreaterThanOrEqualTo(1);

        // The shipped producer stamped the registry's id, not 0.
        byte[] frame = pollRaw(topic, "doc-confluent-registered");
        assertThat(ConfluentWireFormat.schemaId(frame))
                .isEqualTo(store.latest(topic + "-value").orElseThrow().globalId());
        assertThat(ConfluentWireFormat.messageIndex(frame)).isEqualTo(DOCUMENT_EVENT_INDEX);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, REDPANDA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "kafka-eventing-confluent-it-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (KafkaConsumer<String, DocumentEvent> consumer = new KafkaConsumer<>(props,
                new StringDeserializer(), confluentDeserializer())) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            while (System.nanoTime() < deadline) {
                ConsumerRecords<String, DocumentEvent> records =
                        consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, DocumentEvent> record : records) {
                    if ("doc-confluent-registered".equals(record.key())) {
                        assertThat(record.value()).isEqualTo(event);
                        return;
                    }
                }
            }
            throw new AssertionError("no DocumentSaved for doc-confluent-registered within 60s");
        }
    }

    // ------------------------------------------------------------------ serdes and registration

    private static KafkaProtobufSerializer<DocumentEvent> confluentSerializer() {
        KafkaProtobufSerializer<DocumentEvent> serializer = new KafkaProtobufSerializer<>();
        serializer.configure(Map.of(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl), false);
        return serializer;
    }

    private static KafkaProtobufDeserializer<DocumentEvent> confluentDeserializer() {
        KafkaProtobufDeserializer<DocumentEvent> deserializer = new KafkaProtobufDeserializer<>();
        Map<String, Object> config = new HashMap<>();
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
        config.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE,
                DocumentEvent.class.getName());
        deserializer.configure(config, false);
        return deserializer;
    }

    /** The DocumentEvent subject under the topic-name strategy, with the import reference. */
    private static void registerDocumentEventSubject(String topic) throws Exception {
        register(ADDRESS_SUBJECT,
                new ProtobufSchema(NodeAddress.getDescriptor()).canonicalString(), "");
        register(topic + "-value",
                new ProtobufSchema(DocumentEvent.getDescriptor()).canonicalString(),
                ",\"references\":[" + referenceJson(ADDRESS_SUBJECT) + "]}");
    }

    private static String referenceJson(String importPath) {
        return "{\"name\":\"" + importPath + "\",\"subject\":\"" + importPath
                + "\",\"version\":1}";
    }

    private static void register(String subject, String schema, String referencesJson)
            throws Exception {
        String encoded = URLEncoder.encode(subject, StandardCharsets.UTF_8).replace("+", "%20");
        String body = "{\"schemaType\":\"PROTOBUF\",\"schema\":" + quote(schema)
                + (referencesJson.isEmpty() ? "}" : referencesJson);
        HttpResponse<String> response = HTTP.send(HttpRequest
                .newBuilder(URI.create(registryUrl + "/subjects/" + encoded + "/versions"))
                .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode())
                .as("registering %s: %s", subject, response.body())
                .isEqualTo(200);
    }

    private static String quote(String text) {
        StringBuilder out = new StringBuilder("\"");
        for (char c : text.toCharArray()) {
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                default -> out.append(c);
            }
        }
        return out.append('"').toString();
    }

    // ------------------------------------------------------------------ service fixtures

    private static void createDrive(DriveServiceGrpc.DriveServiceBlockingStub drives,
            String name, String accountId) {
        drives.createDrive(CreateDriveRequest.newBuilder()
                .setName(name)
                .setAccountId(accountId)
                .setDriveType(DriveType.DRIVE_TYPE_INTAKE)
                .build());
    }

    private static SaveDocumentResponse save(
            DocumentServiceGrpc.DocumentServiceBlockingStub documents,
            String docId, String accountId, String drive) {
        return documents.saveDocument(SaveDocumentRequest.newBuilder()
                .setDocument(fixture(docId, accountId))
                .setDrive(drive)
                .setUseDatasourceId(true)
                .setGraphId("intake:" + accountId)
                .build());
    }

    /** The PENDING outbox row for {@code docId} (any event type). */
    private static DocumentEventRecord pendingFor(RepoServices services, String docId) {
        return services.eventOutbox().claimBatch(1000).stream()
                .filter(r -> r.kafkaKey.equals(docId))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no PENDING outbox row for " + docId));
    }

    private static DocumentEvent parse(DocumentEventRecord record) {
        try {
            return DocumentEvent.parseFrom(record.payload);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new AssertionError("outbox payload did not parse", e);
        }
    }

    /** The raw frame the relay published for {@code docId}. */
    private static byte[] pollRaw(String topic, String docId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, REDPANDA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "kafka-eventing-confluent-it-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props,
                new StringDeserializer(), new ByteArrayDeserializer())) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            while (System.nanoTime() < deadline) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, byte[]> record : records) {
                    if (docId.equals(record.key())) {
                        return record.value();
                    }
                }
            }
            throw new AssertionError("no frame for " + docId + " on " + topic + " within 60s");
        }
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
}
