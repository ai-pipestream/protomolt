package ai.protomolt.proto.kafka.serde;

import ai.protomolt.proto.kafka.wire.ConfluentWireFormat;
import ai.protomolt.proto.registry.CompatibilityWriteGate;
import ai.protomolt.proto.registry.InMemorySchemaRegistryStore;
import ai.protomolt.proto.registry.SchemaReference;
import ai.protomolt.proto.registry.StoredSchema;
import ai.protomolt.proto.registry.service.SchemaRegistryServer;
import ai.protomolt.proto.registry.service.SchemaRegistryServerConfig;
import ai.protomolt.proto.repo.v1.DocumentEvent;
import ai.protomolt.proto.repo.v1.DocumentSaved;
import ai.protomolt.proto.repo.v1.NodeAddress;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The interop claim, proven in all four directions on a real broker against our own registry
 * server: protomolt's serde and Confluent's reference serde
 * ({@code io.confluent:kafka-protobuf-serializer}) write byte-identical frames, each reads what
 * the other writes, and Confluent's client auto-registers against
 * {@link SchemaRegistryServer} the way it would against Confluent's own.
 *
 * <p>The schema is deliberately the nasty one the repo service actually ships:
 * {@link DocumentEvent}, a multi-message file with a oneof wrapper and two imports. The
 * Confluent client registers the non-well-known import as a referenced subject
 * ({@code ai/pipestream/proto/repo/v1/address.proto}); the well-known
 * {@code google/protobuf/timestamp.proto} never becomes a subject, on either side. The wrapper
 * is the fifth message in its file, so every frame here carries the non-trivial message index
 * {@code [4]}.</p>
 *
 * <p>The registry is a {@link SchemaRegistryServer} on an ephemeral port over an
 * {@link InMemorySchemaRegistryStore} with the {@link CompatibilityWriteGate} armed; the broker
 * is a Testcontainers Redpanda. The suite skips when Docker is unavailable.</p>
 */
@Testcontainers(disabledWithoutDocker = true)
class SerdeConfluentInteropIntegrationTest {

    // Same baseline image as the other broker lanes (testcontainers' own pinned tag).
    @Container
    static final RedpandaContainer REDPANDA = new RedpandaContainer(
            DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v22.2.1"));

    private static final String EVENT_TYPE = "ai.pipestream.proto.repo.v1.DocumentEvent";
    /**
     * Confluent registers non-well-known imports as referenced subjects named by the import
     * path. Well-known types ({@code google/protobuf/*}) never become subjects: the reference
     * client resolves them from its bundled copy, and our compiler does the same.
     */
    private static final String TIMESTAMP_SUBJECT = "google/protobuf/timestamp.proto";
    private static final String ADDRESS_SUBJECT = "ai/pipestream/proto/repo/v1/address.proto";
    /** DocumentEvent is the fifth top-level message in document_events.proto. */
    private static final List<Integer> DOCUMENT_EVENT_INDEX = List.of(4);

    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private static final String RUN = Long.toUnsignedString(System.nanoTime(), 36);

    private static InMemorySchemaRegistryStore store;
    private static SchemaRegistryServer registryServer;
    private static String registryUrl;
    private static String descriptorSetBase64;
    private static String addressText;
    private static String documentEventText;
    private static boolean importSubjectsRegistered;

    /** Wire-breaking v2 of DocumentEvent: field 1 keeps its number but changes type. */
    private static final String GATE_V2_BREAKING = """
            syntax = "proto3";
            package ai.pipestream.proto.repo.v1;
            import "ai/pipestream/proto/repo/v1/address.proto";
            import "google/protobuf/timestamp.proto";
            option java_multiple_files = true;
            option java_package = "ai.pipestream.proto.repo.v1";
            message DocumentSaved {
              NodeAddress address = 1;
              string checksum = 2;
              int64 doc_version = 3;
              int64 size_bytes = 4;
              google.protobuf.Timestamp saved_at = 5;
              map<string, string> metadata = 99;
            }
            message DocumentDeleted {
              NodeAddress address = 1;
              string checksum = 2;
              google.protobuf.Timestamp deleted_at = 3;
              map<string, string> metadata = 99;
            }
            message PurgeRequested {
              NodeAddress address = 1;
              string purge_id = 2;
              string checksum = 3;
              google.protobuf.Timestamp requested_at = 4;
              map<string, string> metadata = 99;
            }
            message DocumentPurged {
              NodeAddress address = 1;
              string purge_id = 2;
              string checksum = 3;
              google.protobuf.Timestamp purged_at = 4;
              map<string, string> metadata = 99;
            }
            message DocumentEvent {
              int64 event_id = 1;
              oneof event {
                DocumentSaved saved = 2;
                DocumentDeleted deleted = 3;
                PurgeRequested purge_requested = 4;
                DocumentPurged purged = 5;
              }
              map<string, string> metadata = 99;
            }
            """;

    /** Compatible evolution: a new oneof arm is a new field number; old readers skip it. */
    private static final String GATE_V3_COMPATIBLE = GATE_V2_BREAKING
            .replace("int64 event_id = 1;", "string event_id = 1;")
            .replace("DocumentPurged purged = 5;",
                    "DocumentPurged purged = 5;\n    DocumentArchived archived = 6;")
            + "\nmessage DocumentArchived {\n  string reason = 1;\n}\n";

    @BeforeAll
    static void boot() {
        store = new InMemorySchemaRegistryStore(new CompatibilityWriteGate());
        registryServer = new SchemaRegistryServer(
                SchemaRegistryServerConfig.defaults().withHost("127.0.0.1").withPort(0), store);
        registryServer.start();
        registryUrl = "http://127.0.0.1:" + registryServer.actualPort();
        descriptorSetBase64 = descriptorSetBase64();
        addressText = new ProtobufSchema(NodeAddress.getDescriptor()).canonicalString();
        documentEventText = new ProtobufSchema(DocumentEvent.getDescriptor()).canonicalString();
    }

    @AfterAll
    static void tearDown() {
        registryServer.close();
    }

    // ---------------------------------------------------------------- quadrants

    /**
     * Quadrant 1: protomolt writes, Confluent reads. The schema is registered out of band, the
     * way a production deployment publishes it; the protomolt producer stamps the registry's id
     * and the index of the type in the registry's file. The frame is asserted byte-identical to
     * what the reference serializer emits for the same record.
     */
    @Test
    void protoMoltWritesAndConfluentReads() throws Exception {
        String topic = topic("q1");
        registerDocumentEventSubject(topic);
        DocumentEvent event = sampleEvent("q1-doc");

        // Confluent goes first so a (re)registration on its side lands before protomolt looks
        // the subject up; with identical content the store dedupes to the same id either way.
        byte[] referenceFrame;
        try (var serializer = confluentSerializer()) {
            referenceFrame = serializer.serialize(topic, event);
        }
        byte[] protoMoltFrame;
        try (var serializer = protoMoltSerializer(true)) {
            protoMoltFrame = serializer.serialize(topic, event);
        }
        int registeredId = store.latest(topic + "-value").orElseThrow().globalId();
        assertThat(ConfluentWireFormat.schemaId(protoMoltFrame)).isEqualTo(registeredId);
        assertThat(ConfluentWireFormat.messageIndex(protoMoltFrame))
                .isEqualTo(DOCUMENT_EVENT_INDEX);
        assertThat(protoMoltFrame)
                .as("byte for byte what the reference serializer writes for the same record")
                .isEqualTo(referenceFrame);

        try (var serializer = protoMoltSerializer(true)) {
            produce(topic, "q1-doc", event, serializer);
        }
        try (var deserializer = confluentDeserializer()) {
            assertThat(consumeOne(topic, deserializer, "q1-doc")).isEqualTo(event);
        }
    }

    /**
     * Quadrant 2: Confluent writes (auto-registering against our server), protomolt reads
     * unpinned: the frame's id and index resolve to the type through the registry, and the
     * record comes back as the application's generated class.
     */
    @Test
    void confluentWritesAndProtoMoltReads() throws Exception {
        String topic = topic("q2");
        DocumentEvent event = sampleEvent("q2-doc");

        byte[] referenceFrame;
        try (var serializer = confluentSerializer()) {
            referenceFrame = serializer.serialize(topic, event);
        }
        int registeredId = store.latest(topic + "-value").orElseThrow().globalId();
        assertThat(ConfluentWireFormat.schemaId(referenceFrame)).isEqualTo(registeredId);
        assertThat(ConfluentWireFormat.messageIndex(referenceFrame))
                .isEqualTo(DOCUMENT_EVENT_INDEX);

        // Off the broker: our deserializer resolves the reference frame by id alone.
        try (var deserializer = protoMoltDeserializer(false)) {
            Message back = deserializer.deserialize(topic, referenceFrame);
            assertThat(back).isInstanceOf(DocumentEvent.class).isEqualTo(event);
        }

        try (var serializer = confluentSerializer()) {
            produce(topic, "q2-doc", event, serializer);
        }
        try (var deserializer = protoMoltDeserializer(false)) {
            Message back = consumeOne(topic, deserializer, "q2-doc");
            assertThat(back).isInstanceOf(DocumentEvent.class).isEqualTo(event);
        }
    }

    /**
     * Quadrant 3: the reference client end to end against our registry server: auto-registration,
     * referenced subjects for the imports, id assignment, and a Confluent consumer resolving the
     * frame back. The registration layout is asserted explicitly: the value subject holds one
     * version whose references name both import subjects at version 1, and the frame carries the
     * id the registry assigned.
     */
    @Test
    void confluentWritesAndConfluentReadsThroughOurRegistry() throws Exception {
        String topic = topic("q3");
        DocumentEvent event = sampleEvent("q3-doc");

        try (var serializer = confluentSerializer()) {
            produce(topic, "q3-doc", event, serializer);
        }
        try (var deserializer = confluentDeserializer()) {
            assertThat(consumeOne(topic, deserializer, "q3-doc")).isEqualTo(event);
        }

        assertThat(store.subjects())
                .contains(ADDRESS_SUBJECT, topic + "-value")
                .as("well-known imports are never registered as subjects")
                .doesNotContain(TIMESTAMP_SUBJECT);
        StoredSchema valueSchema = store.latest(topic + "-value").orElseThrow();
        assertThat(valueSchema.version()).isEqualTo(1);
        assertThat(valueSchema.references())
                .as("the timestamp import resolves from the bundled well-known types, "
                        + "so only address.proto is a reference")
                .containsExactly(new SchemaReference(ADDRESS_SUBJECT, ADDRESS_SUBJECT, 1));
        assertThat(valueSchema.globalId()).isPositive();

        // The id the reference client put in the frame is the id our registry assigned, and our
        // own deserializer resolves the same frame by that id.
        byte[] frame;
        try (var serializer = confluentSerializer()) {
            frame = serializer.serialize(topic, event);
        }
        assertThat(ConfluentWireFormat.schemaId(frame)).isEqualTo(valueSchema.globalId());
        try (var deserializer = protoMoltDeserializer(false)) {
            assertThat(deserializer.deserialize(topic, frame)).isEqualTo(event);
        }
    }

    /**
     * Quadrant 4: the production topology, protomolt at both ends with the registry configured:
     * the producer stamps the registry id, the consumer resolves it back, pinned to
     * DocumentEvent.
     */
    @Test
    void protoMoltWritesAndProtoMoltReadsWithTheRegistryConfigured() throws Exception {
        String topic = topic("q4");
        registerDocumentEventSubject(topic);
        DocumentEvent event = sampleEvent("q4-doc");

        try (var serializer = protoMoltSerializer(true)) {
            produce(topic, "q4-doc", event, serializer);
        }
        try (var deserializer = protoMoltDeserializer(true)) {
            Message back = consumeOne(topic, deserializer, "q4-doc");
            assertThat(back).isInstanceOf(DocumentEvent.class).isEqualTo(event);
        }
    }

    /**
     * The compatibility contract at the registry boundary: a wire-breaking v2 (field 1 keeps
     * its number but changes type) is refused with the Confluent-style 409, and a compatible
     * evolution (a new oneof arm under a fresh field number) is accepted as version 2.
     */
    @Test
    void theWriteGateRefusesWireBreakingEvolutionAndAcceptsACompatibleOne() throws Exception {
        String subject = "document-events-gate-" + RUN + "-value";
        ensureImportSubjects();
        register(subject, documentEventText, referencesJson());

        HttpResponse<String> refused = postRegister(subject, GATE_V2_BREAKING, referencesJson());
        assertThat(refused.statusCode()).isEqualTo(409);
        assertThat(refused.body()).contains("\"error_code\":409").contains("incompatible");
        assertThat(store.latest(subject).orElseThrow().version())
                .as("a refused registration adds no version")
                .isEqualTo(1);

        int evolvedId = register(subject, GATE_V3_COMPATIBLE, referencesJson());
        StoredSchema evolved = store.latest(subject).orElseThrow();
        assertThat(evolved.version()).isEqualTo(2);
        assertThat(evolved.globalId()).isEqualTo(evolvedId);
    }

    // ---------------------------------------------------------------- serdes

    private static ProtoMoltProtobufSerializer protoMoltSerializer(boolean pinned) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64, descriptorSetBase64);
        config.put(ProtoMoltSerdeConfig.SCHEMA_REGISTRY_URL, registryUrl);
        if (pinned) {
            config.put(ProtoMoltSerdeConfig.MESSAGE_TYPE, EVENT_TYPE);
        }
        ProtoMoltProtobufSerializer serializer = new ProtoMoltProtobufSerializer();
        serializer.configure(config, false);
        return serializer;
    }

    private static ProtoMoltProtobufDeserializer protoMoltDeserializer(boolean pinned) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64, descriptorSetBase64);
        config.put(ProtoMoltSerdeConfig.SCHEMA_REGISTRY_URL, registryUrl);
        if (pinned) {
            config.put(ProtoMoltSerdeConfig.MESSAGE_TYPE, EVENT_TYPE);
        }
        ProtoMoltProtobufDeserializer deserializer = new ProtoMoltProtobufDeserializer();
        deserializer.configure(config, false);
        return deserializer;
    }

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

    // ---------------------------------------------------------------- broker

    private static <V> void produce(String topic, String key, V value,
            org.apache.kafka.common.serialization.Serializer<V> serializer) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, REDPANDA.getBootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        try (KafkaProducer<String, V> producer =
                     new KafkaProducer<>(props, new StringSerializer(), serializer)) {
            producer.send(new ProducerRecord<>(topic, key, value)).get();
        }
    }

    private static <V> V consumeOne(String topic,
            org.apache.kafka.common.serialization.Deserializer<V> deserializer, String key) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, REDPANDA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "serde-interop-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (KafkaConsumer<String, V> consumer =
                     new KafkaConsumer<>(props, new StringDeserializer(), deserializer)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            while (System.nanoTime() < deadline) {
                ConsumerRecords<String, V> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, V> record : records) {
                    if (key.equals(record.key())) {
                        return record.value();
                    }
                }
            }
            throw new AssertionError("no record with key " + key + " on " + topic + " within 60s");
        }
    }

    // ---------------------------------------------------------------- registration

    private static String topic(String quadrant) {
        return "serde-interop-" + quadrant + "-" + RUN;
    }

    /** The import subject, registered the way the Confluent client names it (the import path). */
    private static void ensureImportSubjects() throws Exception {
        if (importSubjectsRegistered) {
            return;
        }
        register(ADDRESS_SUBJECT, addressText, "");
        importSubjectsRegistered = true;
    }

    /** The DocumentEvent subject under the topic-name strategy, with both import references. */
    private static int registerDocumentEventSubject(String topic) throws Exception {
        ensureImportSubjects();
        return register(topic + "-value", documentEventText, referencesJson());
    }

    private static String referencesJson() {
        return ",\"references\":[" + referenceJson(ADDRESS_SUBJECT) + "]}";
    }

    private static String referenceJson(String importPath) {
        return "{\"name\":\"" + importPath + "\",\"subject\":\"" + importPath + "\",\"version\":1}";
    }

    private static int register(String subject, String schema, String referencesJson)
            throws Exception {
        HttpResponse<String> response = postRegister(subject, schema, referencesJson);
        assertThat(response.statusCode())
                .as("registering %s: %s", subject, response.body())
                .isEqualTo(200);
        String json = response.body();
        int at = json.indexOf("\"id\"");
        return Integer.parseInt(json.substring(json.indexOf(':', at) + 1)
                .replaceAll("[^0-9].*$", "").trim());
    }

    private static HttpResponse<String> postRegister(String subject, String schema,
            String referencesJson) throws Exception {
        String encoded = URLEncoder.encode(subject, StandardCharsets.UTF_8).replace("+", "%20");
        String body = "{\"schemaType\":\"PROTOBUF\",\"schema\":" + quote(schema)
                + (referencesJson.isEmpty() ? "}" : referencesJson);
        return HTTP.send(HttpRequest
                .newBuilder(URI.create(registryUrl + "/subjects/" + encoded + "/versions"))
                .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build(), HttpResponse.BodyHandlers.ofString());
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

    // ---------------------------------------------------------------- fixtures

    private static DocumentEvent sampleEvent(String docId) {
        return DocumentEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setSaved(DocumentSaved.newBuilder()
                        .setAddress(NodeAddress.newBuilder()
                                .setDocId(docId)
                                .setGraphAddressId("ds-1")
                                .setAccountId("acct-interop")
                                .setGraphId("intake:acct-interop"))
                        .setChecksum("cksum-" + docId)
                        .setDocVersion(3)
                        .setSizeBytes(1234L)
                        .setSavedAt(Timestamp.newBuilder()
                                .setSeconds(1_700_000_000L).setNanos(42))
                        .putMetadata("origin", "serde-interop"))
                .build();
    }

    /**
     * The producer's packaged contract: document_events.proto plus its transitive imports, from
     * the generated classes' own runtime descriptors (the same closure
     * {@code DocumentEventFactory.descriptorSetBase64()} ships in the repo service).
     */
    private static String descriptorSetBase64() {
        Map<String, FileDescriptorProto> files = new LinkedHashMap<>();
        ArrayDeque<FileDescriptor> queue =
                new ArrayDeque<>(List.of(DocumentEvent.getDescriptor().getFile()));
        while (!queue.isEmpty()) {
            FileDescriptor file = queue.pop();
            if (files.put(file.getName(), file.toProto()) == null) {
                queue.addAll(file.getDependencies());
            }
        }
        return Base64.getEncoder().encodeToString(FileDescriptorSet.newBuilder()
                .addAllFile(files.values())
                .build().toByteArray());
    }
}
