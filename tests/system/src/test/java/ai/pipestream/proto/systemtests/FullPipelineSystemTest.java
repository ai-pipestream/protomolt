package ai.pipestream.proto.systemtests;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.gather.GatheringDescriptorLoader;
import ai.pipestream.proto.gather.git.GitProtoGatherer;
import ai.pipestream.proto.index.opensearch.OpenSearchMappingGenerator;
import ai.pipestream.proto.index.spi.CatalogIndexingHintSource;
import ai.pipestream.proto.index.spi.IndexFieldKind;
import ai.pipestream.proto.index.spi.IndexingPlan;
import ai.pipestream.proto.index.spi.IndexingPlanFactory;
import ai.pipestream.proto.index.spi.ResolvedFieldHint;
import ai.pipestream.proto.json.ProtobufJsonTranscoder;
import ai.pipestream.proto.jsonschema.ProtoJsonSchemaGenerator;
import ai.pipestream.proto.registry.CompatibilityWriteGate;
import ai.pipestream.proto.registry.GitSchemaRegistryStore;
import ai.pipestream.proto.registry.server.SchemaRegistryServer;
import ai.pipestream.proto.registry.server.SchemaRegistryServerConfig;
import ai.pipestream.proto.schema.confluent.ConfluentSchemaPublisher;
import ai.pipestream.proto.schema.confluent.ConfluentSchemaRegistryLoader;
import ai.pipestream.proto.sources.ProtoSourceSet;
import ai.pipestream.proto.sources.publish.PublishOptions;
import ai.pipestream.proto.sources.publish.PublishResult;
import ai.pipestream.proto.sources.publish.PublishResult.Action;
import ai.pipestream.proto.sources.publish.PublishResult.FileOutcome;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import ai.pipestream.proto.validate.model.FieldConstraints;
import ai.pipestream.proto.validate.model.IntegralConstraints;
import ai.pipestream.proto.validate.model.MessageConstraints;
import ai.pipestream.proto.validate.spi.ValidationRuleSource;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.eclipse.jgit.api.Git;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;

/**
 * The flagship end-to-end story: a schema team keeps {@code .proto} sources in a plain Git
 * repository; ProtoMolt gathers them (real JGit clone over {@code file://}), compiles them,
 * publishes them into our own Git-backed registry server through the Confluent subjects
 * protocol, loads them back through the same protocol, and then consumes the loaded dynamic
 * descriptors: JSON transcoding, validation, JSON Schema generation, and index-mapping
 * generation. Finally the compatibility write-gate is proven live: an incompatible commit is
 * rejected with the {@code FIELD_TYPE_CHANGED} rule while v1 keeps being served, and a
 * compatible commit sails through as version 2.
 *
 * <p>Ordered steps are one continuous scenario — state flows from step to step on purpose.</p>
 *
 * <p><b>Validation-options fallback.</b> The team schemas here are plain proto3 without
 * {@code ai.pipestream.proto.validate.v1} options-in-text. The gather path compiles with the
 * Wire-based {@link ai.pipestream.proto.sources.ProtoSourceCompiler}, and nothing in the repo
 * demonstrates Wire compiling {@code extend google.protobuf.FieldOptions} declarations plus
 * option usages into descriptors that {@code AiPipestreamRuleSource} could read back (the
 * validation module compiles its option-bearing fixtures with protoc via the protobuf Gradle
 * plugin instead). So this test exercises the sanctioned programmatic path: a
 * {@link ValidationRuleSource} supplies the same neutral-model constraints the options would
 * have carried, and {@link ProtoValidator} enforces them against registry-loaded
 * {@link DynamicMessage}s.</p>
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FullPipelineSystemTest {

    private static final String COMMON_PATH = "shop/v1/common.proto";
    private static final String CUSTOMER_PATH = "shop/v1/customer.proto";
    private static final String ORDER_PATH = "shop/v1/order.proto";

    private static final String COMMON_PROTO = """
            syntax = "proto3";
            package shop.v1;

            enum OrderStatus {
              ORDER_STATUS_UNSPECIFIED = 0;
              ORDER_STATUS_NEW = 1;
              ORDER_STATUS_PAID = 2;
            }

            message Money {
              string currency = 1;
              int64 amount_micros = 2;
            }
            """;

    private static final String CUSTOMER_PROTO = """
            syntax = "proto3";
            package shop.v1;

            message Customer {
              string id = 1;
              string email = 2;
            }
            """;

    private static final String ORDER_PROTO = """
            syntax = "proto3";
            package shop.v1;

            import "shop/v1/common.proto";
            import "shop/v1/customer.proto";
            import "google/protobuf/timestamp.proto";

            message Order {
              string id = 1;
              Customer customer = 2;
              Money total = 3;
              OrderStatus status = 4;
              google.protobuf.Timestamp created = 5;
            }
            """;

    /** Step 6's poison pill: the order id changes wire shape from string to varint. */
    private static final String INCOMPATIBLE_ORDER_PROTO =
            ORDER_PROTO.replace("string id = 1;", "int64 id = 1;");

    /** Step 7's fix: original shape restored, plus a purely additive field. */
    private static final String COMPATIBLE_ORDER_PROTO = ORDER_PROTO.replace(
            "google.protobuf.Timestamp created = 5;",
            "google.protobuf.Timestamp created = 5;\n  string note = 6;");

    private static final String VALID_ORDER_JSON = """
            {
              "id": "o-1001",
              "customer": {"id": "c-7", "email": "kim@example.com"},
              "total": {"currency": "USD", "amountMicros": "12500000"},
              "status": "ORDER_STATUS_PAID",
              "created": "2026-07-14T10:15:30Z"
            }
            """;

    private static final String INVALID_ORDER_JSON = """
            {
              "id": "",
              "total": {"currency": "", "amountMicros": "-5"}
            }
            """;

    @TempDir
    static Path work;

    private static Path teamRepo;
    private static String teamRepoUrl;
    private static Git teamGit;
    private static GitProtoGatherer gatherer;
    private static GatheringDescriptorLoader gitLoader;
    private static GitSchemaRegistryStore store;
    private static SchemaRegistryServer server;
    private static URI base;
    private static ProtoSourceSet gathered;

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    @BeforeAll
    static void initPaths() {
        teamRepo = work.resolve("team-repo");
        teamRepoUrl = teamRepo.toUri().toString();
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (gitLoader != null) {
            gitLoader.close();
        }
        if (server != null) {
            server.close();
        }
        if (store != null) {
            store.close();
        }
        if (teamGit != null) {
            teamGit.close();
        }
    }

    @Test
    @Order(1)
    void step1_schemaTeamCommitsAReferenceLinkedProtoTreeToGit() throws Exception {
        Files.createDirectories(teamRepo);
        teamGit = Git.init().setDirectory(teamRepo.toFile()).setInitialBranch("main").call();

        writeTeamFile("proto/" + COMMON_PATH, COMMON_PROTO);
        writeTeamFile("proto/" + CUSTOMER_PATH, CUSTOMER_PROTO);
        writeTeamFile("proto/" + ORDER_PATH, ORDER_PROTO);
        commitAll("shop schemas v1");

        assertThat(teamRepo.resolve("proto/" + ORDER_PATH)).isRegularFile();
        assertThat(teamGit.log().call().iterator()).hasNext();
    }

    @Test
    @Order(2)
    void step2_gathererClonesTheRepoAndTheSourcesCompileToLinkedDescriptors() throws Exception {
        gatherer = GitProtoGatherer.builder()
                .repo(teamRepoUrl)
                .ref("main")
                .cacheDir(work.resolve("git-cache"))
                .build();

        gathered = gatherer.gather();
        assertThat(gathered.paths())
                .containsExactlyInAnyOrder(COMMON_PATH, CUSTOMER_PATH, ORDER_PATH);
        assertThat(gathered.get(ORDER_PATH).orElseThrow().origin())
                .isEqualTo("git:" + teamRepoUrl + "@main");

        // The gathered tree also compiles straight to runtime descriptors via the loader SPI.
        gitLoader = new GatheringDescriptorLoader(gatherer);
        FileDescriptor orderFile = gitLoader.loadDescriptorForType("shop.v1.Order");
        assertThat(orderFile).isNotNull();
        Descriptor order = orderFile.findMessageTypeByName("Order");
        assertThat(order.getFields()).hasSize(5);
        assertThat(order.findFieldByName("total").getMessageType().getFullName())
                .isEqualTo("shop.v1.Money");
    }

    @Test
    @Order(3)
    void step3_gitBackedRegistryServerStartsBehindTheCompatibilityGate() throws Exception {
        store = GitSchemaRegistryStore.builder()
                .repositoryDir(work.resolve("registry-repo"))
                .writeGate(new CompatibilityWriteGate())
                .build();
        server = new SchemaRegistryServer(
                SchemaRegistryServerConfig.defaults().withHost("127.0.0.1").withPort(0), store);
        base = URI.create("http://127.0.0.1:" + server.start());

        assertThat(store.subjects()).isEmpty();
        assertThat(store.globalCompatibilityMode()).isEqualTo("BACKWARD");

        HttpResponse<String> health = HTTP.send(
                HttpRequest.newBuilder(base.resolve("/health"))
                        .timeout(Duration.ofSeconds(10)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(health.statusCode()).isEqualTo(200);
    }

    @Test
    @Order(4)
    void step4_publisherRegistersTheGatheredSetAndEveryFileIsCreated() throws Exception {
        try (ConfluentSchemaPublisher publisher = new ConfluentSchemaPublisher(base)) {
            PublishResult result = publisher.publish(gathered, PublishOptions.defaults());
            result.throwIfFailed();

            assertThat(result.outcomes())
                    .extracting(FileOutcome::path, FileOutcome::action)
                    .containsExactlyInAnyOrder(
                            tuple(COMMON_PATH, Action.CREATED),
                            tuple(CUSTOMER_PATH, Action.CREATED),
                            tuple(ORDER_PATH, Action.CREATED));
            assertThat(result.outcomes())
                    .allSatisfy(outcome -> assertThat(outcome.detail()).isEqualTo("version 1"));
        }
        assertThat(store.subjects()).containsExactly(COMMON_PATH, CUSTOMER_PATH, ORDER_PATH);
        assertThat(store.versions(ORDER_PATH)).containsExactly(1);
    }

    @Test
    @Order(5)
    void step5_loadedSchemaDrivesTranscodingValidationJsonSchemaAndIndexMappings()
            throws Exception {
        try (ConfluentSchemaRegistryLoader loader = new ConfluentSchemaRegistryLoader(base)) {
            assertThat(loader.loadDescriptors()).hasSize(3);
            assertThat(loader.lastSkippedSubjectCount()).isZero();

            FileDescriptor orderFile = loader.loadDescriptor("shop.v1.Order");
            Descriptor order = orderFile.findMessageTypeByName("Order");
            assertThat(order.findFieldByName("customer").getMessageType().getFullName())
                    .isEqualTo("shop.v1.Customer");
            assertThat(order.findFieldByName("created").getMessageType().getFullName())
                    .isEqualTo("google.protobuf.Timestamp");

            // JSON -> DynamicMessage through the descriptor registry.
            DescriptorRegistry registry = DescriptorRegistry.create();
            registry.registerFile(orderFile);
            ProtobufJsonTranscoder transcoder = new ProtobufJsonTranscoder(registry);
            DynamicMessage message = transcoder.fromJsonDynamic(VALID_ORDER_JSON, "shop.v1.Order");

            assertThat(message.getField(order.findFieldByName("id"))).isEqualTo("o-1001");
            Message total = (Message) message.getField(order.findFieldByName("total"));
            Descriptor money = total.getDescriptorForType();
            assertThat(total.getField(money.findFieldByName("currency"))).isEqualTo("USD");
            assertThat(total.getField(money.findFieldByName("amount_micros")))
                    .isEqualTo(12_500_000L);
            EnumValueDescriptor status =
                    (EnumValueDescriptor) message.getField(order.findFieldByName("status"));
            assertThat(status.getName()).isEqualTo("ORDER_STATUS_PAID");
            Message created = (Message) message.getField(order.findFieldByName("created"));
            long seconds = (Long) created.getField(
                    created.getDescriptorForType().findFieldByName("seconds"));
            assertThat(Instant.ofEpochSecond(seconds))
                    .isEqualTo(Instant.parse("2026-07-14T10:15:30Z"));

            // Round-trip: the printed JSON carries the canonical field names back out.
            assertThat(transcoder.toJson(message)).contains("amountMicros", "ORDER_STATUS_PAID");

            // Validation over the dynamic message, rules supplied programmatically (see javadoc).
            ProtoValidator validator =
                    ProtoValidator.forMessageType(order, List.of(new ShopRules()));
            assertThat(validator.validate(message).valid()).isTrue();

            DynamicMessage invalid = transcoder.fromJsonDynamic(INVALID_ORDER_JSON, "shop.v1.Order");
            ValidationResult failed = validator.validate(invalid);
            assertThat(failed.valid()).isFalse();
            assertThat(failed.violations())
                    .anyMatch(v -> v.path().equals("id") && v.ruleId().equals("required"))
                    .anyMatch(v -> v.path().equals("total.currency")
                            && v.ruleId().equals("required"))
                    .anyMatch(v -> v.path().equals("total.amount_micros")
                            && v.ruleId().startsWith("int64."));

            // JSON Schema generation from the same loaded descriptor.
            Map<String, Object> schema = ProtoJsonSchemaGenerator.create().generate(order);
            assertThat(schema.get("$schema"))
                    .isEqualTo("https://json-schema.org/draft/2020-12/schema");
            assertThat(schema.get("$ref")).isEqualTo("#/$defs/shop.v1.Order");
            @SuppressWarnings("unchecked")
            Map<String, Object> defs = (Map<String, Object>) schema.get("$defs");
            assertThat(defs).containsKeys("shop.v1.Order", "shop.v1.Money", "shop.v1.Customer");

            // Indexing plan + OpenSearch mappings, hints supplied through the catalog source.
            CatalogIndexingHintSource catalog = new CatalogIndexingHintSource()
                    .put("shop.v1.Order", "id", ResolvedFieldHint.of(IndexFieldKind.KEYWORD));
            IndexingPlan plan = IndexingPlanFactory.defaults(catalog).create(order);
            assertThat(plan.messageFullName()).isEqualTo("shop.v1.Order");
            Optional<IndexingPlan.IndexedField> idField = plan.find("id");
            assertThat(idField).isPresent();
            assertThat(idField.orElseThrow().type()).isEqualTo(IndexFieldKind.KEYWORD);

            Map<String, Object> mappings = new OpenSearchMappingGenerator().generate(plan);
            @SuppressWarnings("unchecked")
            Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
            assertThat(properties).isNotEmpty();
            @SuppressWarnings("unchecked")
            Map<String, Object> idMapping = (Map<String, Object>) properties.get("id");
            assertThat(idMapping).containsEntry("type", "keyword");
        }
    }

    @Test
    @Order(6)
    void step6_incompatibleCommitIsRejectedWith409CarryingTheRuleAndV1KeepsServing()
            throws Exception {
        writeTeamFile("proto/" + ORDER_PATH, INCOMPATIBLE_ORDER_PROTO);
        commitAll("BREAKING: order id becomes int64");

        // Same gatherer, same cache dir: this is the fetch-and-reset path, not a fresh clone.
        ProtoSourceSet regathered = gatherer.gather();
        assertThat(regathered.get(ORDER_PATH).orElseThrow().content()).contains("int64 id = 1;");

        try (ConfluentSchemaPublisher publisher = new ConfluentSchemaPublisher(base)) {
            PublishResult result = publisher.publish(regathered, PublishOptions.defaults());

            assertThat(result.outcomes())
                    .extracting(FileOutcome::path, FileOutcome::action)
                    .containsExactlyInAnyOrder(
                            tuple(COMMON_PATH, Action.UNCHANGED),
                            tuple(CUSTOMER_PATH, Action.UNCHANGED),
                            tuple(ORDER_PATH, Action.FAILED));
            FileOutcome failure = result.failures().get(0);
            assertThat(failure.path()).isEqualTo(ORDER_PATH);
            assertThat(failure.detail())
                    .startsWith("HTTP 409")
                    .contains("FIELD_TYPE_CHANGED")
                    .contains("shop.v1.Order.id");
        }

        // The gate held the line: no v2 was written and the registry still serves v1.
        assertThat(store.versions(ORDER_PATH)).containsExactly(1);
        try (ConfluentSchemaRegistryLoader loader = new ConfluentSchemaRegistryLoader(base)) {
            FieldDescriptor id = loader.loadDescriptor("shop.v1.Order")
                    .findMessageTypeByName("Order").findFieldByName("id");
            assertThat(id.getType()).isEqualTo(FieldDescriptor.Type.STRING);
        }
    }

    @Test
    @Order(7)
    void step7_compatibleCommitIsAcceptedAsVersion2AndTheNewFieldIsServed() throws Exception {
        writeTeamFile("proto/" + ORDER_PATH, COMPATIBLE_ORDER_PROTO);
        commitAll("revert breaking change, add note field");

        ProtoSourceSet regathered = gatherer.gather();
        try (ConfluentSchemaPublisher publisher = new ConfluentSchemaPublisher(base)) {
            PublishResult result = publisher.publish(regathered, PublishOptions.defaults());
            result.throwIfFailed();

            assertThat(result.outcomes())
                    .extracting(FileOutcome::path, FileOutcome::action)
                    .containsExactlyInAnyOrder(
                            tuple(COMMON_PATH, Action.UNCHANGED),
                            tuple(CUSTOMER_PATH, Action.UNCHANGED),
                            tuple(ORDER_PATH, Action.UPDATED));
            assertThat(result.outcomes().stream()
                    .filter(outcome -> outcome.path().equals(ORDER_PATH))
                    .findFirst().orElseThrow().detail()).isEqualTo("version 2");
        }
        assertThat(store.versions(ORDER_PATH)).containsExactly(1, 2);

        // A fresh registry client sees the evolved schema...
        try (ConfluentSchemaRegistryLoader loader = new ConfluentSchemaRegistryLoader(base)) {
            Descriptor order = loader.loadDescriptor("shop.v1.Order")
                    .findMessageTypeByName("Order");
            assertThat(order.findFieldByName("note")).isNotNull();
            assertThat(order.findFieldByName("id").getType())
                    .isEqualTo(FieldDescriptor.Type.STRING);
        }

        // ...and so does the direct git loader once refreshed (drop cache, re-gather, recompile).
        gitLoader.refresh();
        Descriptor fromGit = gitLoader.loadDescriptorForType("shop.v1.Order")
                .findMessageTypeByName("Order");
        assertThat(fromGit.findFieldByName("note")).isNotNull();
    }

    // ------------------------------------------------------------------ helpers

    private static void writeTeamFile(String relative, String content) throws Exception {
        Path file = teamRepo.resolve(relative);
        Files.createDirectories(file.getParent());
        Files.writeString(file, content);
    }

    private static void commitAll(String message) throws Exception {
        teamGit.add().addFilepattern(".").call();
        teamGit.add().setUpdate(true).addFilepattern(".").call();
        teamGit.commit()
                .setMessage(message)
                .setAuthor("Shop Schema Team", "schemas@shop.test")
                .setCommitter("Shop Schema Team", "schemas@shop.test")
                .setSign(false)
                .call();
    }

    /**
     * The constraints the {@code validate.v1} options would have carried, supplied through the
     * programmatic {@link ValidationRuleSource} SPI instead (see the class javadoc for why).
     */
    private static final class ShopRules implements ValidationRuleSource {

        @Override
        public Optional<FieldConstraints> fieldConstraints(FieldDescriptor field) {
            return switch (field.getFullName()) {
                case "shop.v1.Order.id", "shop.v1.Money.currency" ->
                        Optional.of(FieldConstraints.builder().required(true).build());
                case "shop.v1.Money.amount_micros" -> Optional.of(FieldConstraints.builder()
                        .integral(IntegralConstraints.builder("int64").gte(0).build())
                        .build());
                default -> Optional.empty();
            };
        }

        @Override
        public Optional<MessageConstraints> messageConstraints(Descriptor message) {
            return Optional.empty();
        }
    }
}
