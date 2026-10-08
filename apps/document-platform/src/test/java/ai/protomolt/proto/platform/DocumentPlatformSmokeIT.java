package ai.protomolt.proto.platform;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.authz.AccessPolicyCallers;
import ai.protomolt.proto.intake.service.identity.ApiKeyServerInterceptor;
import ai.protomolt.proto.intake.service.identity.InMemoryApiKeyIdentityResolver;
import ai.protomolt.proto.intake.service.identity.IntakeScope;
import ai.protomolt.proto.intake.v1.IngestDocumentRequest;
import ai.protomolt.proto.intake.v1.IngestDocumentResponse;
import ai.protomolt.proto.intake.v1.IntakeServiceGrpc;
import ai.protomolt.proto.intake.v1.RawPayload;
import ai.protomolt.proto.jobs.service.store.WorkflowRunStoreConfig;
import ai.protomolt.proto.metric.MetricServiceGrpc;
import ai.protomolt.proto.metric.QueryMetricsRequest;
import ai.protomolt.proto.metric.QueryMetricsResponse;
import ai.protomolt.proto.parse.v1.ParseDocumentRequest;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.service.RepoServiceConfig;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import ai.protomolt.proto.repo.v1.DeleteDocumentByReferenceCommand;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentPart;
import ai.protomolt.proto.repo.v1.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v1.DriveServiceGrpc;
import ai.protomolt.proto.repo.v1.DriveType;
import ai.protomolt.proto.repo.v1.GetDocumentByReferenceRequest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import ai.protomolt.proto.search.service.RepoDocumentMapping;
import ai.protomolt.proto.search.v1.DeleteAndUnindexRequest;
import ai.protomolt.proto.search.v1.ParseAndIndexRequest;
import ai.protomolt.proto.search.v1.SearchLane;
import ai.protomolt.proto.search.v1.SearchRequest;
import ai.protomolt.proto.search.v1.SearchResponse;
import ai.protomolt.proto.search.v1.SearchServiceGrpc;
import io.grpc.Metadata;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import java.net.URI;
import java.util.Map;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Smoke-proves the one-container platform through its OWN composition root
 * and its EXTERNAL surfaces only: every call in this test crosses a real
 * TCP port the container would expose. Ingest over the intake service, a
 * durable parse submitted through the registry's submit-workflow action and
 * completed by the running worker, the parsed result read back over repo's
 * public gRPC, the registry serving the fleet document model, the playground
 * page serving, the durable parse-and-index workflow producing a search hit
 * through the search service, a replay that re-derives without duplicating, and
 * the delete-and-unindex workflow leaving the document unsearchable.
 */
@Testcontainers(disabledWithoutDocker = true)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DocumentPlatformSmokeIT {

    static final String ACCOUNT = "acct-platform";
    static final String API_KEY = "platform-smoke-key";
    /** Synthetic operator token: a node mounting the repository role is always guarded. */
    static final String OPERATOR_TOKEN = "platform-smoke-operator-token";
    /** Synthetic console login; the guarded console admits access-policy principals only. */
    static final String CONSOLE_CREDENTIAL = "platform-smoke-console-credential";
    static final ObjectMapper MAPPER = new ObjectMapper();
    static final HttpClient HTTP = HttpClient.newHttpClient();

    @Container
    static final PostgreSQLContainer REPO_DB = new PostgreSQLContainer("postgres:18-alpine");

    @Container
    static final PostgreSQLContainer JOBS_DB = new PostgreSQLContainer("postgres:18-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    @TempDir
    static Path work;

    static DocumentPlatform platform;
    static ManagedChannel intakeChannel;
    static ManagedChannel repoChannel;
    static URI registryBase;
    static String consoleCookie;

    static IngestDocumentResponse receipt;

    @BeforeAll
    static void boot() throws Exception {
        Path policy = work.resolve("access-policy.json");
        Files.writeString(policy, """
                {"principals": [
                  {"name": "console-operator",
                   "credentialSha256": ["%s"],
                   "scopes": ["search-query", "schema-read", "service-invoke"]}
                ]}""".formatted(AccessPolicyCallers.sha256Hex(CONSOLE_CREDENTIAL)));
        platform = DocumentPlatform.start(
                new DocumentPlatformConfig(
                        new RepoServiceConfig(
                                0,
                                new LedgerConfig(
                                        REPO_DB.getJdbcUrl(),
                                        REPO_DB.getUsername(),
                                        REPO_DB.getPassword()),
                                LOCALSTACK.getEndpoint().toString(),
                                LOCALSTACK.getRegion(),
                                LOCALSTACK.getAccessKey(),
                                LOCALSTACK.getSecretKey(),
                                "platform-docs",
                                0,
                                null, null, null, null, 0, 0L),
                        new WorkflowRunStoreConfig(
                                JOBS_DB.getJdbcUrl(),
                                JOBS_DB.getUsername(),
                                JOBS_DB.getPassword(),
                                WorkflowRunStoreConfig.DEFAULT_POOL_SIZE,
                                WorkflowRunStoreConfig.DEFAULT_MIGRATION_LOCATION),
                        work.resolve("registry.git"),
                        0, 0, 0, 0,
                        null, null, null,
                        60L,
                        1,
                        0,
                        work.resolve("search-index"),
                        0,
                        0,
                        null,
                        Map.of(DocumentPlatformConfig.ENV_API_TOKEN, OPERATOR_TOKEN,
                                DocumentPlatformConfig.ENV_ACCESS_POLICY, policy.toString())),
                new InMemoryApiKeyIdentityResolver()
                        .register(API_KEY, IntakeScope.unrestricted(ACCOUNT)));

        repoChannel = operatorChannel(platform.repoPort());
        DriveServiceGrpc.newBlockingStub(repoChannel).createDrive(CreateDriveRequest.newBuilder()
                .setName("intake")
                .setAccountId(ACCOUNT)
                .setDriveType(DriveType.DRIVE_TYPE_INTAKE)
                .build());
        intakeChannel = NettyChannelBuilder.forAddress("127.0.0.1", platform.intakePort())
                .usePlaintext()
                .build();
        registryBase = URI.create("http://127.0.0.1:" + platform.registryPort());
    }

    @AfterAll
    static void shutdown() {
        if (intakeChannel != null) {
            intakeChannel.shutdownNow();
        }
        if (repoChannel != null) {
            repoChannel.shutdownNow();
        }
        if (platform != null) {
            platform.close();
        }
    }

    @Test
    @Order(1)
    void theRegistryServesTheDocumentModelFromFirstBoot() throws Exception {
        HttpResponse<String> subjects = HTTP.send(
                HttpRequest.newBuilder(registryBase.resolve("/subjects"))
                        .header("api_token", OPERATOR_TOKEN).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(subjects.statusCode()).isEqualTo(200);
        assertThat(subjects.body()).contains(DocumentPlatform.DOCUMENT_SUBJECT);
    }

    @Test
    @Order(2)
    void aDocumentEntersThroughTheAuthenticatedIntakeServiceOverTcp() {
        Metadata metadata = new Metadata();
        metadata.put(ApiKeyServerInterceptor.API_KEY, API_KEY);
        receipt = IntakeServiceGrpc.newBlockingStub(intakeChannel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
                .ingestDocument(IngestDocumentRequest.newBuilder()
                        .setRaw(RawPayload.newBuilder()
                                .setData(ByteString.copyFromUtf8(
                                        "Platform Smoke\n\nThe one container works."))
                                .setFilename("smoke.txt")
                                .setMimeType("text/plain"))
                        .setDatasourceId("ds-smoke")
                        .build());
        assertThat(receipt.getDocId()).isNotBlank();
        assertThat(receipt.getAddress().getGraphId()).isEqualTo("intake:" + ACCOUNT);
    }

    @Test
    @Order(3)
    void aDurableParseSubmittedThroughTheRegistryActionCompletes() throws Exception {
        ObjectNode submit = MAPPER.createObjectNode();
        submit.put("workflowName", "parse-document");
        submit.set("input", MAPPER.readTree(JsonFormat.printer().print(
                ParseDocumentRequest.newBuilder().setAddress(receipt.getAddress()).build())));
        JsonNode submitted = postAction("submit-workflow", submit);
        assertThat(submitted.path("ok").asBoolean()).as(submitted.toString()).isTrue();
        String jobId = submitted.path("jobId").asText();
        assertThat(jobId).isNotBlank();

        // The platform's own worker loops claim and complete the job.
        String status = "";
        for (int i = 0; i < 120 && !"COMPLETED".equals(status); i++) {
            ObjectNode get = MAPPER.createObjectNode();
            get.put("jobId", jobId);
            JsonNode job = postAction("get-job", get);
            status = job.path("job").path("status").asText(job.path("status").asText());
            if ("FAILED".equals(status) || "DEAD".equals(status)) {
                throw new AssertionError("parse job " + status + ": " + job);
            }
            if (!"COMPLETED".equals(status)) {
                Thread.sleep(500);
            }
        }
        assertThat(status).isEqualTo("COMPLETED");

        Document stored = DocumentServiceGrpc.newBlockingStub(repoChannel)
                .getDocumentByReference(GetDocumentByReferenceRequest.newBuilder()
                        .setAddress(receipt.getAddress())
                        .addParts(DocumentPart.DOCUMENT_PART_CORE)
                        .addParts(DocumentPart.DOCUMENT_PART_PARSED)
                        .build())
                .getDocument();
        assertThat(stored.getParserResultsMap()).containsKey("text");
        assertThat(stored.getSearchMetadata().getTitle()).isEqualTo("Platform Smoke");
    }

    @Test
    @Order(4)
    void parseAndIndexMakesTheDocumentSearchableOverTcp() throws Exception {
        ObjectNode submit = MAPPER.createObjectNode();
        submit.put("workflowName", "parse-and-index");
        submit.set("input", MAPPER.readTree(JsonFormat.printer().print(
                ParseAndIndexRequest.newBuilder()
                        .setAddress(receipt.getAddress())
                        .setMappingSubject(RepoDocumentMapping.SUBJECT)
                        .build())));
        JsonNode submitted = postAction("submit-workflow", submit);
        assertThat(submitted.path("ok").asBoolean()).as(submitted.toString()).isTrue();
        String jobId = submitted.path("jobId").asText();

        String status = "";
        for (int i = 0; i < 120 && !"COMPLETED".equals(status); i++) {
            ObjectNode get = MAPPER.createObjectNode();
            get.put("jobId", jobId);
            JsonNode job = postAction("get-job", get);
            status = job.path("job").path("status").asText(job.path("status").asText());
            if ("FAILED".equals(status) || "DEAD".equals(status)) {
                throw new AssertionError("parse-and-index job " + status + ": " + job);
            }
            if (!"COMPLETED".equals(status)) {
                Thread.sleep(500);
            }
        }
        assertThat(status).isEqualTo("COMPLETED");

        ManagedChannel searchChannel = operatorChannel(platform.searchPort());
        try {
            SearchResponse hits = SearchServiceGrpc.newBlockingStub(searchChannel)
                    .search(SearchRequest.newBuilder()
                            .setMappingSubject(RepoDocumentMapping.SUBJECT)
                            .setQuery("container works")
                            .setK(3)
                            .setLane(SearchLane.SEARCH_LANE_LEXICAL)
                            .build());
            assertThat(hits.getHitsList()).isNotEmpty();
            assertThat(hits.getHits(0).getDocId()).isEqualTo(receipt.getDocId());
            assertThat(hits.getHits(0).getStoredMap())
                    .containsEntry("search_metadata_title",
                            ai.protomolt.proto.search.v1.StoredValue.newBuilder()
                                    .setStringValue("Platform Smoke").build());
        } finally {
            searchChannel.shutdownNow();
        }
    }

    @Test
    @Order(5)
    void replayResubmitsTheCorpusAndNeverDuplicates() throws Exception {
        ObjectNode input = MAPPER.createObjectNode();
        input.put("workflowName", "parse-and-index");
        input.put("mappingSubject", RepoDocumentMapping.SUBJECT);
        input.put("drive", "intake");
        input.put("accountId", ACCOUNT);
        JsonNode replayed = postAction("replay-documents", input);
        assertThat(replayed.path("submitted").asInt()).isGreaterThanOrEqualTo(1);

        for (JsonNode jobId : replayed.path("jobIds")) {
            String status = "";
            for (int i = 0; i < 120 && !"COMPLETED".equals(status); i++) {
                ObjectNode get = MAPPER.createObjectNode();
                get.put("jobId", jobId.asText());
                JsonNode job = postAction("get-job", get);
                status = job.path("job").path("status").asText(job.path("status").asText());
                if ("FAILED".equals(status) || "DEAD".equals(status)) {
                    throw new AssertionError("replayed job " + status + ": " + job);
                }
                if (!"COMPLETED".equals(status)) {
                    Thread.sleep(500);
                }
            }
            assertThat(status).isEqualTo("COMPLETED");
        }

        // The replay re-indexed everything; nothing duplicated.
        ManagedChannel searchChannel = operatorChannel(platform.searchPort());
        try {
            SearchResponse hits = SearchServiceGrpc.newBlockingStub(searchChannel)
                    .search(SearchRequest.newBuilder()
                            .setMappingSubject(RepoDocumentMapping.SUBJECT)
                            .setQuery("container works")
                            .setK(10)
                            .setLane(SearchLane.SEARCH_LANE_LEXICAL)
                            .build());
            assertThat(hits.getHitsList()).hasSize(1);
            assertThat(hits.getHits(0).getDocId()).isEqualTo(receipt.getDocId());
        } finally {
            searchChannel.shutdownNow();
        }
    }

    @Test
    @Order(6)
    void thePlaygroundServes() throws Exception {
        HttpResponse<String> page = HTTP.send(
                HttpRequest.newBuilder(
                                URI.create("http://127.0.0.1:" + platform.playgroundPort() + "/"))
                        .GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(page.statusCode()).isEqualTo(200);
        assertThat(page.body()).contains("Parser Playground");
    }

    @Test
    @Order(7)
    void theSearchConsoleServesAndBridgesTheSearchService() throws Exception {
        String base = "http://127.0.0.1:" + platform.searchConsolePort();
        HttpResponse<String> page = HTTP.send(
                HttpRequest.newBuilder(URI.create(base + "/")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(page.statusCode()).isEqualTo(200);
        assertThat(page.body()).contains("Search Console");

        // The guarded console signs in an access-policy principal and bridges with its session.
        HttpResponse<String> login = HTTP.send(
                HttpRequest.newBuilder(URI.create(base + "/session"))
                        .POST(HttpRequest.BodyPublishers.ofString(CONSOLE_CREDENTIAL)).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(login.statusCode()).as(login.body()).isEqualTo(200);
        consoleCookie = login.headers().firstValue("Set-Cookie").orElseThrow().split(";", 2)[0];

        HttpResponse<String> subjects = HTTP.send(
                HttpRequest.newBuilder(URI.create(base + "/subjects"))
                        .header("Cookie", consoleCookie).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(subjects.statusCode()).isEqualTo(200);
        JsonNode surface = MAPPER.readTree(subjects.body());
        assertThat(surface.path("subjects").get(0).path("subject").asText())
                .isEqualTo(RepoDocumentMapping.SUBJECT);

        HttpResponse<String> hits = HTTP.send(
                HttpRequest.newBuilder(URI.create(base + "/search"))
                        .header("Content-Type", "application/json")
                        .header("Cookie", consoleCookie)
                        .POST(HttpRequest.BodyPublishers.ofString(
                                "{\"mappingSubject\":\"" + RepoDocumentMapping.SUBJECT
                                        + "\",\"query\":\"container works\",\"k\":5,"
                                        + "\"lane\":\"SEARCH_LANE_LEXICAL\"}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(hits.statusCode()).isEqualTo(200);
        assertThat(MAPPER.readTree(hits.body()).path("hits").get(0).path("docId").asText())
                .isEqualTo(receipt.getDocId());

        // The operations panel rides the actions proxy onto the registry route.
        HttpResponse<String> jobs = HTTP.send(
                HttpRequest.newBuilder(URI.create(base + "/actions/list-jobs"))
                        .header("Content-Type", "application/json")
                        .header("Cookie", consoleCookie)
                        .POST(HttpRequest.BodyPublishers.ofString("{}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(jobs.statusCode()).isEqualTo(200);
        assertThat(MAPPER.readTree(jobs.body()).path("jobs").isArray()).isTrue();
    }

    @Test
    @Order(8)
    void theMetricServiceCountsTheCorpusOverTcpAndThroughTheCatalogVerbs() throws Exception {
        // The gRPC surface: the same live index the search service serves.
        ManagedChannel metricsChannel = operatorChannel(platform.metricsPort());
        try {
            QueryMetricsResponse answered = MetricServiceGrpc.newBlockingStub(metricsChannel)
                    .queryMetrics(QueryMetricsRequest.newBuilder()
                            .setMappingSubject(RepoDocumentMapping.SUBJECT)
                            .addMeasures("documents")
                            .setLimit(10)
                            .build());
            assertThat(answered.getRowsList()).hasSize(1);
            assertThat(answered.getRows(0).getMeasuresMap())
                    .containsEntry("documents", 1.0);
        } finally {
            metricsChannel.shutdownNow();
        }

        // The catalog verbs the metrics role contributed, over the registry's
        // actions route (the same surface MCP serves).
        ObjectNode describe = MAPPER.createObjectNode();
        describe.put("mappingSubject", RepoDocumentMapping.SUBJECT);
        JsonNode described = postAction("describe-mapping", describe);
        assertThat(described.path("members").toString())
                .contains("documents")
                .contains("document_type")
                .contains("language")
                .contains("category")
                .contains("processed_date");

        // The verb accepts a QueryMetricsRequest as its envelope, the same message the
        // MetricService RPC takes, so the payload is not wrapped.
        ObjectNode query = MAPPER.createObjectNode();
        query.put("mappingSubject", RepoDocumentMapping.SUBJECT);
        query.putArray("measures").add("documents");
        query.put("limit", 10);
        JsonNode counted = postAction("query-metrics", query);
        assertThat(counted.path("rows").get(0).path("measures").path("documents").asDouble())
                .isEqualTo(1.0);
    }

    @Test
    @Order(9)
    void deleteAndUnindexRemovesTheDocumentFromSearch() throws Exception {
        ObjectNode submit = MAPPER.createObjectNode();
        submit.put("workflowName", "delete-and-unindex");
        submit.set("input", MAPPER.readTree(JsonFormat.printer().print(
                DeleteAndUnindexRequest.newBuilder()
                        .setAddress(receipt.getAddress())
                        .setMappingSubject(RepoDocumentMapping.SUBJECT)
                        .build())));
        JsonNode submitted = postAction("submit-workflow", submit);
        assertThat(submitted.path("ok").asBoolean()).as(submitted.toString()).isTrue();
        String jobId = submitted.path("jobId").asText();

        String status = "";
        for (int i = 0; i < 120 && !"COMPLETED".equals(status); i++) {
            ObjectNode get = MAPPER.createObjectNode();
            get.put("jobId", jobId);
            JsonNode job = postAction("get-job", get);
            status = job.path("job").path("status").asText(job.path("status").asText());
            if ("FAILED".equals(status) || "DEAD".equals(status)) {
                throw new AssertionError("delete-and-unindex job " + status + ": " + job);
            }
            if (!"COMPLETED".equals(status)) {
                Thread.sleep(500);
            }
        }
        assertThat(status).isEqualTo("COMPLETED");

        // The document no longer answers the query that found it before.
        ManagedChannel searchChannel = operatorChannel(platform.searchPort());
        try {
            SearchResponse hits = SearchServiceGrpc.newBlockingStub(searchChannel)
                    .search(SearchRequest.newBuilder()
                            .setMappingSubject(RepoDocumentMapping.SUBJECT)
                            .setQuery("container works")
                            .setK(10)
                            .setLane(SearchLane.SEARCH_LANE_LEXICAL)
                            .build());
            assertThat(hits.getHitsList()).isEmpty();
        } finally {
            searchChannel.shutdownNow();
        }
    }

    @Test
    @Order(10)
    void replayPruneReconcilesTheIndexAgainstTheRepository() throws Exception {
        // Two fresh documents enter and index; one is then deleted straight
        // through repo's gRPC, BYPASSING delete-and-unindex — the drift
        // prune exists for.
        Metadata metadata = new Metadata();
        metadata.put(ApiKeyServerInterceptor.API_KEY, API_KEY);
        var intake = IntakeServiceGrpc.newBlockingStub(intakeChannel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
        IngestDocumentResponse keeper = intake.ingestDocument(IngestDocumentRequest.newBuilder()
                .setRaw(RawPayload.newBuilder()
                        .setData(ByteString.copyFromUtf8(
                                "Prune Keeper\n\nThe lighthouse keeper stays."))
                        .setFilename("keeper.txt")
                        .setMimeType("text/plain"))
                .setDatasourceId("ds-prune")
                .build());
        IngestDocumentResponse stale = intake.ingestDocument(IngestDocumentRequest.newBuilder()
                .setRaw(RawPayload.newBuilder()
                        .setData(ByteString.copyFromUtf8(
                                "Prune Stale\n\nThe drifting buoy vanishes."))
                        .setFilename("stale.txt")
                        .setMimeType("text/plain"))
                .setDatasourceId("ds-prune")
                .build());
        for (IngestDocumentResponse doc : java.util.List.of(keeper, stale)) {
            ObjectNode submit = MAPPER.createObjectNode();
            submit.put("workflowName", "parse-and-index");
            submit.set("input", MAPPER.readTree(JsonFormat.printer().print(
                    ParseAndIndexRequest.newBuilder()
                            .setAddress(doc.getAddress())
                            .setMappingSubject(RepoDocumentMapping.SUBJECT)
                            .build())));
            JsonNode submitted = postAction("submit-workflow", submit);
            String jobId = submitted.path("jobId").asText();
            String status = "";
            for (int i = 0; i < 120 && !"COMPLETED".equals(status); i++) {
                ObjectNode get = MAPPER.createObjectNode();
                get.put("jobId", jobId);
                JsonNode job = postAction("get-job", get);
                status = job.path("job").path("status").asText(job.path("status").asText());
                if ("FAILED".equals(status) || "DEAD".equals(status)) {
                    throw new AssertionError("prune-setup job " + status + ": " + job);
                }
                if (!"COMPLETED".equals(status)) {
                    Thread.sleep(500);
                }
            }
            assertThat(status).isEqualTo("COMPLETED");
        }

        DocumentServiceGrpc.newBlockingStub(repoChannel).deleteDocument(
                ai.protomolt.proto.repo.v1.DeleteDocumentRequest.newBuilder()
                        .setByReference(DeleteDocumentByReferenceCommand.newBuilder()
                                .setAddress(stale.getAddress()))
                        .build());

        ObjectNode reconcile = MAPPER.createObjectNode();
        reconcile.put("workflowName", "parse-and-index");
        reconcile.put("mappingSubject", RepoDocumentMapping.SUBJECT);
        reconcile.put("prune", true);
        JsonNode reconciled = postAction("replay-documents", reconcile);
        // The tombstoned row dropped out of the listing (so it did not
        // resubmit) and its index entry was pruned.
        assertThat(reconciled.path("submitted").asInt()).isGreaterThanOrEqualTo(1);
        assertThat(reconciled.path("pruned").asInt()).isGreaterThanOrEqualTo(1);

        ManagedChannel searchChannel = operatorChannel(platform.searchPort());
        try {
            SearchResponse pruned = SearchServiceGrpc.newBlockingStub(searchChannel)
                    .search(SearchRequest.newBuilder()
                            .setMappingSubject(RepoDocumentMapping.SUBJECT)
                            .setQuery("drifting buoy")
                            .setK(10)
                            .setLane(SearchLane.SEARCH_LANE_LEXICAL)
                            .build());
            assertThat(pruned.getHitsList()).isEmpty();
            SearchResponse kept = SearchServiceGrpc.newBlockingStub(searchChannel)
                    .search(SearchRequest.newBuilder()
                            .setMappingSubject(RepoDocumentMapping.SUBJECT)
                            .setQuery("lighthouse keeper")
                            .setK(10)
                            .setLane(SearchLane.SEARCH_LANE_LEXICAL)
                            .build());
            assertThat(kept.getHitsList()).isNotEmpty();
            assertThat(kept.getHits(0).getDocId()).isEqualTo(keeper.getDocId());
        } finally {
            searchChannel.shutdownNow();
        }
    }

    /** A TCP channel presenting the synthetic operator token to a guarded listener. */
    static ManagedChannel operatorChannel(int port) {
        Metadata identity = new Metadata();
        identity.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), OPERATOR_TOKEN);
        return NettyChannelBuilder.forAddress("127.0.0.1", port)
                .usePlaintext()
                .intercept(MetadataUtils.newAttachHeadersInterceptor(identity))
                .build();
    }

    static JsonNode postAction(String name, ObjectNode input) throws Exception {
        HttpResponse<String> response = HTTP.send(
                HttpRequest.newBuilder(registryBase.resolve("/protomolt/actions/" + name))
                        .header("Content-Type", "application/json")
                        .header("api_token", OPERATOR_TOKEN)
                        .POST(HttpRequest.BodyPublishers.ofString(input.toString()))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
        return MAPPER.readTree(response.body());
    }
}
