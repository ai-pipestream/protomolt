package ai.protomolt.proto.systemtests;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.workflow.WorkflowRunner;
import ai.protomolt.proto.search.chunk.PolicyDerivation;
import ai.protomolt.proto.search.chunk.SentencePackedChunker;
import ai.protomolt.proto.search.embedding.EmbeddingProvider;
import ai.protomolt.proto.search.embedding.EmbeddingProviders;
import ai.protomolt.proto.search.embedding.model2vec.Model2VecEmbeddingProvider;
import ai.protomolt.proto.search.index.lucene.ProtoLuceneMapper;
import ai.protomolt.proto.search.index.spi.CatalogIndexingHintSource;
import ai.protomolt.proto.search.index.spi.ChunkingPolicy;
import ai.protomolt.proto.search.index.spi.IndexFieldKind;
import ai.protomolt.proto.search.index.spi.IndexMapping;
import ai.protomolt.proto.search.index.spi.IndexMappingFactory;
import ai.protomolt.proto.search.index.spi.ResolvedFieldHint;
import ai.protomolt.proto.search.index.spi.VectorSimilarity;
import ai.protomolt.proto.intake.service.IntakeServiceConfig;
import ai.protomolt.proto.intake.service.IntakeServices;
import ai.protomolt.proto.intake.service.identity.ApiKeyServerInterceptor;
import ai.protomolt.proto.intake.service.identity.InMemoryApiKeyIdentityResolver;
import ai.protomolt.proto.intake.service.identity.IntakeScope;
import ai.protomolt.proto.intake.v1.IngestDocumentRequest;
import ai.protomolt.proto.intake.v1.IngestDocumentResponse;
import ai.protomolt.proto.intake.v1.IntakeServiceGrpc;
import ai.protomolt.proto.intake.v1.RawPayload;
import ai.protomolt.proto.jobs.service.WorkflowRunSubmitter;
import ai.protomolt.proto.jobs.service.WorkflowRunsConfig;
import ai.protomolt.proto.jobs.service.store.WorkflowRunDatabase;
import ai.protomolt.proto.jobs.service.store.WorkflowRunRecord;
import ai.protomolt.proto.jobs.service.store.WorkflowRunStoreConfig;
import ai.protomolt.proto.jobs.service.store.JdbcWorkflowRunStore;
import ai.protomolt.proto.jobs.service.worker.WorkflowRunWorker;
import ai.protomolt.proto.parse.service.ParseWorkflows;
import ai.protomolt.proto.parse.service.ParseCoordinatorConfig;
import ai.protomolt.proto.parse.service.ParseCoordinatorServices;
import ai.protomolt.proto.parse.service.ParserRegistry;
import ai.protomolt.proto.parse.service.RoutingRules;
import ai.protomolt.proto.parse.text.TextParserService;
import ai.protomolt.proto.parse.v1.ParseDocumentRequest;
import ai.protomolt.proto.parse.v1.RoutingRule;
import ai.protomolt.proto.registry.GitSchemaRegistryStore;
import ai.protomolt.proto.registry.service.SchemaRegistryServer;
import ai.protomolt.proto.registry.service.SchemaRegistryServerConfig;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.service.RepoServiceConfig;
import ai.protomolt.proto.repo.service.RepoServices;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentPart;
import ai.protomolt.proto.repo.v1.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v1.DriveServiceGrpc;
import ai.protomolt.proto.repo.v1.DriveType;
import ai.protomolt.proto.repo.v1.GetDocumentByReferenceRequest;
import ai.protomolt.proto.schema.confluent.ConfluentSchemaPublisher;
import ai.protomolt.proto.seo.SeoIndexing;
import ai.protomolt.proto.seo.v1.Article;
import ai.protomolt.proto.seo.v1.DublinCore;
import ai.protomolt.proto.seo.v1.SearchStandard;
import ai.protomolt.proto.sources.ProtoSourceSet;
import ai.protomolt.proto.sources.publish.PublishOptions;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.MetadataUtils;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
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
 * THE golden path, end to end in one JVM (the all-in-one embedding path every
 * platform service supports): a court-corpus document enters the
 * authenticated intake service, a durable workflow run routes it through the
 * parsing coordinator to the reference text parser, the parsed result and
 * folded metadata persist in the repository, the document indexes into
 * Lucene under mappings carrying the search standard, and search — lexical and
 * vector — brings it back. The schema registry runs throughout and serves
 * the fleet document model as a published artifact.
 *
 * <p>Vector search runs the embedding lane end to end: a chunking policy
 * names the product-default model2vec provider, the ServiceLoader resolves
 * and validates it, and the policy derivation chunks the parsed body and
 * embeds every chunk. The model is a synthetic corpus-vocabulary Model2Vec
 * directory ({@link CorpusModel2Vec}), so the lane is proven with the real
 * provider while staying deterministic (model quality has its own gated
 * live suite).
 */
@Testcontainers(disabledWithoutDocker = true)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class GoldenPathSystemTest {

    static final String ACCOUNT = "acct-golden";
    static final String API_KEY = "golden-key";
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

    static RepoServices repo;
    static ManagedChannel repoChannel;
    static Server textParserServer;
    static ParseCoordinatorServices coordinator;
    static String coordinatorName;
    static IntakeServices intake;
    static ManagedChannel intakeChannel;
    static GitSchemaRegistryStore registry;
    static SchemaRegistryServer registryServer;
    static URI registryBase;
    static WorkflowRunDatabase jobsDatabase;
    static JdbcWorkflowRunStore jobs;
    static WorkflowRunWorker worker;
    static WorkflowRunSubmitter submitter;
    static ActionContext context;

    static String caseName;
    static String plainText;
    static IngestDocumentResponse receipt;
    static Document stored;

    @BeforeAll
    static void bootThePlatform() throws Exception {
        // Repository: the document store on real Postgres + S3.
        repo = RepoServices.build(new RepoServiceConfig(
                0,
                new LedgerConfig(REPO_DB.getJdbcUrl(), REPO_DB.getUsername(), REPO_DB.getPassword()),
                LOCALSTACK.getEndpoint().toString(),
                LOCALSTACK.getRegion(),
                LOCALSTACK.getAccessKey(),
                LOCALSTACK.getSecretKey(),
                "golden-docs",
                0,
                null, null, null, null, 0, 0L));
        repo.startInProcess("golden-repo");
        repoChannel = InProcessChannelBuilder.forName("golden-repo").build();
        DriveServiceGrpc.newBlockingStub(repoChannel).createDrive(CreateDriveRequest.newBuilder()
                .setName("intake")
                .setAccountId(ACCOUNT)
                .setDriveType(DriveType.DRIVE_TYPE_INTAKE)
                .build());

        // The reference parser, a separate service like any fleet member.
        String parserName = InProcessServerBuilder.generateName();
        textParserServer = InProcessServerBuilder.forName(parserName)
                .directExecutor()
                .addService(new TextParserService())
                .build()
                .start();

        // The parsing coordinator: routing rules are service config.
        coordinator = ParseCoordinatorServices.build(
                new ParseCoordinatorConfig(
                        0, ParseCoordinatorConfig.INPROCESS_TARGET_PREFIX + "golden-repo",
                        "intake", 60),
                RoutingRules.of(List.of(RoutingRule.newBuilder()
                        .setRuleId("r-text")
                        .setWhen("mime_type == 'text/plain'")
                        .setParserName(TextParserService.PARSER_NAME)
                        .setPriority(10)
                        .build())),
                ParserRegistry.of(Map.of(
                        TextParserService.PARSER_NAME,
                        ParseCoordinatorConfig.INPROCESS_TARGET_PREFIX + parserName)));
        coordinatorName = InProcessServerBuilder.generateName();
        coordinator.startInProcess(coordinatorName);

        // The intake service, API-key authenticated.
        intake = IntakeServices.build(
                new IntakeServiceConfig(
                        0,
                        IntakeServiceConfig.INPROCESS_TARGET_PREFIX + "golden-repo",
                        IntakeServiceConfig.DEFAULT_MAX_PAYLOAD_BYTES),
                new InMemoryApiKeyIdentityResolver()
                        .register(API_KEY, IntakeScope.unrestricted(ACCOUNT)));
        intake.startInProcess("golden-intake");
        intakeChannel = InProcessChannelBuilder.forName("golden-intake").build();

        // The schema registry, on for the whole run.
        registry = GitSchemaRegistryStore.builder()
                .repositoryDir(work.resolve("registry.git"))
                .build();
        registryServer = new SchemaRegistryServer(
                SchemaRegistryServerConfig.defaults().withHost("127.0.0.1").withPort(0), registry);
        registryBase = URI.create("http://127.0.0.1:" + registryServer.start());

        // The durable executor on its own Postgres.
        jobsDatabase = new WorkflowRunDatabase(new WorkflowRunStoreConfig(
                JOBS_DB.getJdbcUrl(), JOBS_DB.getUsername(), JOBS_DB.getPassword(),
                WorkflowRunStoreConfig.DEFAULT_POOL_SIZE,
                WorkflowRunStoreConfig.DEFAULT_MIGRATION_LOCATION));
        jobs = new JdbcWorkflowRunStore(jobsDatabase);
        context = ActionContext.create();
        // The parse checkpoint carries the parser's docling document as an
        // Any; the checkpoint transcoder resolves it through this registry.
        context.registry().registerFile(ai.protomolt.proto.parse.document.v1.DocumentProto.getDescriptor());
        WorkflowRunsConfig jobsConfig = new WorkflowRunsConfig(
                "golden-worker", 1, Duration.ofMinutes(1), Duration.ofMillis(50),
                0, 3, 4, null, WorkflowRunsConfig.DEFAULT_EVENTS_TOPIC, null, null);
        WorkflowRunner runner = new WorkflowRunner(step -> InProcessChannelBuilder
                .forName(step.target().substring(
                        ParseCoordinatorConfig.INPROCESS_TARGET_PREFIX.length()))
                .build());
        submitter = new WorkflowRunSubmitter(jobs, null, jobsConfig.maxAttemptsDefault());
        worker = new WorkflowRunWorker(jobs, context, null, runner, jobsConfig);

        // The court corpus fixture (from the samples jar).
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                GoldenPathSystemTest.class.getClassLoader()
                        .getResourceAsStream("fixtures/court/opinions_sample.jsonl"),
                StandardCharsets.UTF_8))) {
            JsonNode opinion = MAPPER.readTree(reader.readLine());
            caseName = opinion.path("case_name").asText();
            plainText = opinion.path("plain_text").asText();
        }
        assertThat(caseName).isNotBlank();
        assertThat(plainText).isNotBlank();
    }

    @AfterAll
    static void shutdown() {
        if (intakeChannel != null) {
            intakeChannel.shutdownNow();
        }
        if (intake != null) {
            intake.close();
        }
        if (coordinator != null) {
            coordinator.close();
        }
        if (textParserServer != null) {
            textParserServer.shutdownNow();
        }
        if (registryServer != null) {
            registryServer.close();
        }
        if (registry != null) {
            registry.close();
        }
        if (jobsDatabase != null) {
            jobsDatabase.close();
        }
        if (repoChannel != null) {
            repoChannel.shutdownNow();
        }
        if (repo != null) {
            repo.close();
        }
    }

    @Test
    @Order(1)
    void theRegistryServesTheFleetDocumentModelAsAPublishedArtifact() throws Exception {
        String documentProto = Files.readString(
                Path.of("..", "..", "parse", "document", "src", "main", "proto",
                        "ai", "pipestream", "proto", "parse", "document", "v1", "document.proto"));
        ProtoSourceSet sources = ProtoSourceSet.builder()
                .add("ai/pipestream/proto/parse/document/v1/document.proto", documentProto, "fleet")
                .build();
        try (ConfluentSchemaPublisher publisher = new ConfluentSchemaPublisher(registryBase)) {
            publisher.publish(sources, PublishOptions.defaults()).throwIfFailed();
        }
        assertThat(registry.subjects()).contains("ai/pipestream/proto/parse/document/v1/document.proto");

        HttpResponse<byte[]> descriptorSet = HTTP.send(
                HttpRequest.newBuilder(registryBase.resolve(
                                "/protomolt/subjects/ai%2Fpipestream%2Fproto%2Fparse%2Fdocument%2Fv1%2Fdocument.proto/descriptor-set"))
                        .timeout(Duration.ofSeconds(10)).GET().build(),
                HttpResponse.BodyHandlers.ofByteArray());
        assertThat(descriptorSet.statusCode()).isEqualTo(200);
        FileDescriptorSet parsed = FileDescriptorSet.parseFrom(descriptorSet.body());
        assertThat(parsed.getFileList())
                .anySatisfy(file -> assertThat(file.getMessageTypeList())
                        .extracting(m -> m.getName())
                        .contains("Document"));
    }

    @Test
    @Order(2)
    void theCourtDocumentEntersThroughTheAuthenticatedIntakeService() {
        Metadata metadata = new Metadata();
        metadata.put(ApiKeyServerInterceptor.API_KEY, API_KEY);
        String payload = caseName + "\n\n" + plainText;
        receipt = IntakeServiceGrpc.newBlockingStub(intakeChannel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
                .ingestDocument(IngestDocumentRequest.newBuilder()
                        .setRaw(RawPayload.newBuilder()
                                .setData(ByteString.copyFromUtf8(payload))
                                .setFilename("opinion-1.txt")
                                .setMimeType("text/plain"))
                        .setDatasourceId("ds-court")
                        .build());
        assertThat(receipt.getDocId()).isNotBlank();
        assertThat(receipt.getAddress().getGraphId()).isEqualTo("intake:" + ACCOUNT);
    }

    @Test
    @Order(3)
    void aDurableJobParsesItThroughTheCoordinator() throws Exception {
        String inputJson = JsonFormat.printer().print(
                ParseDocumentRequest.newBuilder().setAddress(receipt.getAddress()).build());
        WorkflowRunSubmitter.Outcome outcome = submitter.submit(
                ParseWorkflows.parseDocumentWorkflow(
                        ParseCoordinatorConfig.INPROCESS_TARGET_PREFIX + coordinatorName, 60_000),
                null,
                MAPPER.readTree(inputJson),
                null,
                context);
        assertThat(outcome.ok()).as(outcome.toString()).isTrue();

        assertThat(worker.workOnce()).isTrue();
        WorkflowRunRecord job = jobs.get(UUID.fromString(outcome.jobId())).orElseThrow();
        assertThat(job.status).as("job error: " + job.error).isEqualTo(WorkflowRunRecord.STATUS_COMPLETED);

        stored = DocumentServiceGrpc.newBlockingStub(repoChannel)
                .getDocumentByReference(GetDocumentByReferenceRequest.newBuilder()
                        .setAddress(receipt.getAddress())
                        .addParts(DocumentPart.DOCUMENT_PART_CORE)
                        .addParts(DocumentPart.DOCUMENT_PART_PARSED)
                        .build())
                .getDocument();
        assertThat(stored.getParserResultsMap()).containsKey(TextParserService.PARSER_NAME);
        // The fold arbitrated the parser's title claim into CORE metadata.
        assertThat(stored.getSearchMetadata().getTitle()).isEqualTo(caseName);
    }

    @Test
    @Order(4)
    void theDocumentIndexesUnderTheStandardAndComesBackFromSearch() throws Exception {
        // Repo-document mapping: identity + searchable text.
        CatalogIndexingHintSource catalog = new CatalogIndexingHintSource();
        catalog.put(Document.getDescriptor().getFullName(), "doc_id",
                ResolvedFieldHint.of(IndexFieldKind.KEYWORD));
        catalog.put(Document.getDescriptor().getFullName(), "search_metadata",
                ResolvedFieldHint.of(IndexFieldKind.TEXT));
        catalog.put("ai.pipestream.proto.repo.v1.SearchMetadata", "title",
                ResolvedFieldHint.builder(IndexFieldKind.TEXT).stored(true).build());
        catalog.put("ai.pipestream.proto.repo.v1.SearchMetadata", "body",
                ResolvedFieldHint.builder(IndexFieldKind.TEXT).stored(true).build());
        // Storage and provenance planes stay out of the index.
        for (String skipped : new String[] {
                "parser_results", "blob_bag", "structured_data", "ownership", "doc_id_derivation"}) {
            catalog.put(Document.getDescriptor().getFullName(), skipped, ResolvedFieldHint.skipped());
        }
        for (String skipped : new String[] {"semantic_results", "custom_fields", "metadata"}) {
            catalog.put("ai.pipestream.proto.repo.v1.SearchMetadata", skipped,
                    ResolvedFieldHint.skipped());
        }
        IndexMapping documentMapping =
                IndexMappingFactory.defaults(catalog).create(Document.getDescriptor());

        // Body text from the parsed result, folded onto CORE by the coordinator.
        String body = stored.getSearchMetadata().getBody();
        Document indexable = stored.toBuilder()
                .setSearchMetadata(stored.getSearchMetadata().toBuilder().setBody(
                        body.isBlank() ? plainText : body))
                .build();

        // The search standard, populated from the fold, mapped by ITS mapping.
        SearchStandard standard = SearchStandard.newBuilder()
                .setDublinCore(DublinCore.newBuilder()
                        .setTitle(stored.getSearchMetadata().getTitle())
                        .setLanguage("en"))
                .setArticle(Article.newBuilder()
                        .setHeadline(stored.getSearchMetadata().getTitle())
                        .setDatePublished(Timestamp.newBuilder().setSeconds(1265760000L)))
                .build();
        IndexMapping seoMapping = SeoIndexing.mappingFor(SearchStandard.getDescriptor());

        // The chunking policy a shape would carry, naming the product-default
        // model2vec provider. The lane resolves the provider through the
        // ServiceLoader from the policy alone and validates it before any text.
        ChunkingPolicy policy = new ChunkingPolicy(
                new ChunkingPolicy.ChunkingSpec(
                        SentencePackedChunker.STRATEGY, SentencePackedChunker.STRATEGY_VERSION,
                        120, 16, 20, 200, SentencePackedChunker.BOUNDARY),
                new ChunkingPolicy.EmbeddingSpec(
                        Model2VecEmbeddingProvider.PROVIDER_ID, CorpusModel2Vec.DIMENSION,
                        VectorSimilarity.COSINE, true),
                "", true);
        Path model = Files.createDirectories(work.resolve("model2vec"));
        CorpusModel2Vec.write(model, indexable.getSearchMetadata().getBody());
        EmbeddingProvider embedder;
        System.setProperty(Model2VecEmbeddingProvider.PATH_PROPERTY, model.toString());
        try {
            embedder = EmbeddingProviders.forSpec(policy.embedding());
        } finally {
            System.clearProperty(Model2VecEmbeddingProvider.PATH_PROPERTY);
        }
        List<PolicyDerivation.DerivedChunk> derived = new PolicyDerivation(embedder)
                .derive(indexable.getSearchMetadata().getBody(), policy);
        assertThat(derived.size()).as("the opinion body chunks").isGreaterThan(1);

        // Chunk identity is <doc_id>#<generation>#<ordinal>; the generation is
        // the policy digest (a policy change is a data change, a new chunk set).
        String generation = policy.digest().substring(0, 12);
        // The policy's empty vectorField means the engine convention
        // "<field>#<model>" over the embedded source field.
        String vectorField = "search_metadata_body#" + policy.embedding().model();

        Path indexDir = work.resolve("lucene");
        ProtoLuceneMapper mapper = new ProtoLuceneMapper(
                new ai.protomolt.proto.mapper.ProtoFieldMapperImpl(
                        new ai.protomolt.proto.descriptors.DescriptorRegistry()));
        try (IndexWriter writer = new IndexWriter(
                FSDirectory.open(indexDir), new IndexWriterConfig(new StandardAnalyzer()))) {
            // One Lucene block: the chunk children first, the parent last.
            List<org.apache.lucene.document.Document> block = new ArrayList<>();
            for (PolicyDerivation.DerivedChunk chunk : derived) {
                org.apache.lucene.document.Document child = new org.apache.lucene.document.Document();
                child.add(new StringField("chunk_id",
                        indexable.getDocId() + "#" + generation + "#" + chunk.chunk().ordinal(),
                        Field.Store.YES));
                child.add(new StringField("doc_id", indexable.getDocId(), Field.Store.YES));
                if (policy.storeChunkText()) {
                    child.add(new StoredField("chunk_text", chunk.chunk().text()));
                }
                child.add(new KnnFloatVectorField(vectorField, chunk.vector(),
                        VectorSimilarityFunction.COSINE));
                block.add(child);
            }
            org.apache.lucene.document.Document luceneDoc = mapper.map(indexable, documentMapping);
            // Fold the standard's fields into the same indexed document.
            for (var field : mapper.map(standard, seoMapping).getFields()) {
                luceneDoc.add(field);
            }
            block.add(luceneDoc);
            writer.addDocuments(block);
            writer.commit();
        }

        try (DirectoryReader reader = DirectoryReader.open(FSDirectory.open(indexDir))) {
            IndexSearcher searcher = new IndexSearcher(reader);
            // Lexical: a token from the case name through the analyzed title.
            String token = caseName.toLowerCase().replaceAll("[^a-z ]", " ").trim().split("\\s+")[0];
            TopDocs byTitle = searcher.search(
                    new TermQuery(new Term("search_metadata_title", token)), 5);
            assertThat(byTitle.totalHits.value()).isGreaterThan(0);
            // The standard's fields are searchable in the same index.
            TopDocs byLanguage = searcher.search(
                    new TermQuery(new Term("dublin_core_language", "en")), 5);
            assertThat(byLanguage.totalHits.value()).isGreaterThan(0);
            // Vector: a query embedded by the same provider ranks the chunk it
            // was drawn from first, and the chunk carries its parent identity.
            PolicyDerivation.DerivedChunk probe = derived.get(derived.size() / 2);
            TopDocs byVector = searcher.search(
                    new KnnFloatVectorQuery(vectorField,
                            embedder.embed(probe.chunk().text()), 3),
                    3);
            assertThat(byVector.totalHits.value()).isGreaterThan(0);
            org.apache.lucene.document.Document nearest =
                    searcher.storedFields().document(byVector.scoreDocs[0].doc);
            assertThat(nearest.get("chunk_id")).isEqualTo(
                    indexable.getDocId() + "#" + generation + "#" + probe.chunk().ordinal());
            assertThat(nearest.get("doc_id")).isEqualTo(indexable.getDocId());
            assertThat(nearest.get("chunk_text")).isEqualTo(probe.chunk().text());
        }
    }
}
