package ai.pipestream.proto.parse.service;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.workflow.WorkflowRunner;
import ai.pipestream.proto.jobs.service.WorkflowRunSubmitter;
import ai.pipestream.proto.jobs.service.WorkflowRunsConfig;
import ai.pipestream.proto.jobs.service.store.WorkflowRunDatabase;
import ai.pipestream.proto.jobs.service.store.WorkflowRunRecord;
import ai.pipestream.proto.jobs.service.store.WorkflowRunStoreConfig;
import ai.pipestream.proto.jobs.service.store.JdbcWorkflowRunStore;
import ai.pipestream.proto.jobs.service.worker.WorkflowRunWorker;
import ai.pipestream.proto.parse.v1.ParseDocumentRequest;
import ai.pipestream.proto.repo.v1.Blob;
import ai.pipestream.proto.repo.v1.BlobBag;
import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.repo.v1.NodeAddress;
import ai.pipestream.proto.repo.v1.OwnershipContext;
import ai.pipestream.proto.parse.v1.RoutingRule;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Proves a parse runs as a DURABLE JOB: the parse-document workflow submitted
 * to the jobs store (real Postgres), claimed and executed by a
 * {@link WorkflowRunWorker}, its step response checkpointed on the job row, a
 * transient coordinator failure requeued and retried to completion, and the
 * parse's side effect (the coordinator's PARSED save) landing exactly once
 * per successful execution.
 */
@Testcontainers(disabledWithoutDocker = true)
class DurableParseJobIT {

    static final String ACCOUNT = "acct-durable";
    static final NodeAddress ADDRESS = NodeAddress.newBuilder()
            .setDocId("doc-durable-1")
            .setGraphAddressId("ds-d")
            .setAccountId(ACCOUNT)
            .setGraphId("intake:" + ACCOUNT)
            .build();
    static final ObjectMapper MAPPER = new ObjectMapper();

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    static FakeDocumentService repo;
    static Server repoServer;
    static FakeParserPlugin parser;
    static Server parserServer;
    static ParseCoordinatorServices coordinator;
    static String coordinatorName;

    static WorkflowRunDatabase database;
    static JdbcWorkflowRunStore store;
    static WorkflowRunSubmitter submitter;
    static WorkflowRunWorker worker;
    static ActionContext context;

    @BeforeAll
    static void boot() throws Exception {
        repo = new FakeDocumentService();
        String repoName = InProcessServerBuilder.generateName();
        repoServer = InProcessServerBuilder.forName(repoName)
                .directExecutor()
                .addService(repo)
                .build()
                .start();

        parser = new FakeParserPlugin("alpha", "1.0");
        String parserName = InProcessServerBuilder.generateName();
        parserServer = InProcessServerBuilder.forName(parserName)
                .directExecutor()
                .addService(parser)
                .build()
                .start();

        coordinator = ParseCoordinatorServices.build(
                new ParseCoordinatorConfig(
                        0, ParseCoordinatorConfig.INPROCESS_TARGET_PREFIX + repoName, "intake", 30),
                RoutingRules.of(List.of(RoutingRule.newBuilder()
                        .setRuleId("r-any")
                        .setWhen("true")
                        .setParserName("alpha")
                        .setPriority(1)
                        .build())),
                ParserRegistry.of(Map.of(
                        "alpha", ParseCoordinatorConfig.INPROCESS_TARGET_PREFIX + parserName)));
        coordinatorName = InProcessServerBuilder.generateName();
        coordinator.startInProcess(coordinatorName);

        database = new WorkflowRunDatabase(new WorkflowRunStoreConfig(
                POSTGRES.getJdbcUrl(),
                POSTGRES.getUsername(),
                POSTGRES.getPassword(),
                WorkflowRunStoreConfig.DEFAULT_POOL_SIZE,
                WorkflowRunStoreConfig.DEFAULT_MIGRATION_LOCATION));
        store = new JdbcWorkflowRunStore(database);
        context = ActionContext.create();
        WorkflowRunsConfig config = new WorkflowRunsConfig(
                "durable-parse-it",
                1,
                Duration.ofSeconds(30),
                Duration.ofMillis(50),
                0,      // zero backoff base: retries are immediately claimable
                3,
                4,
                null,
                WorkflowRunsConfig.DEFAULT_EVENTS_TOPIC,
                null,
                null);
        WorkflowRunner runner = new WorkflowRunner(step -> InProcessChannelBuilder
                .forName(step.target().substring(
                        ParseCoordinatorConfig.INPROCESS_TARGET_PREFIX.length()))
                .build());
        submitter = new WorkflowRunSubmitter(store, null, config.maxAttemptsDefault());
        worker = new WorkflowRunWorker(store, context, null, runner, config);
    }

    @AfterAll
    static void shutdown() {
        if (coordinator != null) {
            coordinator.close();
        }
        if (parserServer != null) {
            parserServer.shutdownNow();
        }
        if (repoServer != null) {
            repoServer.shutdownNow();
        }
        if (database != null) {
            database.close();
        }
    }

    @BeforeEach
    void reset() {
        repo.saves.clear();
        repo.failReadsRemaining.set(0);
        repo.seed(ADDRESS, Document.newBuilder()
                .setDocId(ADDRESS.getDocId())
                .setOwnership(OwnershipContext.newBuilder()
                        .setAccountId(ACCOUNT)
                        .setDatasourceId(ADDRESS.getGraphAddressId()))
                .setBlobBag(BlobBag.newBuilder().setBlob(Blob.newBuilder()
                        .setBlobId("b-1")
                        .setData(ByteString.copyFromUtf8("plain text payload"))
                        .setMimeType("text/plain")
                        .setFilename("durable.txt")))
                .build());
    }

    static UUID submitParseJob() throws Exception {
        String inputJson = JsonFormat.printer().print(
                ParseDocumentRequest.newBuilder().setAddress(ADDRESS).build());
        JsonNode input = MAPPER.readTree(inputJson);
        WorkflowRunSubmitter.Outcome outcome = submitter.submit(
                ParseWorkflows.parseDocumentWorkflow(
                        ParseCoordinatorConfig.INPROCESS_TARGET_PREFIX + coordinatorName, 30_000),
                null,
                input,
                null,
                context);
        assertThat(outcome.ok()).as(outcome.toString()).isTrue();
        return UUID.fromString(outcome.jobId());
    }

    @Test
    void aParseRunsAsACheckpointedJobToCompletion() throws Exception {
        UUID jobId = submitParseJob();

        assertThat(worker.workOnce()).isTrue();

        WorkflowRunRecord job = store.get(jobId).orElseThrow();
        assertThat(job.status).isEqualTo(WorkflowRunRecord.STATUS_COMPLETED);
        // The parse step's full response is checkpointed on the row.
        assertThat(job.checkpoints).contains("\"parse\"").contains("parserResults");
        assertThat(job.result).contains("alpha");
        // The parse's side effect landed exactly once.
        assertThat(repo.saves).hasSize(1);
        assertThat(repo.saves.getFirst().getDocument().getParserResultsMap()).containsKey("alpha");
    }

    @Test
    void aTransientCoordinatorFailureRequeuesAndRetriesToCompletion() throws Exception {
        repo.failReadsRemaining.set(1);
        UUID jobId = submitParseJob();

        // First execution hits the injected UNAVAILABLE read: requeued, not dead.
        assertThat(worker.workOnce()).isTrue();
        WorkflowRunRecord afterFailure = store.get(jobId).orElseThrow();
        assertThat(afterFailure.status).isEqualTo(WorkflowRunRecord.STATUS_QUEUED);
        assertThat(afterFailure.attempt).isEqualTo(1);
        assertThat(repo.saves).isEmpty();

        // The retry (zero backoff base) completes the parse. run_after is
        // stamped from the JVM clock and claimed against the database clock,
        // so eligibility can lag by a few milliseconds: poll, bounded.
        boolean retried = false;
        for (int i = 0; i < 100 && !retried; i++) {
            retried = worker.workOnce();
            if (!retried) {
                Thread.sleep(50);
            }
        }
        assertThat(retried).isTrue();
        WorkflowRunRecord done = store.get(jobId).orElseThrow();
        assertThat(done.status).isEqualTo(WorkflowRunRecord.STATUS_COMPLETED);
        assertThat(repo.saves).hasSize(1);
    }
}
