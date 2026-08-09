package ai.pipestream.proto.jobs.service.worker;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.chain.ChainRunner;
import ai.pipestream.proto.jobs.service.ChainJobSubmitter;
import ai.pipestream.proto.jobs.service.ChainJobsConfig;
import ai.pipestream.proto.jobs.service.ValidatingChains;
import ai.pipestream.proto.jobs.service.store.ChainJobEventRecord;
import ai.pipestream.proto.jobs.service.store.ChainJobRecord;
import ai.pipestream.proto.jobs.service.store.InMemoryChainJobStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.inprocess.InProcessChannelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The worker's VALIDATION verdict against declared response rules (the
 * {@link ValidatingChains} fixture): a step whose {@code validate} flag is set
 * has its response checked against the rules the output type declares — a
 * rejection is a verdict (FAILED with the violations, no retry), not a
 * retryable error.
 */
class ChainJobWorkerValidationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    ValidatingChains chains;
    InMemoryChainJobStore store;
    ChainJobWorker worker;
    ChainJobSubmitter submitter;
    ActionContext context;

    @BeforeEach
    void fresh() {
        chains = new ValidatingChains();
        store = new InMemoryChainJobStore();
        context = ActionContext.create();
        ChainJobsConfig config = new ChainJobsConfig("test-worker", 1,
                Duration.ofSeconds(30), Duration.ofMillis(50), 1, 3, 4,
                null, "chain-job-events", null, null);
        submitter = new ChainJobSubmitter(store, null, config.maxAttemptsDefault());
    }

    @AfterEach
    void stop() {
        chains.stop();
    }

    /** A worker whose channels reach the fixture's in-process server. */
    private ChainJobWorker workerTo(String inProcessName) {
        return new ChainJobWorker(store, context, null,
                new ChainRunner(step -> InProcessChannelBuilder.forName(inProcessName).build()),
                new ChainJobsConfig("test-worker", 1, Duration.ofSeconds(30),
                        Duration.ofMillis(50), 1, 3, 4, null, "chain-job-events", null, null));
    }

    private UUID submit() {
        ChainJobSubmitter.Outcome outcome = submitter.submit(
                chains.validatingTokenizeChain("in-process"), null,
                jsonInput(), null, context);
        assertThat(outcome.ok()).as("submit failed: %s", outcome.error()).isTrue();
        return UUID.fromString(outcome.jobId());
    }

    private static com.fasterxml.jackson.databind.JsonNode jsonInput() {
        try {
            return MAPPER.readTree("{\"text\": \"hi\"}");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    void aResponsePassingItsDeclaredRulesCompletes() throws Exception {
        String name = chains.startTokenizer("hello");
        worker = workerTo(name);
        UUID jobId = submit();

        assertThat(worker.workOnce()).isTrue();
        ChainJobRecord job = store.get(jobId).orElseThrow();
        assertThat(job.status).isEqualTo(ChainJobRecord.STATUS_COMPLETED);
        assertThat(MAPPER.readTree(job.result).get("tag").asText()).isEqualTo("hello");
        assertThat(eventTypes()).containsExactly(
                ChainJobEventRecord.TYPE_ACCEPTED,
                ChainJobEventRecord.TYPE_STEP_CHECKPOINT,
                ChainJobEventRecord.TYPE_COMPLETED);
    }

    @Test
    void aResponseFailingItsDeclaredRulesIsAVerdictNotARetry() throws Exception {
        String name = chains.startTokenizer("no");
        worker = workerTo(name);
        UUID jobId = submit();

        assertThat(worker.workOnce()).isTrue();
        ChainJobRecord job = store.get(jobId).orElseThrow();
        // VALIDATION is a verdict: FAILED with the violations, no requeue.
        assertThat(job.status).isEqualTo(ChainJobRecord.STATUS_FAILED);
        assertThat(job.error).contains("VALIDATION").contains("response failed validation")
                .contains("tag");
        assertThat(job.attempt).isEqualTo(1);
        assertThat(job.completedAt).isNotNull();
        assertThat(eventTypes()).containsExactly(
                ChainJobEventRecord.TYPE_ACCEPTED, ChainJobEventRecord.TYPE_FAILED);

        // A verdict never requeues: nothing is claimable.
        assertThat(worker.workOnce()).isFalse();
    }

    private List<String> eventTypes() {
        return store.events().stream().map(event -> event.eventType).toList();
    }
}
