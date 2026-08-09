package ai.pipestream.proto.jobs.service;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.chain.ChainRepository;
import ai.pipestream.proto.jobs.service.store.ChainJobEventRecord;
import ai.pipestream.proto.jobs.service.store.ChainJobRecord;
import ai.pipestream.proto.jobs.service.store.InMemoryChainJobStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The submit path directly (no verb envelope): chain resolution (inline,
 * stored name, missing repository), the parse/verify/input-validation
 * failures that come back as verdicts, the jobId idempotency key, and the
 * row/event the happy path persists. Submission never executes anything, so
 * no gRPC server is involved.
 */
class ChainJobSubmitterTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    static TestChains chains;
    static ActionContext context;

    InMemoryChainJobStore store;
    ChainJobSubmitter submitter;

    @BeforeAll
    static void compileFixture() {
        chains = new TestChains();
        context = ActionContext.create();
    }

    @BeforeEach
    void fresh() {
        store = new InMemoryChainJobStore();
        submitter = new ChainJobSubmitter(store, null, 3);
    }

    private static JsonNode json(String text) {
        try {
            return MAPPER.readTree(text);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    void anInlineChainQueuesTheRowWithItsDeclaredName() {
        ChainJobSubmitter.Outcome outcome = submitter.submit(
                chains.twoStepChain("in-process", null), null, json("{\"text\": \"hi\"}"),
                null, context);

        assertThat(outcome.ok()).as("error: %s", outcome.error()).isTrue();
        assertThat(outcome.jobId()).isNotBlank();
        assertThat(outcome.status()).isEqualTo(ChainJobRecord.STATUS_QUEUED);
        assertThat(outcome.error()).isNull();

        ChainJobRecord row = store.get(UUID.fromString(outcome.jobId())).orElseThrow();
        assertThat(row.chainName).isEqualTo("embed-text");
        assertThat(row.status).isEqualTo(ChainJobRecord.STATUS_QUEUED);
        assertThat(row.maxAttempts).isEqualTo(3);
        assertThat(row.attempt).isZero();
        assertThat(row.runAfter).isNotNull();
        assertThat(row.checkpoints).isEqualTo("[]");
        assertThat(row.input).contains("\"text\":\"hi\"");
        assertThat(row.chainDefinition).contains("tokenize");

        // The ACCEPTED event commits with the row.
        assertThat(store.events()).hasSize(1);
        assertThat(store.events().get(0).eventType)
                .isEqualTo(ChainJobEventRecord.TYPE_ACCEPTED);
        assertThat(store.events().get(0).kafkaKey).isEqualTo(outcome.jobId());
    }

    @Test
    void aClientJobIdIsTheIdempotencyKey() {
        String jobId = UUID.randomUUID().toString();
        ObjectNode chain = chains.twoStepChain("in-process", null);
        ChainJobSubmitter.Outcome first = submitter.submit(chain, null,
                json("{\"text\": \"hi\"}"), jobId, context);
        assertThat(first.ok()).isTrue();
        assertThat(first.jobId()).isEqualTo(jobId);

        // A resubmit (even with extra whitespace around the id) returns the
        // existing row and writes nothing.
        ChainJobSubmitter.Outcome second = submitter.submit(chain, null,
                json("{\"text\": \"hi\"}"), " " + jobId + " ", context);
        assertThat(second.ok()).isTrue();
        assertThat(second.jobId()).isEqualTo(jobId);
        assertThat(store.events()).hasSize(1);
    }

    @Test
    void aBlankJobIdMintsOne() {
        ChainJobSubmitter.Outcome outcome = submitter.submit(
                chains.twoStepChain("in-process", null), null, json("{\"text\": \"hi\"}"),
                "   ", context);
        assertThat(outcome.ok()).isTrue();
        assertThat(UUID.fromString(outcome.jobId())).isNotNull();
    }

    @Test
    void aNonUuidJobIdFails() {
        ChainJobSubmitter.Outcome outcome = submitter.submit(
                chains.twoStepChain("in-process", null), null, json("{\"text\": \"hi\"}"),
                "not-a-uuid", context);
        assertThat(outcome.ok()).isFalse();
        assertThat(outcome.error()).contains("'jobId' must be a uuid").contains("not-a-uuid");
        assertThat(store.list(null, null, 10, 0)).isEmpty();
    }

    @Test
    void aMissingChainOrInputFailsCleanly() {
        // Neither inline chain nor chain name.
        ChainJobSubmitter.Outcome nothing = submitter.submit(null, null,
                json("{\"text\": \"hi\"}"), null, context);
        assertThat(nothing.ok()).isFalse();
        assertThat(nothing.error()).contains("'chain' (or 'chainName') and 'input'");
        assertThat(nothing.failedStep()).isEmpty();

        // A blank chain name is no chain name.
        ChainJobSubmitter.Outcome blank = submitter.submit(null, "  ",
                json("{\"text\": \"hi\"}"), null, context);
        assertThat(blank.ok()).isFalse();
        assertThat(blank.error()).contains("'chain' (or 'chainName') and 'input'");

        // A chain without its input object.
        ChainJobSubmitter.Outcome noInput = submitter.submit(
                chains.twoStepChain("in-process", null), null, null, null, context);
        assertThat(noInput.ok()).isFalse();
        assertThat(noInput.error()).contains("'chain' (or 'chainName') and 'input'");

        // A scalar input is not an input object.
        ChainJobSubmitter.Outcome scalarInput = submitter.submit(
                chains.twoStepChain("in-process", null), null, json("\"just a string\""),
                null, context);
        assertThat(scalarInput.ok()).isFalse();
        assertThat(scalarInput.error()).contains("'chain' (or 'chainName') and 'input'");

        assertThat(store.list(null, null, 10, 0)).isEmpty();
    }

    @Test
    void aStoredNameNeedsAMountedRepositoryAndAKnownName() {
        // No repository at all.
        ChainJobSubmitter.Outcome noRepo = submitter.submit(null, "embed-text",
                json("{\"text\": \"hi\"}"), null, context);
        assertThat(noRepo.ok()).isFalse();
        assertThat(noRepo.error()).contains("No chain repository is mounted");

        // A repository that does not know the name.
        ChainJobSubmitter withRepo = new ChainJobSubmitter(store,
                (ChainRepository) name -> Optional.empty(), 3);
        ChainJobSubmitter.Outcome unknown = withRepo.submit(null, "court-decoration",
                json("{\"text\": \"hi\"}"), null, context);
        assertThat(unknown.ok()).isFalse();
        assertThat(unknown.error()).contains("No stored chain named 'court-decoration'");

        // A repository that resolves it — under a name that is not the
        // chain's declared name, so the row proves which one wins.
        ObjectNode stored = chains.twoStepChain("in-process", null);
        ChainJobSubmitter resolving = new ChainJobSubmitter(store,
                (ChainRepository) name -> "court-decoration".equals(name)
                        ? Optional.of(stored) : Optional.empty(), 5);
        ChainJobSubmitter.Outcome resolved = resolving.submit(null, "court-decoration",
                json("{\"text\": \"hi\"}"), null, context);
        assertThat(resolved.ok()).as("error: %s", resolved.error()).isTrue();
        ChainJobRecord row = store.get(UUID.fromString(resolved.jobId())).orElseThrow();
        // The stored name (not the chain's declared name) stamps the row.
        assertThat(row.chainName).isEqualTo("court-decoration");
        assertThat(row.maxAttempts).isEqualTo(5);
    }

    @Test
    void aChainThatDoesNotParseFailsWithTheStepName() {
        ObjectNode broken = chains.twoStepChain("in-process", null);
        ((ObjectNode) broken.get("steps").get(1)).remove("method");
        ChainJobSubmitter.Outcome outcome = submitter.submit(broken, null,
                json("{\"text\": \"hi\"}"), null, context);
        assertThat(outcome.ok()).isFalse();
        assertThat(outcome.failedStep()).isEqualTo("embed");
        assertThat(outcome.error()).contains("'target' and 'method'");
        assertThat(store.list(null, null, 10, 0)).isEmpty();
    }

    @Test
    void anInputThatIsNotProto3JsonForTheInputTypeFails() {
        // An object for a string field: the parser tolerates unknown fields
        // and coerces scalars (ignoringUnknownFields, protobuf leniency), but
        // never a JSON object where a string belongs.
        ChainJobSubmitter.Outcome outcome = submitter.submit(
                chains.twoStepChain("in-process", null), null,
                json("{\"text\": {\"nested\": true}}"), null, context);
        assertThat(outcome.ok()).isFalse();
        assertThat(outcome.error()).contains("'input' is not valid proto3 JSON")
                .contains("jobs.test.Text");
        assertThat(store.list(null, null, 10, 0)).isEmpty();
    }

    @Test
    void anUnnamedInlineChainIsStampedInline() {
        ObjectNode chain = chains.twoStepChain("in-process", null);
        chain.remove("name");
        ChainJobSubmitter.Outcome outcome = submitter.submit(chain, null,
                json("{\"text\": \"hi\"}"), null, context);
        assertThat(outcome.ok()).as("error: %s", outcome.error()).isTrue();
        assertThat(store.get(UUID.fromString(outcome.jobId())).orElseThrow().chainName)
                .isEqualTo("inline");
    }
}
