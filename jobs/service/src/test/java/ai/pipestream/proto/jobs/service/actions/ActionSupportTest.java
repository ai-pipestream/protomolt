package ai.pipestream.proto.jobs.service.actions;

import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.jobs.service.store.WorkflowRunRecord;
import ai.pipestream.proto.jobs.service.store.InMemoryWorkflowRunStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The verbs' shared plumbing directly: the null-store "unavailable" gate, the
 * envelope field validators (required/optional strings, objects, ints,
 * offsets), and the row → JSON mapping in its full (get-job) and summary
 * (list-jobs) shapes.
 */
class ActionSupportTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static ObjectNode envelope(String json) {
        try {
            return (ObjectNode) MAPPER.readTree(json);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static void assertInvalidInput(Throwable thrown, String messagePart) {
        assertThat(thrown).isInstanceOfSatisfying(ActionException.class, e -> {
            assertThat(e.code()).isEqualTo("invalid-input");
            assertThat(e.getMessage()).contains(messagePart);
        });
    }

    @Test
    void theNullStoreGateAnswersUnavailableWithTheDocumentedMessage() throws Exception {
        assertThatThrownBy(() -> ActionSupport.requireStore(null))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("unavailable");
                    assertThat(e.getMessage()).isEqualTo(ActionSupport.UNAVAILABLE_MESSAGE);
                });
        InMemoryWorkflowRunStore store = new InMemoryWorkflowRunStore();
        assertThat(ActionSupport.requireStore(store)).isSameAs(store);
    }

    @Test
    void requireStringRejectsAbsentNullAndNonTextualValues() throws Exception {
        ObjectNode input = envelope("{\"present\": \"value\", \"nul\": null, \"num\": 5}");
        assertThat(ActionSupport.requireString(input, "present")).isEqualTo("value");

        assertInvalidInput(
                catchThrowable(() -> ActionSupport.requireString(input, "missing")),
                "Missing required string field 'missing'");
        assertInvalidInput(catchThrowable(() -> ActionSupport.requireString(input, "nul")),
                "Missing required string field 'nul'");
        assertInvalidInput(catchThrowable(() -> ActionSupport.requireString(input, "num")),
                "Field 'num' must be a string");
    }

    @Test
    void optionalStringReturnsNullWhenAbsentButRejectsNonTextualValues() throws Exception {
        ObjectNode input = envelope("{\"present\": \"value\", \"nul\": null, \"num\": 5}");
        assertThat(ActionSupport.optionalString(input, "missing")).isNull();
        assertThat(ActionSupport.optionalString(input, "nul")).isNull();
        assertThat(ActionSupport.optionalString(input, "present")).isEqualTo("value");
        assertInvalidInput(catchThrowable(() -> ActionSupport.optionalString(input, "num")),
                "Field 'num' must be a string");
    }

    @Test
    void requireObjectAndOptionalObjectValidateShapes() throws Exception {
        ObjectNode input = envelope("{\"obj\": {\"a\": 1}, \"text\": \"hi\", \"nul\": null}");
        assertThat(ActionSupport.requireObject(input, "obj").get("a").asInt()).isEqualTo(1);
        assertInvalidInput(catchThrowable(() -> ActionSupport.requireObject(input, "missing")),
                "Missing required object field 'missing'");
        assertInvalidInput(catchThrowable(() -> ActionSupport.requireObject(input, "nul")),
                "Missing required object field 'nul'");
        assertInvalidInput(catchThrowable(() -> ActionSupport.requireObject(input, "text")),
                "Field 'text' must be a JSON object");

        assertThat(ActionSupport.optionalObject(input, "missing")).isNull();
        assertThat(ActionSupport.optionalObject(input, "nul")).isNull();
        assertThat(ActionSupport.optionalObject(input, "obj")).isNotNull();
        assertInvalidInput(catchThrowable(() -> ActionSupport.optionalObject(input, "text")),
                "Field 'text' must be a JSON object");
    }

    @Test
    void optionalIntFallsBackRejectsNonIntegersAndClampsToTheCeiling() throws Exception {
        ObjectNode input = envelope(
                "{\"nul\": null, \"ok\": 7, \"big\": 100000, \"low\": 0,"
                        + " \"text\": \"50\", \"decimal\": 1.5e10}");
        assertThat(ActionSupport.optionalInt(input, "missing", 50, 1, 500)).isEqualTo(50);
        assertThat(ActionSupport.optionalInt(input, "nul", 50, 1, 500)).isEqualTo(50);
        assertThat(ActionSupport.optionalInt(input, "ok", 50, 1, 500)).isEqualTo(7);
        // Over the ceiling clamps instead of failing.
        assertThat(ActionSupport.optionalInt(input, "big", 50, 1, 500)).isEqualTo(500);
        assertInvalidInput(catchThrowable(() -> ActionSupport.optionalInt(input, "low", 50, 1, 500)),
                "Field 'low' must be >= 1");
        assertInvalidInput(catchThrowable(() -> ActionSupport.optionalInt(input, "text", 50, 1, 500)),
                "Field 'text' must be an integer");
        // A decimal beyond the int range cannot convert either.
        assertInvalidInput(catchThrowable(() -> ActionSupport.optionalInt(input, "decimal", 50, 1, 500)),
                "Field 'decimal' must be an integer");
    }

    @Test
    void optionalOffsetRejectsNegativesAndNonIntegers() throws Exception {
        ObjectNode input = envelope(
                "{\"ok\": 40, \"neg\": -1, \"text\": \"0\", \"nul\": null}");
        assertThat(ActionSupport.optionalOffset(input, "missing")).isZero();
        assertThat(ActionSupport.optionalOffset(input, "nul")).isZero();
        assertThat(ActionSupport.optionalOffset(input, "ok")).isEqualTo(40);
        assertInvalidInput(catchThrowable(() -> ActionSupport.optionalOffset(input, "neg")),
                "Field 'neg' must be a non-negative integer");
        assertInvalidInput(catchThrowable(() -> ActionSupport.optionalOffset(input, "text")),
                "Field 'text' must be a non-negative integer");
    }

    /** A fully-populated row, straight out of a completed workflow's lifecycle. */
    private static WorkflowRunRecord completedJob() {
        WorkflowRunRecord job = new WorkflowRunRecord();
        job.jobId = UUID.randomUUID();
        job.workflowName = "embed-text";
        job.workflowDefinition = "{\"name\": \"embed-text\"}";
        job.input = "{\"text\": \"hi\"}";
        job.status = WorkflowRunRecord.STATUS_COMPLETED;
        job.attempt = 2;
        job.checkpoints = "[{\"name\": \"tokenize\", \"skipped\": false}]";
        job.result = "{\"sourceText\": \"hi\"}";
        job.verdict = "2 steps, output jobs.test.Embedding";
        job.createdAt = Instant.parse("2026-01-01T00:00:00Z");
        job.updatedAt = Instant.parse("2026-01-01T00:01:00Z");
        job.completedAt = Instant.parse("2026-01-01T00:01:00Z");
        return job;
    }

    @Test
    void jobJsonFullCarriesTheWholeRowWithParsedJsonbColumns() {
        WorkflowRunRecord job = completedJob();
        job.outstandingStep = "review";
        job.error = "something";
        ObjectNode node = ActionSupport.jobJson(job, true);

        assertThat(node.get("jobId").asText()).isEqualTo(job.jobId.toString());
        assertThat(node.get("workflowName").asText()).isEqualTo("embed-text");
        assertThat(node.get("status").asText()).isEqualTo("COMPLETED");
        assertThat(node.get("attempt").asInt()).isEqualTo(2);
        assertThat(node.get("outstandingStep").asText()).isEqualTo("review");
        // The JSONB columns come back as real JSON, not strings.
        assertThat(node.get("input").get("text").asText()).isEqualTo("hi");
        assertThat(node.get("checkpoints")).hasSize(1);
        assertThat(node.get("checkpoints").get(0).get("name").asText()).isEqualTo("tokenize");
        assertThat(node.get("result").get("sourceText").asText()).isEqualTo("hi");
        assertThat(node.get("verdict").asText()).isEqualTo("2 steps, output jobs.test.Embedding");
        assertThat(node.get("error").asText()).isEqualTo("something");
        assertThat(node.get("createdAt").asText()).isEqualTo("2026-01-01T00:00:00Z");
        assertThat(node.get("updatedAt").asText()).isEqualTo("2026-01-01T00:01:00Z");
        assertThat(node.get("completedAt").asText()).isEqualTo("2026-01-01T00:01:00Z");
    }

    @Test
    void jobJsonSummaryOmitsTheHeavyColumns() {
        ObjectNode node = ActionSupport.jobJson(completedJob(), false);
        assertThat(node.has("input")).isFalse();
        assertThat(node.has("checkpoints")).isFalse();
        assertThat(node.has("result")).isFalse();
        assertThat(node.get("jobId").asText()).isNotBlank();
        assertThat(node.get("status").asText()).isEqualTo("COMPLETED");
        // The cheap metadata stays on the summary.
        assertThat(node.get("verdict").asText()).contains("2 steps");
    }

    @Test
    void jobJsonOmitsNullsAndDefaultsNullCheckpointsToAnEmptyArray() {
        WorkflowRunRecord job = new WorkflowRunRecord();
        job.jobId = UUID.randomUUID();
        job.workflowName = "embed-text";
        job.input = "{}";
        job.checkpoints = null;
        ObjectNode node = ActionSupport.jobJson(job, true);

        assertThat(node.get("checkpoints")).isEmpty();
        assertThat(node.has("result")).isFalse();
        assertThat(node.has("outstandingStep")).isFalse();
        assertThat(node.has("verdict")).isFalse();
        assertThat(node.has("error")).isFalse();
        assertThat(node.has("createdAt")).isFalse();
        assertThat(node.has("updatedAt")).isFalse();
        assertThat(node.has("completedAt")).isFalse();
    }

    @Test
    void unreadableStoredJsonbFailsLoud() {
        WorkflowRunRecord job = new WorkflowRunRecord();
        job.jobId = UUID.randomUUID();
        job.workflowName = "embed-text";
        job.input = "{not json";
        assertThatThrownBy(() -> ActionSupport.jobJson(job, true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("stored JSONB is not readable");
    }

}
