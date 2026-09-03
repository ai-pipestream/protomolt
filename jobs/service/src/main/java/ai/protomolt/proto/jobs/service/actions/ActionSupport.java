package ai.protomolt.proto.jobs.service.actions;

import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.Reply;
import ai.protomolt.proto.jobs.service.store.WorkflowRunRecord;
import ai.protomolt.proto.jobs.service.store.WorkflowRunStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.util.UUID;

/**
 * The shared plumbing of the workflow-runs verbs: the null-store "unavailable"
 * gate (a server without {@code --jobs-jdbc}/{@code --jobs-kafka} answers
 * every jobs verb the same way), minimal envelope validation (the actions
 * module's own {@code Inputs} is package-private), and the row → JSON
 * mapping for get-job/list-jobs.
 */
final class ActionSupport {

    /** The message every jobs verb answers when no store is mounted. */
    static final String UNAVAILABLE_MESSAGE = "workflow runs are not configured on this "
            + "server (start protomolt-serve with --jobs-jdbc and --jobs-kafka)";

    private static final com.fasterxml.jackson.databind.ObjectMapper JSON =
            new com.fasterxml.jackson.databind.ObjectMapper();

    private ActionSupport() {
    }

    /** The null-store gate: no store means every call answers "unavailable". */
    static WorkflowRunStore requireStore(WorkflowRunStore store) throws ActionException {
        if (store == null) {
            throw new ActionException("unavailable", UNAVAILABLE_MESSAGE);
        }
        return store;
    }

    static ActionException invalidInput(String message) {
        return new ActionException("invalid-input", message);
    }

    static String requireString(ObjectNode input, String field) throws ActionException {
        JsonNode node = input.get(field);
        if (node == null || node.isNull()) {
            throw invalidInput("Missing required string field '" + field + "'");
        }
        if (!node.isTextual()) {
            throw invalidInput("Field '" + field + "' must be a string");
        }
        return node.asText();
    }

    /** Returns {@code null} when absent; rejects present non-string values. */
    static String optionalString(ObjectNode input, String field) throws ActionException {
        JsonNode node = input.get(field);
        if (node == null || node.isNull()) {
            return null;
        }
        if (!node.isTextual()) {
            throw invalidInput("Field '" + field + "' must be a string");
        }
        return node.asText();
    }

    static ObjectNode requireObject(ObjectNode input, String field) throws ActionException {
        JsonNode node = input.get(field);
        if (node == null || node.isNull()) {
            throw invalidInput("Missing required object field '" + field + "'");
        }
        if (!node.isObject()) {
            throw invalidInput("Field '" + field + "' must be a JSON object");
        }
        return (ObjectNode) node;
    }

    /** Returns {@code null} when absent; rejects present non-object values. */
    static ObjectNode optionalObject(ObjectNode input, String field) throws ActionException {
        JsonNode node = input.get(field);
        if (node == null || node.isNull()) {
            return null;
        }
        if (!node.isObject()) {
            throw invalidInput("Field '" + field + "' must be a JSON object");
        }
        return (ObjectNode) node;
    }

    /** Parses {@code field} as an int in [min, max]; absent → {@code fallback}. */
    static int optionalInt(ObjectNode input, String field, int fallback, int min, int max)
            throws ActionException {
        JsonNode node = input.get(field);
        if (node == null || node.isNull()) {
            return fallback;
        }
        if (!node.canConvertToInt()) {
            throw invalidInput("Field '" + field + "' must be an integer");
        }
        int value = node.asInt();
        if (value < min) {
            throw invalidInput("Field '" + field + "' must be >= " + min);
        }
        return Math.min(value, max);
    }

    /** Parses {@code field} as a non-negative long; absent → 0. */
    static long optionalOffset(ObjectNode input, String field) throws ActionException {
        JsonNode node = input.get(field);
        if (node == null || node.isNull()) {
            return 0;
        }
        if (!node.canConvertToLong() || node.asLong() < 0) {
            throw invalidInput("Field '" + field + "' must be a non-negative integer");
        }
        return node.asLong();
    }

    /**
     * The row as the get-job JSON document: camelCase, nulls omitted,
     * timestamps ISO-8601, the JSONB columns ({@code input},
     * {@code checkpoints}, {@code result}) re-parsed into real JSON.
     *
     * @param job the row
     * @param full true for get-job (input/checkpoints/result included),
     *        false for the list-jobs summary
     * @return the JSON document
     */
    /**
     * Writes one run into {@code run}, the contract's own shape for it.
     *
     * @param full true for get-job (input, checkpoints and result included), false for the
     *        list-jobs summary
     */
    static void writeJob(Reply run, WorkflowRunRecord job, boolean full) {
        run.set("jobId", job.jobId.toString())
                .set("workflowName", job.workflowName)
                .set("status", job.status)
                .set("attempt", job.attempt)
                .set("outstandingStep", text(job.outstandingStep));
        if (full) {
            run.set("input", parse(job.input));
            for (JsonNode entry : parse(job.checkpoints == null ? "[]" : job.checkpoints)) {
                run.append("checkpoints")
                        .set("name", entry.path("name").asText())
                        .set("skipped", entry.path("skipped").asBoolean())
                        .set("response", entry.path("response"))
                        .build();
            }
            if (job.result != null) {
                run.set("result", parse(job.result));
            }
        }
        run.set("verdict", text(job.verdict))
                .set("error", text(job.error))
                .set("createdAt", text(iso(job.createdAt)))
                .set("updatedAt", text(iso(job.updatedAt)))
                .set("completedAt", text(iso(job.completedAt)))
                .build();
    }

    /**
     * The job id a request names.
     *
     * <p>The message declares it as a uuid, so a malformed one is refused before dispatch;
     * this parses what the rule already accepted.
     */
    static UUID jobId(String text) throws ActionException {
        try {
            return UUID.fromString(text.trim());
        } catch (IllegalArgumentException e) {
            throw invalidInput("'jobId' must be a uuid; got '" + text + "'");
        }
    }

    /** An absent value as the empty string the field already means it by. */
    private static String text(String value) {
        return value == null ? "" : value;
    }

    static ObjectNode jobJson(WorkflowRunRecord job, boolean full) {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.put("jobId", job.jobId.toString());
        node.put("workflowName", job.workflowName);
        node.put("status", job.status);
        node.put("attempt", job.attempt);
        putText(node, "outstandingStep", job.outstandingStep);
        if (full) {
            node.set("input", parse(job.input));
            node.set("checkpoints", parse(job.checkpoints == null ? "[]" : job.checkpoints));
            if (job.result != null) {
                node.set("result", parse(job.result));
            }
        }
        putText(node, "verdict", job.verdict);
        putText(node, "error", job.error);
        putText(node, "createdAt", iso(job.createdAt));
        putText(node, "updatedAt", iso(job.updatedAt));
        putText(node, "completedAt", iso(job.completedAt));
        return node;
    }

    private static JsonNode parse(String json) {
        try {
            return JSON.readTree(json);
        } catch (Exception e) {
            throw new IllegalStateException("stored JSONB is not readable: " + e.getMessage(), e);
        }
    }

    private static void putText(ObjectNode node, String field, String value) {
        if (value != null) {
            node.put(field, value);
        }
    }

    private static String iso(Instant instant) {
        return instant == null ? null : instant.toString();
    }
}
