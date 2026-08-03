package ai.pipestream.proto.jobs.service;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.chain.ChainDefinition;
import ai.pipestream.proto.chain.ChainJson;
import ai.pipestream.proto.chain.ChainRepository;
import ai.pipestream.proto.chain.ChainVerifier;
import ai.pipestream.proto.jobs.service.events.ChainJobEventFactory;
import ai.pipestream.proto.jobs.service.store.ChainJobRecord;
import ai.pipestream.proto.jobs.service.store.ChainJobStore;
import ai.pipestream.proto.json.MalformedProtobufJsonException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;
import java.util.UUID;

/**
 * The submit path shared by the {@code submit-chain} verb and the
 * request-topic consumer: resolve the chain (inline object or stored name),
 * parse it, verify it, validate the input parses as the chain's inputType,
 * then insert the QUEUED row and its ACCEPTED event in one transaction.
 * <p>
 * Submission never executes anything — the worker fleet picks the row up.
 * Validation failures come back as an {@link Outcome} with {@code ok ==
 * false} (the verb answers its caller; the consumer writes a FAILED row —
 * nothing is ever silently dropped). Store failures propagate: fail loud.
 */
public final class ChainJobSubmitter {

    /**
     * The submit verdict.
     *
     * @param ok true when the job row exists (newly inserted or idempotent
     *        resubmit)
     * @param jobId the job id (set when {@code ok})
     * @param status the stored row's status
     * @param failedStep the step a parse/verify failure names ("" for
     *        chain-level failures)
     * @param error the failure detail (null when {@code ok})
     */
    public record Outcome(boolean ok, String jobId, String status, String failedStep,
                          String error) {
    }

    private final ChainJobStore store;
    private final ChainRepository repository;
    private final int maxAttemptsDefault;

    /**
     * @param store the jobs store (required)
     * @param repository resolves stored chain names, or null for inline-only
     * @param maxAttemptsDefault the retry ceiling stamped on new jobs
     */
    public ChainJobSubmitter(ChainJobStore store, ChainRepository repository,
            int maxAttemptsDefault) {
        this.store = java.util.Objects.requireNonNull(store, "store");
        this.repository = repository;
        this.maxAttemptsDefault = maxAttemptsDefault;
    }

    /**
     * Submit one job.
     *
     * @param inlineChain an inline chain definition object, or null
     * @param chainName a stored chain name, or null
     * @param input the chain input object (proto3 JSON of the inputType)
     * @param jobIdOrNull the client-generated job uuid, or null/blank to mint
     * @param context type resolution and JSON machinery
     * @return the submit verdict
     */
    public Outcome submit(ObjectNode inlineChain, String chainName, JsonNode input,
            String jobIdOrNull, ActionContext context) {
        JsonNode chainNode = inlineChain;
        if (chainNode == null && chainName != null && !chainName.isBlank()) {
            if (repository == null) {
                return fail("", "No chain repository is mounted; submit with an inline "
                        + "'chain' or start a server with a registry");
            }
            chainNode = repository.chain(chainName).orElse(null);
            if (chainNode == null) {
                return fail("", "No stored chain named '" + chainName + "'");
            }
        }
        if (!(chainNode instanceof ObjectNode chain) || !(input instanceof ObjectNode)) {
            return fail("", "'chain' (or 'chainName') and 'input' objects are required");
        }
        ChainDefinition definition;
        try {
            definition = ChainJson.parse(chain, context);
        } catch (ChainJson.ChainParseException e) {
            return fail(e.step, e.getMessage());
        }
        List<ChainVerifier.Finding> findings = new ChainVerifier().verify(definition);
        if (!findings.isEmpty()) {
            ChainVerifier.Finding first = findings.get(0);
            return fail(first.step(), "chain does not verify (" + findings.size() + " finding"
                    + (findings.size() == 1 ? "" : "s") + "); first: [" + first.kind() + "] "
                    + first.error());
        }
        try {
            context.transcoder().fromJsonDynamic(input.toString(), definition.inputType());
        } catch (MalformedProtobufJsonException e) {
            return fail("", "'input' is not valid proto3 JSON for "
                    + definition.inputType().getFullName() + ": " + e.getMessage());
        }
        UUID jobId;
        if (jobIdOrNull == null || jobIdOrNull.isBlank()) {
            jobId = UUID.randomUUID();
        } else {
            try {
                jobId = UUID.fromString(jobIdOrNull.trim());
            } catch (IllegalArgumentException e) {
                return fail("", "'jobId' must be a uuid; got '" + jobIdOrNull + "'");
            }
        }
        ChainJobRecord record = new ChainJobRecord();
        record.jobId = jobId;
        record.chainName = chainName != null && !chainName.isBlank()
                ? chainName
                : definition.name() == null ? "inline" : definition.name();
        record.chainDefinition = chain.toString();
        record.input = input.toString();
        record.status = ChainJobRecord.STATUS_QUEUED;
        record.maxAttempts = maxAttemptsDefault;
        record.runAfter = java.time.Instant.now();
        ChainJobStore.InsertOutcome outcome =
                store.insert(record, ChainJobEventFactory.accepted(record));
        return new Outcome(true, outcome.job().jobId.toString(), outcome.job().status, "", null);
    }

    private static Outcome fail(String step, String error) {
        return new Outcome(false, null, null, step == null ? "" : step, error);
    }
}
