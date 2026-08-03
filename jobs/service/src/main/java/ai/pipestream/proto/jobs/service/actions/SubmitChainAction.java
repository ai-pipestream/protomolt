package ai.pipestream.proto.jobs.service.actions;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.chain.ChainRepository;
import ai.pipestream.proto.jobs.service.ChainJobSubmitter;
import ai.pipestream.proto.jobs.service.store.ChainJobStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * The {@code submit-chain} verb: accept a chain job for asynchronous
 * execution — the async sibling of {@code run-chain}, same definition, same
 * serial semantics, different coupling. The chain (inline object or stored
 * name) is parsed, verified, and its input validated before anything is
 * persisted; the job row (status QUEUED) and its ACCEPTED event insert in
 * one transaction. {@code jobId} is the idempotency key: resubmitting an
 * existing id returns the existing row untouched.
 * <p>
 * A null store means chain jobs are not configured on this server; every
 * call then answers {@code unavailable}.
 */
public final class SubmitChainAction implements ProtoAction {

    private final ChainJobStore store;
    private final ChainJobSubmitter submitter;

    /**
     * @param store the jobs store, or null when jobs are not configured
     * @param repository resolves stored chain names, or null for inline-only
     * @param maxAttemptsDefault the retry ceiling stamped on new jobs
     */
    public SubmitChainAction(ChainJobStore store, ChainRepository repository,
            int maxAttemptsDefault) {
        this.store = store;
        this.submitter = store == null
                ? null
                : new ChainJobSubmitter(store, repository, maxAttemptsDefault);
    }

    @Override
    public String name() {
        return "submit-chain";
    }

    @Override
    public String description() {
        return "Submits a chain for asynchronous execution as a durable job: the "
                + "definition (inline 'chain' or stored 'chainName') is verified, the "
                + "input validated against the chain's inputType, and the job queued — "
                + "workers execute it with the same serial semantics as run-chain, "
                + "checkpointing every step. 'jobId' is the idempotency key (a uuid is "
                + "minted when absent). Returns the job id and status; lifecycle events "
                + "are published to the chain-job-events topic.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = baseSchema();
        ObjectNode properties = schema.putObject("properties");
        properties.putObject("chain")
                .put("type", "object")
                .put("description", "The chain definition: a schema declaring every "
                        + "step's service, an inputType, and serial steps whose requests "
                        + "are mapped from 'input' plus prior steps' responses (by step "
                        + "name). Steps: {name, target, method, tls?, when?, rules?, "
                        + "celRules?, validate?, deadlineMs?, completion?}; completion "
                        + "'external' parks the job until complete-step supplies the "
                        + "response.");
        properties.putObject("chainName")
                .put("type", "string")
                .put("description", "A stored chain to run instead of an inline 'chain' — "
                        + "registered via the registry's chains endpoint.");
        properties.putObject("input")
                .put("type", "object")
                .put("description", "The chain input, as proto3 JSON of the chain's "
                        + "inputType.");
        properties.putObject("jobId")
                .put("type", "string")
                .put("description", "Client-generated uuid; the idempotency key. Minted "
                        + "when absent.");
        schema.putArray("required").add("input");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        ActionSupport.requireStore(store);
        ObjectNode chain = ActionSupport.optionalObject(input, "chain");
        String chainName = ActionSupport.optionalString(input, "chainName");
        String jobId = ActionSupport.optionalString(input, "jobId");
        JsonNode inputNode = input.get("input");
        ChainJobSubmitter.Outcome outcome =
                submitter.submit(chain, chainName, inputNode, jobId, context);
        ObjectNode result = JsonNodeFactory.instance.objectNode();
        result.put("ok", outcome.ok());
        if (outcome.ok()) {
            result.put("jobId", outcome.jobId());
            result.put("status", outcome.status());
        } else {
            if (outcome.failedStep() != null && !outcome.failedStep().isEmpty()) {
                result.put("failedStep", outcome.failedStep());
            }
            result.put("error", outcome.error());
        }
        return result;
    }

    private static ObjectNode baseSchema() {
        ObjectNode schema = JsonNodeFactory.instance.objectNode();
        schema.put("$schema", "https://json-schema.org/draft/2020-12/schema");
        schema.put("type", "object");
        return schema;
    }
}
