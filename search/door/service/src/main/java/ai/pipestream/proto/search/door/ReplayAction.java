package ai.pipestream.proto.search.door;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.repo.v1.DocumentMetadata;
import ai.pipestream.proto.repo.v1.ListDocumentsRequest;
import ai.pipestream.proto.repo.v1.ListDocumentsResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.util.JsonFormat;

/**
 * The replay operation: re-runs a stored workflow over every document a
 * repository listing matches, one durable run per document. This is what
 * makes a config or shape change an operation instead of a migration — a
 * changed chunking policy or mapping re-derives by resubmitting
 * {@code parse-and-index} over the corpus, and the door's atomic
 * replace-by-identity guarantees replays never duplicate.
 *
 * <p>Every identity is explicit: the workflow name, the mapping subject,
 * and the drive all come from the request. Submission rides the jobs
 * module's own {@code submit-workflow} action, so replay inherits its
 * idempotency and validation.
 */
public final class ReplayAction implements ProtoAction {

    /** The action name: {@value}. */
    public static final String NAME = "replay-documents";

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int PAGE_SIZE = 100;

    private final DocumentLister lister;
    private final ProtoAction submit;

    /**
     * Creates the action.
     *
     * @param lister the repository listing
     * @param submit the jobs {@code submit-workflow} action submissions ride
     */
    public ReplayAction(DocumentLister lister, ProtoAction submit) {
        if (lister == null) {
            throw new IllegalArgumentException("lister must not be null");
        }
        if (submit == null) {
            throw new IllegalArgumentException("submit must not be null");
        }
        this.lister = lister;
        this.submit = submit;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String description() {
        return "Re-runs a stored workflow over every document a repository listing matches"
                + " (one durable run per document, workflow input {address, mappingSubject});"
                + " replays re-derive search state and never duplicate, so this is the"
                + " operation behind a chunking-policy or mapping change.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = MAPPER.createObjectNode();
        schema.put("type", "object");
        ObjectNode properties = schema.putObject("properties");
        properties.putObject("workflowName")
                .put("type", "string")
                .put("description", "Stored workflow to run per document, e.g. parse-and-index");
        properties.putObject("mappingSubject")
                .put("type", "string")
                .put("description", "Mapping subject the workflow indexes under");
        properties.putObject("drive")
                .put("type", "string")
                .put("description", "Drive whose documents replay");
        properties.putObject("accountId")
                .put("type", "string")
                .put("description", "Optional account (tenant) filter");
        schema.putArray("required").add("workflowName").add("mappingSubject").add("drive");
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        String workflowName = requiredString(input, "workflowName");
        String mappingSubject = requiredString(input, "mappingSubject");
        String drive = requiredString(input, "drive");
        JsonNode account = input.get("accountId");

        ObjectNode output = MAPPER.createObjectNode();
        ArrayNode jobIds = output.putArray("jobIds");
        int submitted = 0;
        String continuation = "";
        do {
            ListDocumentsRequest.Builder page = ListDocumentsRequest.newBuilder()
                    .setDrive(drive)
                    .setLimit(PAGE_SIZE)
                    .setContinuationToken(continuation);
            if (account != null && !account.asText().isBlank()) {
                page.setAccountId(account.asText());
            }
            ListDocumentsResponse listing = lister.list(page.build());
            for (DocumentMetadata document : listing.getDocumentsList()) {
                ObjectNode workflowInput = MAPPER.createObjectNode();
                workflowInput.set("address", addressJson(document));
                workflowInput.put("mappingSubject", mappingSubject);
                ObjectNode envelope = MAPPER.createObjectNode();
                envelope.put("workflowName", workflowName);
                envelope.set("input", workflowInput);
                ObjectNode result = submit.execute(envelope, context);
                jobIds.add(result.path("jobId").asText());
                submitted++;
            }
            continuation = listing.getNextContinuationToken();
        } while (!continuation.isEmpty());
        output.put("submitted", submitted);
        return output;
    }

    private static JsonNode addressJson(DocumentMetadata document) throws ActionException {
        try {
            return MAPPER.readTree(JsonFormat.printer().print(document.getAddress()));
        } catch (Exception e) {
            throw new ActionException("invalid-address",
                    "cannot render the stored address of '" + document.getDocId() + "': "
                            + e.getMessage());
        }
    }

    private static String requiredString(ObjectNode input, String field) throws ActionException {
        JsonNode value = input.get(field);
        if (value == null || !value.isTextual() || value.asText().isBlank()) {
            throw new ActionException("invalid-input", "'" + field + "' is required");
        }
        return value.asText();
    }
}
