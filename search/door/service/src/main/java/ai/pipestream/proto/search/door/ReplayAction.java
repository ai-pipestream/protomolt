package ai.pipestream.proto.search.door;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.repo.v1.DocumentMetadata;
import ai.pipestream.proto.repo.v1.ListDocumentsRequest;
import ai.pipestream.proto.repo.v1.ListDocumentsResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.util.JsonFormat;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;

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
 * validation; with the optional {@code replayId} input, each document's run
 * also carries a deterministic {@code jobId} derived from it, so
 * resubmitting the same replay is idempotent at the jobs layer too.
 *
 * <p>With {@code prune} set, the replay is a reconcile: it runs over the
 * whole repository listing (a scope would make pruning delete other
 * scopes' documents, so {@code drive} and {@code accountId} are refused),
 * and indexed documents the listing no longer contains are removed from
 * the subject's index. The indexed set is captured before the listing
 * pages, so a document indexed concurrently is never a prune candidate.
 */
public final class ReplayAction implements ProtoAction {

    /** The action name: {@value}. */
    public static final String NAME = "replay-documents";

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int PAGE_SIZE = 100;

    private final DocumentLister lister;
    private final ProtoAction submit;
    private final SubjectIndex index;

    /**
     * Creates the action.
     *
     * @param lister the repository listing
     * @param submit the jobs {@code submit-workflow} action submissions ride
     * @param index the door's index surface pruning reconciles through
     */
    public ReplayAction(DocumentLister lister, ProtoAction submit, SubjectIndex index) {
        if (lister == null) {
            throw new IllegalArgumentException("lister must not be null");
        }
        if (submit == null) {
            throw new IllegalArgumentException("submit must not be null");
        }
        if (index == null) {
            throw new IllegalArgumentException("index must not be null");
        }
        this.lister = lister;
        this.submit = submit;
        this.index = index;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String requiredScope() {
        return Scopes.SEARCH_INDEX;
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
                .put("description", "Drive whose documents replay; required unless prune,"
                        + " refused with it");
        properties.putObject("accountId")
                .put("type", "string")
                .put("description", "Optional account (tenant) filter; refused with prune");
        properties.putObject("replayId")
                .put("type", "string")
                .put("description", "Optional replay identifier; when set, every document's"
                        + " run gets a deterministic jobId derived from it, so re-running"
                        + " the same replay resubmits nothing");
        properties.putObject("prune")
                .put("type", "boolean")
                .put("description", "Reconcile: replay the whole repository listing and"
                        + " remove indexed documents the listing no longer contains;"
                        + " unscoped by design, so drive and accountId are refused");
        schema.putArray("required").add("workflowName").add("mappingSubject");
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        String workflowName = requiredString(input, "workflowName");
        String mappingSubject = requiredString(input, "mappingSubject");
        boolean prune = input.path("prune").asBoolean(false);
        JsonNode account = input.get("accountId");
        String drive;
        Set<String> indexedBefore = null;
        if (prune) {
            if (optionalString(input, "drive") != null) {
                throw new ActionException("invalid-input", "'drive' cannot be set with"
                        + " 'prune': prune reconciles the whole mapping subject against"
                        + " the whole repository listing, and a drive-scoped listing"
                        + " would prune other drives' documents");
            }
            if (account != null && !account.asText().isBlank()) {
                throw new ActionException("invalid-input", "'accountId' cannot be set"
                        + " with 'prune': prune reconciles the whole mapping subject"
                        + " against the whole repository listing, and an account-scoped"
                        + " listing would prune other accounts' documents");
            }
            drive = "";
            // Captured before the listing pages: a document indexed while
            // the replay runs is never a prune candidate. Also refuses an
            // unknown subject before anything resubmits.
            indexedBefore = index.indexedDocIds(mappingSubject);
        } else {
            drive = requiredString(input, "drive");
        }
        String replayId = optionalString(input, "replayId");

        ObjectNode output = MAPPER.createObjectNode();
        ArrayNode jobIds = output.putArray("jobIds");
        Set<String> listed = new LinkedHashSet<>();
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
                listed.add(document.getDocId());
                ObjectNode workflowInput = MAPPER.createObjectNode();
                workflowInput.set("address", addressJson(document));
                workflowInput.put("mappingSubject", mappingSubject);
                ObjectNode envelope = MAPPER.createObjectNode();
                envelope.put("workflowName", workflowName);
                envelope.set("input", workflowInput);
                if (replayId != null) {
                    envelope.put("jobId", runJobId(
                            replayId, workflowName, mappingSubject, drive,
                            document.getDocId()));
                }
                ObjectNode result = submit.execute(envelope, context);
                jobIds.add(result.path("jobId").asText());
                submitted++;
            }
            continuation = listing.getNextContinuationToken();
        } while (!continuation.isEmpty());
        output.put("submitted", submitted);
        if (prune) {
            ArrayNode prunedDocIds = output.putArray("prunedDocIds");
            for (String docId : indexedBefore) {
                if (!listed.contains(docId)) {
                    index.delete(mappingSubject, docId);
                    prunedDocIds.add(docId);
                }
            }
            output.put("pruned", prunedDocIds.size());
        }
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

    private static String optionalString(ObjectNode input, String field) {
        JsonNode value = input.get(field);
        return value == null || !value.isTextual() || value.asText().isBlank()
                ? null
                : value.asText();
    }

    /**
     * The deterministic run id one document of one replay gets: resubmitting
     * the same replay names the same ids, and {@code submit-workflow} treats
     * {@code jobId} as its idempotency key.
     */
    private static String runJobId(String replayId, String workflowName,
            String mappingSubject, String drive, String docId) {
        return UUID.nameUUIDFromBytes(String.join("\n",
                        replayId, workflowName, mappingSubject, drive, docId)
                .getBytes(StandardCharsets.UTF_8))
                .toString();
    }
}
