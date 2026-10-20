package ai.protomolt.proto.search.service;

import com.google.protobuf.Message;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.DocumentSummary;
import ai.protomolt.proto.repo.v2.ListDocumentsRequest;
import ai.protomolt.proto.repo.v2.ListDocumentsResponse;
import ai.protomolt.proto.search.v1.ReplayDocumentsRequest;
import ai.protomolt.proto.search.v1.ReplayDocumentsResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
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
 * {@code parse-and-index} over the corpus, and the service's atomic
 * replace-by-identity guarantees replays never duplicate.
 *
 * <p>Every identity is explicit: the workflow name, the mapping subject,
 * and the account all come from the request. Submission rides the jobs
 * module's own {@code submit-workflow} action, so replay inherits its
 * validation; with the optional {@code replayId} input, each document's run
 * also carries a deterministic {@code jobId} derived from it, so
 * resubmitting the same replay is idempotent at the jobs layer too.
 *
 * <p>With {@code prune} set, the replay is a reconcile: it runs over the
 * account's whole listing (a datasource scope would make pruning delete
 * other scopes' documents, so {@code graphAddressId} is refused), and
 * indexed documents the listing no longer contains are removed from the
 * subject's index. The indexed set is captured before the listing pages,
 * so a document indexed concurrently is never a prune candidate. The
 * repository v2 listing is account-scoped, so both modes name an account;
 * there is no repository-wide listing to reconcile against.
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
     * @param index the service's index surface pruning reconciles through
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
    public Descriptor requestType() {
        return ReplayDocumentsRequest.getDescriptor();
    }

    @Override
    public Descriptor responseType() {
        return ReplayDocumentsResponse.getDescriptor();
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        ReplayDocumentsRequest request = CatalogContract.as(
                input, ReplayDocumentsRequest.getDefaultInstance(), name());
        String workflowName = request.getWorkflowName();
        String mappingSubject = request.getMappingSubject();
        boolean prune = request.getPrune();
        String accountId = request.getAccountId();
        String graphAddressId = request.getGraphAddressId();
        Set<String> indexedBefore = null;
        if (prune) {
            // Captured before the listing pages: a document indexed while
            // the replay runs is never a prune candidate. Also refuses an
            // unknown subject before anything resubmits.
            indexedBefore = index.indexedDocIds(mappingSubject);
        }
        String replayId = request.getReplayId();

        ReplayDocumentsResponse.Builder output = ReplayDocumentsResponse.newBuilder();
        Set<String> listed = new LinkedHashSet<>();
        int submitted = 0;
        String pageToken = "";
        do {
            ListDocumentsRequest.Builder page = ListDocumentsRequest.newBuilder()
                    .setAccountId(accountId)
                    .setPageSize(PAGE_SIZE)
                    .setPageToken(pageToken);
            if (!graphAddressId.isEmpty()) {
                page.setGraphAddressId(graphAddressId);
            }
            ListDocumentsResponse listing = lister.list(page.build());
            for (DocumentSummary document : listing.getDocumentsList()) {
                listed.add(document.getAddress().getDocId());
                ObjectNode workflowInput = MAPPER.createObjectNode();
                workflowInput.set("address", addressJson(document.getAddress()));
                workflowInput.put("mappingSubject", mappingSubject);
                ObjectNode envelope = MAPPER.createObjectNode();
                envelope.put("workflowName", workflowName);
                envelope.set("input", workflowInput);
                // Absent means an empty string on the wire, not null. Treating the empty
                // string as a name would give every replay a deterministic job id and make
                // an unnamed replay silently idempotent.
                if (!replayId.isEmpty()) {
                    envelope.put("jobId", runJobId(
                            replayId, workflowName, mappingSubject, accountId,
                            graphAddressId, document.getAddress().getDocId()));
                }
                // The submit verb runs on its own request message, so the envelope built
                // here converts at this call the same way it would at any other front.
                ObjectNode result = CatalogContract.toReply(submit.execute(
                        CatalogContract.toRequest(envelope, submit.requestType(), submit.name()),
                        context), submit.name());
                output.addJobIds(result.path("jobId").asText());
                submitted++;
            }
            pageToken = listing.getNextPageToken();
        } while (!pageToken.isEmpty());
        output.setSubmitted(submitted);
        if (prune) {
            for (String docId : indexedBefore) {
                if (!listed.contains(docId)) {
                    index.delete(mappingSubject, docId);
                    output.addPrunedDocIds(docId);
                }
            }
            output.setPruned(output.getPrunedDocIdsCount());
        }
        return output.build();
    }

    private static JsonNode addressJson(NodeAddress address) throws ActionException {
        try {
            return MAPPER.readTree(JsonFormat.printer().print(address));
        } catch (Exception e) {
            throw new ActionException("invalid-address",
                    "cannot render the stored address of '" + address.getDocId() + "': "
                            + e.getMessage());
        }
    }

    /**
     * The deterministic run id one document of one replay gets: resubmitting
     * the same replay names the same ids, and {@code submit-workflow} treats
     * {@code jobId} as its idempotency key.
     */
    private static String runJobId(String replayId, String workflowName,
            String mappingSubject, String accountId, String graphAddressId, String docId) {
        return UUID.nameUUIDFromBytes(String.join("\n",
                        replayId, workflowName, mappingSubject, accountId, graphAddressId, docId)
                .getBytes(StandardCharsets.UTF_8))
                .toString();
    }
}
