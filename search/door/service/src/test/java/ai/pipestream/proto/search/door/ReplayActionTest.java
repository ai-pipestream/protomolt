package ai.pipestream.proto.search.door;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.repo.v1.DocumentMetadata;
import ai.pipestream.proto.repo.v1.ListDocumentsResponse;
import ai.pipestream.proto.repo.v1.NodeAddress;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Replay pages the listing and rides the submit action: one durable run
 * per stored document, both identities explicit on every submission.
 */
class ReplayActionTest {

    static final ObjectMapper MAPPER = new ObjectMapper();

    /** Records every submission and answers with a synthetic job id. */
    static class RecordingSubmit implements ProtoAction {

        final List<ObjectNode> submissions = new ArrayList<>();

        @Override
        public String name() {
            return "submit-workflow";
        }

        @Override
        public String description() {
            return "records";
        }

        @Override
        public ObjectNode inputSchema() {
            return MAPPER.createObjectNode();
        }

        @Override
        public ObjectNode execute(ObjectNode input, ActionContext context)
                throws ActionException {
            submissions.add(input.deepCopy());
            ObjectNode result = MAPPER.createObjectNode();
            result.put("ok", true);
            result.put("jobId", "job-" + submissions.size());
            return result;
        }
    }

    /** A map-backed index surface: one subject and its doc ids. */
    static class FakeIndex implements SubjectIndex {

        final String subject;
        final Set<String> ids = new LinkedHashSet<>();
        final List<String> deleted = new ArrayList<>();

        FakeIndex(String subject, String... initial) {
            this.subject = subject;
            ids.addAll(List.of(initial));
        }

        @Override
        public Set<String> indexedDocIds(String subjectName) {
            check(subjectName);
            return new LinkedHashSet<>(ids);
        }

        @Override
        public void delete(String subjectName, String docId) {
            check(subjectName);
            ids.remove(docId);
            deleted.add(docId);
        }

        private void check(String subjectName) {
            if (!subject.equals(subjectName)) {
                throw new IllegalArgumentException(
                        "unknown mapping subject '" + subjectName + "'");
            }
        }
    }

    static ReplayAction replay(DocumentLister lister, ProtoAction submit) {
        return new ReplayAction(lister, submit, new FakeIndex(RepoDocumentMapping.SUBJECT));
    }

    static DocumentMetadata document(String docId) {
        return DocumentMetadata.newBuilder()
                .setDocId(docId)
                .setAddress(NodeAddress.newBuilder()
                        .setDocId(docId)
                        .setGraphAddressId("ds-court")
                        .setAccountId("acct")
                        .setGraphId("intake:acct"))
                .build();
    }

    @Test
    void replaySubmitsOneRunPerDocumentAcrossPages() throws Exception {
        RecordingSubmit submit = new RecordingSubmit();
        ReplayAction replay = replay(request -> {
            // Two pages: the first hands back a continuation token.
            if (request.getContinuationToken().isEmpty()) {
                assertThat(request.getDrive()).isEqualTo("intake");
                assertThat(request.getAccountId()).isEqualTo("acct");
                return ListDocumentsResponse.newBuilder()
                        .addDocuments(document("doc-1"))
                        .addDocuments(document("doc-2"))
                        .setNextContinuationToken("page-2")
                        .build();
            }
            return ListDocumentsResponse.newBuilder()
                    .addDocuments(document("doc-3"))
                    .build();
        }, submit);

        ObjectNode input = MAPPER.createObjectNode();
        input.put("workflowName", "parse-and-index");
        input.put("mappingSubject", RepoDocumentMapping.SUBJECT);
        input.put("drive", "intake");
        input.put("accountId", "acct");
        ObjectNode output = replay.execute(input, ActionContext.create());

        assertThat(output.path("submitted").asInt()).isEqualTo(3);
        assertThat(output.path("jobIds")).hasSize(3);
        assertThat(submit.submissions).hasSize(3);
        ObjectNode first = submit.submissions.getFirst();
        assertThat(first.path("workflowName").asText()).isEqualTo("parse-and-index");
        assertThat(first.path("input").path("mappingSubject").asText())
                .isEqualTo(RepoDocumentMapping.SUBJECT);
        assertThat(first.path("input").path("address").path("docId").asText())
                .isEqualTo("doc-1");
        assertThat(first.path("input").path("address").path("graphId").asText())
                .isEqualTo("intake:acct");
    }

    @Test
    void everyIdentityIsRequiredByName() {
        ReplayAction replay = replay(
                request -> ListDocumentsResponse.getDefaultInstance(), new RecordingSubmit());
        for (String missing : new String[] {"workflowName", "mappingSubject", "drive"}) {
            ObjectNode input = MAPPER.createObjectNode();
            input.put("workflowName", "parse-and-index");
            input.put("mappingSubject", RepoDocumentMapping.SUBJECT);
            input.put("drive", "intake");
            input.remove(missing);
            assertThatThrownBy(() -> replay.execute(input, ActionContext.create()))
                    .isInstanceOf(ActionException.class)
                    .hasMessageContaining(missing);
        }
    }

    @Test
    void anEmptyListingSubmitsNothing() throws Exception {
        RecordingSubmit submit = new RecordingSubmit();
        ReplayAction replay = replay(
                request -> ListDocumentsResponse.getDefaultInstance(), submit);
        ObjectNode input = MAPPER.createObjectNode();
        input.put("workflowName", "parse-and-index");
        input.put("mappingSubject", RepoDocumentMapping.SUBJECT);
        input.put("drive", "intake");
        ObjectNode output = replay.execute(input, ActionContext.create());
        assertThat(output.path("submitted").asInt()).isZero();
        assertThat(submit.submissions).isEmpty();
    }

    @Test
    void aSubmissionFailureStopsTheReplayAndSurfaces() {
        RecordingSubmit submit = new RecordingSubmit() {
            @Override
            public ObjectNode execute(ObjectNode input, ActionContext context)
                    throws ActionException {
                if (submissions.size() == 1) {
                    throw new ActionException("submit-refused", "the jobs module refused");
                }
                return super.execute(input, context);
            }
        };
        ReplayAction replay = replay(
                request -> ListDocumentsResponse.newBuilder()
                        .addDocuments(document("doc-1"))
                        .addDocuments(document("doc-2"))
                        .addDocuments(document("doc-3"))
                        .build(),
                submit);
        ObjectNode input = MAPPER.createObjectNode();
        input.put("workflowName", "parse-and-index");
        input.put("mappingSubject", RepoDocumentMapping.SUBJECT);
        input.put("drive", "intake");
        // The refusal propagates; the run submitted before it stays submitted.
        assertThatThrownBy(() -> replay.execute(input, ActionContext.create()))
                .isInstanceOf(ActionException.class)
                .hasMessageContaining("the jobs module refused");
        assertThat(submit.submissions).hasSize(1);
    }

    @Test
    void anEmptyPageStillFollowsItsContinuationToken() throws Exception {
        RecordingSubmit submit = new RecordingSubmit();
        ReplayAction replay = replay(request -> {
            if (request.getContinuationToken().isEmpty()) {
                // An empty first page with a continuation: paging continues.
                return ListDocumentsResponse.newBuilder()
                        .setNextContinuationToken("page-2")
                        .build();
            }
            return ListDocumentsResponse.newBuilder()
                    .addDocuments(document("doc-1"))
                    .build();
        }, submit);
        ObjectNode input = MAPPER.createObjectNode();
        input.put("workflowName", "parse-and-index");
        input.put("mappingSubject", RepoDocumentMapping.SUBJECT);
        input.put("drive", "intake");
        ObjectNode output = replay.execute(input, ActionContext.create());
        assertThat(output.path("submitted").asInt()).isEqualTo(1);
        assertThat(submit.submissions).hasSize(1);
    }

    @Test
    void anAbsentAccountIdSendsNoAccountFilter() throws Exception {
        List<String> accountFilters = new ArrayList<>();
        ReplayAction replay = replay(request -> {
            accountFilters.add(request.getAccountId());
            return ListDocumentsResponse.getDefaultInstance();
        }, new RecordingSubmit());
        ObjectNode input = MAPPER.createObjectNode();
        input.put("workflowName", "parse-and-index");
        input.put("mappingSubject", RepoDocumentMapping.SUBJECT);
        input.put("drive", "intake");
        replay.execute(input, ActionContext.create());
        assertThat(accountFilters).containsExactly("");
    }

    @Test
    void pruneRemovesIndexedDocumentsTheListingNoLongerContains() throws Exception {
        RecordingSubmit submit = new RecordingSubmit();
        FakeIndex index = new FakeIndex(RepoDocumentMapping.SUBJECT, "doc-1", "doc-stale");
        ReplayAction action = new ReplayAction(request -> {
            // Unscoped by design: prune reconciles the whole listing.
            assertThat(request.getDrive()).isEmpty();
            assertThat(request.getAccountId()).isEmpty();
            return ListDocumentsResponse.newBuilder()
                    .addDocuments(document("doc-1"))
                    .build();
        }, submit, index);

        ObjectNode input = MAPPER.createObjectNode();
        input.put("workflowName", "parse-and-index");
        input.put("mappingSubject", RepoDocumentMapping.SUBJECT);
        input.put("prune", true);
        ObjectNode output = action.execute(input, ActionContext.create());

        assertThat(output.path("submitted").asInt()).isEqualTo(1);
        assertThat(output.path("pruned").asInt()).isEqualTo(1);
        assertThat(output.path("prunedDocIds")).hasSize(1);
        assertThat(output.path("prunedDocIds").get(0).asText()).isEqualTo("doc-stale");
        assertThat(index.deleted).containsExactly("doc-stale");
        assertThat(index.ids).containsExactly("doc-1");
    }

    @Test
    void pruneRefusesAnyScopeByName() {
        ReplayAction action = new ReplayAction(
                request -> ListDocumentsResponse.getDefaultInstance(),
                new RecordingSubmit(),
                new FakeIndex(RepoDocumentMapping.SUBJECT));
        for (String scope : new String[] {"drive", "accountId"}) {
            ObjectNode input = MAPPER.createObjectNode();
            input.put("workflowName", "parse-and-index");
            input.put("mappingSubject", RepoDocumentMapping.SUBJECT);
            input.put("prune", true);
            input.put(scope, scope.equals("drive") ? "intake" : "acct");
            assertThatThrownBy(() -> action.execute(input, ActionContext.create()))
                    .isInstanceOf(ActionException.class)
                    .hasMessageContaining(scope)
                    .hasMessageContaining("prune");
        }
    }

    @Test
    void withoutPruneNothingIsPrunedAndTheOutputSaysNothingAboutIt() throws Exception {
        FakeIndex index = new FakeIndex(RepoDocumentMapping.SUBJECT, "doc-stale");
        ReplayAction action = new ReplayAction(
                request -> ListDocumentsResponse.getDefaultInstance(),
                new RecordingSubmit(), index);
        ObjectNode input = MAPPER.createObjectNode();
        input.put("workflowName", "parse-and-index");
        input.put("mappingSubject", RepoDocumentMapping.SUBJECT);
        input.put("drive", "intake");
        ObjectNode output = action.execute(input, ActionContext.create());
        assertThat(output.has("pruned")).isFalse();
        assertThat(index.deleted).isEmpty();
        assertThat(index.ids).containsExactly("doc-stale");
    }

    @Test
    void aReplayIdStampsDeterministicRunIdsSoResubmissionDeduplicates() throws Exception {
        RecordingSubmit first = new RecordingSubmit();
        ReplayAction replay = replay(
                request -> ListDocumentsResponse.newBuilder()
                        .addDocuments(document("doc-1"))
                        .addDocuments(document("doc-2"))
                        .build(),
                first);
        ObjectNode input = MAPPER.createObjectNode();
        input.put("workflowName", "parse-and-index");
        input.put("mappingSubject", RepoDocumentMapping.SUBJECT);
        input.put("drive", "intake");
        input.put("replayId", "policy-change-2026-08");

        replay.execute(input, ActionContext.create());
        assertThat(first.submissions).hasSize(2);
        List<String> firstIds = first.submissions.stream()
                .map(envelope -> envelope.path("jobId").asText())
                .toList();
        // Distinct per document, well-formed uuids, and stable across a
        // resubmission of the same replay: submit-workflow's idempotency key
        // does the dedup.
        assertThat(firstIds).doesNotHaveDuplicates().allSatisfy(id ->
                assertThat(java.util.UUID.fromString(id)).isNotNull());

        RecordingSubmit second = new RecordingSubmit();
        replay = replay(
                request -> ListDocumentsResponse.newBuilder()
                        .addDocuments(document("doc-1"))
                        .addDocuments(document("doc-2"))
                        .build(),
                second);
        replay.execute(input, ActionContext.create());
        assertThat(second.submissions.stream()
                .map(envelope -> envelope.path("jobId").asText())
                .toList())
                .containsExactlyElementsOf(firstIds);
    }

    @Test
    void withoutAReplayIdSubmissionsCarryNoJobId() throws Exception {
        RecordingSubmit submit = new RecordingSubmit();
        ReplayAction replay = replay(
                request -> ListDocumentsResponse.newBuilder()
                        .addDocuments(document("doc-1"))
                        .build(),
                submit);
        ObjectNode input = MAPPER.createObjectNode();
        input.put("workflowName", "parse-and-index");
        input.put("mappingSubject", RepoDocumentMapping.SUBJECT);
        input.put("drive", "intake");
        replay.execute(input, ActionContext.create());
        assertThat(submit.submissions.getFirst().has("jobId")).isFalse();
    }
}
