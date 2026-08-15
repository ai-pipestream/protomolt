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
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Replay pages the listing and rides the submit action: one durable run
 * per stored document, both identities explicit on every submission.
 */
class ReplayActionTest {

    static final ObjectMapper MAPPER = new ObjectMapper();

    /** Records every submission and answers with a synthetic job id. */
    static final class RecordingSubmit implements ProtoAction {

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
        public ObjectNode execute(ObjectNode input, ActionContext context) {
            submissions.add(input.deepCopy());
            ObjectNode result = MAPPER.createObjectNode();
            result.put("ok", true);
            result.put("jobId", "job-" + submissions.size());
            return result;
        }
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
        ReplayAction replay = new ReplayAction(request -> {
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
        ReplayAction replay = new ReplayAction(
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
        ReplayAction replay = new ReplayAction(
                request -> ListDocumentsResponse.getDefaultInstance(), submit);
        ObjectNode input = MAPPER.createObjectNode();
        input.put("workflowName", "parse-and-index");
        input.put("mappingSubject", RepoDocumentMapping.SUBJECT);
        input.put("drive", "intake");
        ObjectNode output = replay.execute(input, ActionContext.create());
        assertThat(output.path("submitted").asInt()).isZero();
        assertThat(submit.submissions).isEmpty();
    }
}
