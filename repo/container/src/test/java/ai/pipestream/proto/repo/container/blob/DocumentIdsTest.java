package ai.pipestream.proto.repo.container.blob;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DocumentIdsTest {

    @Test
    void nodeIdIsDeterministic() {
        UUID a = DocumentIds.nodeId("doc-1", "node-a", "acct-1", "graph-1");
        UUID b = DocumentIds.nodeId("doc-1", "node-a", "acct-1", "graph-1");
        assertThat(a).isEqualTo(b);
    }

    @Test
    void nodeIdRejectsBlankSegments() {
        assertThatThrownBy(() -> DocumentIds.nodeId(" ", "node-a", "acct-1", "graph-1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("docId");
        assertThatThrownBy(() -> DocumentIds.nodeId("doc-1", null, "acct-1", "graph-1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("graphAddressId");
        assertThatThrownBy(() -> DocumentIds.nodeId("doc-1", "node-a", "", "graph-1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("accountId");
    }

    @Test
    void nodeIdRejectsBlankGraphSegmentWithIntakeMessage() {
        // The dead "blank means intake" convention is unrepresentable.
        assertThatThrownBy(() -> DocumentIds.nodeId("doc-1", "node-a", "acct-1", " "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("graphId cannot be null or blank (intake rows carry intake:<accountId>)");
        assertThatThrownBy(() -> DocumentIds.nodeId("doc-1", "node-a", "acct-1", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("graphId cannot be null or blank (intake rows carry intake:<accountId>)");
    }

    @Test
    void differentGraphYieldsDifferentNodeId() {
        // The cross-graph collision guard: same doc at the same hop name in two
        // independent graphs must resolve to DIFFERENT rows.
        UUID g1 = DocumentIds.nodeId("doc-1", "opensearch-sink", "acct-1", "graph-1");
        UUID g2 = DocumentIds.nodeId("doc-1", "opensearch-sink", "acct-1", "graph-2");
        assertThat(g1).isNotEqualTo(g2);
    }

    @Test
    void blobIdDiffersFromNodeIdForSameLogicalCoordinates() {
        UUID blob = DocumentIds.blobId("doc-1", "ds-1", "acct-1");
        UUID node = DocumentIds.nodeId("doc-1", "ds-1", "acct-1", "intake:acct-1");
        assertThat(blob).isNotEqualTo(node);
    }

    @Test
    void blobIdIsDeterministicAndValidated() {
        assertThat(DocumentIds.blobId("doc-1", "ds-1", "acct-1"))
                .isEqualTo(DocumentIds.blobId("doc-1", "ds-1", "acct-1"));
        assertThatThrownBy(() -> DocumentIds.blobId("doc-1", null, "acct-1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("datasourceId");
    }
}
