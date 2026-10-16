package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.DocumentTarget;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * OutboxRelay drains "in commit order". When one event's publish fails, a later
 * revision of the same document must not be published ahead of it, or a consumer
 * keyed by document sees revision 2 before revision 1.
 */
class OutboxRetryOrderIT {
    @Test void aFailedPublishHoldsBackLaterEventsOfTheSameDocument() throws Exception {
        try (var f = new AccessionFixture()) {
            var account = "acct-" + UUID.randomUUID();
            var a = address(account, "doc-a");
            f.saves.save(processAuthority(), create("a-1", a, document(account, "doc-a", "a1")).build());
            var core = Document.newBuilder().setDocId("doc-a").setOwnership(ownership(account))
                    .setSearchMetadata(SearchMetadata.newBuilder().setTitle("a2")).build();
            f.saves.save(processAuthority(), SaveDocumentRequest.newBuilder().setOperationId("a-2")
                    .setTarget(DocumentTarget.newBuilder().setAddress(a)).setExpected(at(1)).setDocument(core)
                    .addWrites(put(DocumentPart.DOCUMENT_PART_CORE)).setOwner(intakeOwner()).build());

            var b = address(account, "doc-b");
            f.saves.save(processAuthority(), create("b-1", b, document(account, "doc-b", "b1")).build());
            var delivered = new ArrayList<String>();
            var failFirst = new AtomicBoolean(true);
            var relay = new OutboxRelay(f.catalog, (key, event) -> {
                if (event.getAddress().equals(a) && event.getRevision() == 1 && failFirst.getAndSet(false)) throw new java.io.IOException("broker timeout");
                delivered.add(event.getAddress().getDocId() + "@" + event.getRevision());
            });
            assertThat(relay.relayOnce(10)).isEqualTo(1);
            assertThat(delivered).containsExactly("doc-b@1");
            assertThat(f.catalog.pendingEvents(10).stream().map(event -> event.attempts()).toList())
                    .containsExactly(1, 0);
            assertThat(relay.relayOnce(10)).isEqualTo(2);
            assertThat(delivered).as("doc-a's events reach the broker in revision order").containsExactly("doc-b@1", "doc-a@1", "doc-a@2");
        }
    }
}
