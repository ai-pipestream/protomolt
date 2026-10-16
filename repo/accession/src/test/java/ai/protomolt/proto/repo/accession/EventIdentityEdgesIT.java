package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.DocumentEvent;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** #331 from the outside: every split of one delimited string commits, relays once, and keeps a distinct event ID. */
class EventIdentityEdgesIT {
    @Test void everySplitOfOneDelimitedStringIsItsOwnDocumentWithItsOwnEvents() throws Exception {
        try (var f = new AccessionFixture()) {
            var account = "acct-" + UUID.randomUUID();
            // "g|a|b" split three ways across graph_id and graph_address_id, plus the same split with
            // the delimiter moved into doc_id; under the old name these four collided in pairs.
            var addresses = new ArrayList<NodeAddress>();
            for (var parts : new String[][]{{"g", "a|b", "doc"}, {"g|a", "b", "doc"}, {"g|a|b", "x", "doc"},
                    {"g", "a", "b|doc"}}) {
                addresses.add(NodeAddress.newBuilder().setAccountId(account).setGraphId(parts[0])
                        .setGraphAddressId(parts[1]).setDocId(parts[2]).build());
            }
            for (int i = 0; i < addresses.size(); i++) {
                var address = addresses.get(i);
                var request = create("op-" + i, address, document(account, address.getDocId(), "v" + i)).build();
                assertThat(f.saves.save(processAuthority(), request).getAccession().getRevision()).isEqualTo(1);
            }
            var ids = new HashSet<String>();
            for (var address : addresses) {
                assertThat(f.catalog.events(address)).singleElement()
                        .satisfies(event -> assertThat(event.getAddress()).isEqualTo(address));
                ids.add(f.catalog.events(address).getFirst().getEventId());
            }
            assertThat(ids).hasSize(addresses.size());

            // A restarted host derives the same ID for the stored event; the relay delivers each once.
            var restarted = f.restarted();
            try {
                for (var address : addresses)
                    assertThat(DocumentSaves.eventId(address, 1)).isEqualTo(restarted.catalog.events(address).getFirst().getEventId());
                var delivered = new ArrayList<String>();
                new OutboxRelay(restarted.catalog, (key, event) -> delivered.add(event.getEventId())).relayOnce(10);
                assertThat(delivered).containsExactlyInAnyOrderElementsOf(ids);
            } finally {
                restarted.close();
            }
        }
    }

    // Revisions of one document chain distinct IDs, and the same revision number elsewhere differs too.
    @Test void revisionNumbersDoNotCollideAcrossDocuments() throws Exception {
        try (var f = new AccessionFixture()) {
            var account = "acct-" + UUID.randomUUID();
            var one = address(account, "doc-1");
            var two = address(account, "doc-1").toBuilder().setGraphAddressId("ds-2").build();
            f.saves.save(processAuthority(), create("op-1", one, document(account, "doc-1", "a")).build());
            f.saves.save(processAuthority(), create("op-2", two, document(account, "doc-1", "b")).build());
            var ids = new HashSet<String>();
            for (var event : f.catalog.events(one)) ids.add(event.getEventId());
            for (var event : f.catalog.events(two)) ids.add(event.getEventId());
            assertThat(ids).hasSize(2);
            assertThat(f.catalog.events(one)).extracting(DocumentEvent::getEventId)
                    .containsExactly(DocumentSaves.eventId(one, 1));
        }
    }
}
