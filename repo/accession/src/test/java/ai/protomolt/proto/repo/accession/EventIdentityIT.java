package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.v1.NodeAddress;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Regression from Fable's slice 1 review, issue #331. */
class EventIdentityIT {
    private AccessionFixture f;
    private String account;

    @BeforeEach void open() throws Exception {
        f = new AccessionFixture();
        account = "acct-" + UUID.randomUUID();
    }

    @AfterEach void close() { f.close(); }

    @Test void eventIdsOfDistinctAddressesNeverCollide() {
        var first = NodeAddress.newBuilder().setAccountId(account).setGraphId("g")
                .setGraphAddressId("a|b").setDocId("doc-pipe").build();
        var second = NodeAddress.newBuilder().setAccountId(account).setGraphId("g|a")
                .setGraphAddressId("b").setDocId("doc-pipe").build();
        assertThat(first).as("two different call numbers").isNotEqualTo(second);

        f.saves.save(processAuthority(), create("op-pipe-1", first, document(account, "doc-pipe", "one")).build());
        var saved = f.saves.save(processAuthority(), create("op-pipe-2", second, document(account, "doc-pipe", "two")).build());

        assertThat(saved.getAccession().getRevision()).isEqualTo(1);
        assertThat(f.catalog.events(second).getFirst().getEventId())
                .isNotEqualTo(f.catalog.events(first).getFirst().getEventId());
    }

    @Test void eventIdentityIncludesEveryAddressFieldAndRevisionWithoutDelimiterAmbiguity() throws Exception {
        var a = NodeAddress.newBuilder().setAccountId("acct|é").setGraphId("g|x")
                .setGraphAddressId("node|z").setDocId("doc|🚀").build();
        var id = DocumentSaves.eventId(a, Long.MAX_VALUE);
        assertThat(DocumentSaves.eventId(NodeAddress.parseFrom(a.toByteArray()), Long.MAX_VALUE)).isEqualTo(id);
        assertThat(DocumentSaves.eventId(a, Long.MAX_VALUE - 1)).isNotEqualTo(id);
        assertThat(DocumentSaves.eventId(a.toBuilder().setAccountId("other").build(), Long.MAX_VALUE)).isNotEqualTo(id);
        assertThat(DocumentSaves.eventId(a.toBuilder().setGraphId("other").build(), Long.MAX_VALUE)).isNotEqualTo(id);
        assertThat(DocumentSaves.eventId(a.toBuilder().setGraphAddressId("other").build(), Long.MAX_VALUE)).isNotEqualTo(id);
        assertThat(DocumentSaves.eventId(a.toBuilder().setDocId("other").build(), Long.MAX_VALUE)).isNotEqualTo(id);
        var shifted = a.toBuilder().setAccountId("acct").setGraphId("é|g|x").build();
        assertThat(DocumentSaves.eventId(shifted, Long.MAX_VALUE)).isNotEqualTo(id);
        java.util.UUID.fromString(id);
    }

}
