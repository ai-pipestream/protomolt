package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.DocumentEvent;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.DocumentTarget;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** §10 events and §15.9: the relay drains the outbox in commit order, keyed by document. */
class OutboxOrderIT {
    private AccessionFixture f;
    private String account;

    @BeforeEach void open() throws Exception {
        f = new AccessionFixture();
        account = "acct-" + UUID.randomUUID();
    }

    @AfterEach void close() { f.close(); }

    @Test void interleavedDocumentsRelayInCommitOrderWithTheirOwnKeys() throws Exception {
        var a = address(account, "doc-a");
        var b = address(account, "doc-b");
        f.saves.save(processAuthority(), create("a-1", a, document(account, "doc-a", "a1")).build());
        f.saves.save(processAuthority(), create("b-1", b, document(account, "doc-b", "b1")).build());
        f.saves.save(processAuthority(), titleSave("a-2", a, 1, "a2"));
        f.saves.save(processAuthority(), titleSave("b-2", b, 1, "b2"));
        f.saves.save(processAuthority(), titleSave("a-3", a, 2, "a3"));

        var delivered = new ArrayList<String>();
        var relay = new OutboxRelay(f.catalog, (key, event) ->
                delivered.add(key + "@" + event.getRevision() + "/" + event.getOperationId()));
        // Pages smaller than the backlog still keep commit order across passes.
        assertThat(relay.relayOnce(2)).isEqualTo(2);
        assertThat(relay.relayOnce(2)).isEqualTo(2);
        assertThat(relay.relayOnce(2)).isEqualTo(1);
        assertThat(relay.relayOnce(2)).isZero();
        // The key is the full canonical address, never the doc_id alone: two accounts
        // sharing a doc_id must not interleave their revisions on one partition.
        var keyA = ai.protomolt.proto.repo.catalog.CatalogRecords.eventKey(a);
        var keyB = ai.protomolt.proto.repo.catalog.CatalogRecords.eventKey(b);
        assertThat(keyA).isNotEqualTo("doc-a");
        assertThat(keyB).isNotEqualTo("doc-b");
        assertThat(delivered).containsExactly(keyA + "@1/a-1", keyB + "@1/b-1", keyA + "@2/a-2", keyB + "@2/b-2",
                keyA + "@3/a-3");
        assertThat(f.catalog.pendingEvents(10)).isEmpty();
    }

    // Each event links to its predecessor and carries a distinct, stable ID.
    @Test void eventsChainRevisionsAndKeepTheirIdsAcrossRedelivery() throws Exception {
        var a = address(account, "doc-chain");
        f.saves.save(processAuthority(), create("c-1", a, document(account, "doc-chain", "c1")).build());
        f.saves.save(processAuthority(), titleSave("c-2", a, 1, "c2"));
        f.saves.save(processAuthority(), titleSave("c-3", a, 2, "c3"));
        var events = f.catalog.events(a);
        assertThat(events).extracting(DocumentEvent::getPreviousRevision).containsExactly(0L, 1L, 2L);
        assertThat(events).extracting(DocumentEvent::getOperationId).containsExactly("c-1", "c-2", "c-3");
        assertThat(events).extracting(DocumentEvent::getEventId).doesNotHaveDuplicates();

        var failures = new ArrayList<String>();
        var relay = new OutboxRelay(f.catalog, (key, event) -> {
            failures.add(event.getEventId());
            throw new java.io.IOException("broker down");
        });
        assertThat(relay.relayOnce(10)).isZero();
        assertThat(relay.relayOnce(10)).isZero();
        var pending = f.catalog.pendingEvents(10);
        assertThat(pending).hasSize(3);
        assertThat(pending.stream().map(event -> event.attempts()).toList()).containsExactly(2, 0, 0);
        assertThat(failures).containsExactly(events.getFirst().getEventId(), events.getFirst().getEventId());
        assertThat(pending.stream().map(event -> event.event()).toList()).isEqualTo(events);

        List<String> delivered = new ArrayList<>();
        new OutboxRelay(f.catalog, (key, event) -> delivered.add(event.getEventId())).relayOnce(10);
        assertThat(delivered).containsExactlyElementsOf(events.stream().map(DocumentEvent::getEventId).toList());
        assertThat(f.catalog.events(a)).isEqualTo(events);
    }

    @Test void markRelayedStillPersistsWhenMarkAttemptsThrows() throws Exception {
        var a = address(account, "doc-ok");
        var b = address(account, "doc-fail");
        f.saves.save(processAuthority(), create("ok-1", a, document(account, "doc-ok", "ok")).build());
        f.saves.save(processAuthority(), create("fail-1", b, document(account, "doc-fail", "fail")).build());

        var spied = (ai.protomolt.proto.repo.catalog.Catalog) java.lang.reflect.Proxy.newProxyInstance(
                ai.protomolt.proto.repo.catalog.Catalog.class.getClassLoader(),
                new Class<?>[]{ai.protomolt.proto.repo.catalog.Catalog.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("markAttempts")) {
                        throw new RuntimeException("simulated markAttempts failure");
                    }
                    try {
                        return method.invoke(f.catalog, args);
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
        var relay = new OutboxRelay(spied, (key, event) -> {
            if (event.getAddress().equals(b)) throw new java.io.IOException("broker failure on b");
        });
        assertThatThrownBy(() -> relay.relayOnce(10))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("simulated markAttempts failure");
        var remaining = f.catalog.pendingEvents(10);
        assertThat(remaining).hasSize(1);
        assertThat(remaining.getFirst().event().getAddress()).isEqualTo(b);
    }

    private static SaveDocumentRequest titleSave(String operation, NodeAddress address, long expected, String title) {
        var core = Document.newBuilder().setDocId(address.getDocId()).setOwnership(ownership(address.getAccountId()))
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle(title)).build();
        return SaveDocumentRequest.newBuilder().setOperationId(operation)
                .setTarget(DocumentTarget.newBuilder().setAddress(address)).setExpected(at(expected)).setDocument(core)
                .addWrites(put(DocumentPart.DOCUMENT_PART_CORE)).setOwner(intakeOwner()).build();
    }
}
