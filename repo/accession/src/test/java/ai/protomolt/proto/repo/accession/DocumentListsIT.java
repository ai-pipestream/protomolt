package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static ai.protomolt.proto.repo.accession.DocumentDeletesIT.assertCode;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.Principal;
import ai.protomolt.proto.repo.v2.*;
import io.grpc.Status;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class DocumentListsIT {
    @Test void pagesFilterAclTombstonesAndScopeWithoutReadingObjects() throws Exception {
        try (var f = new AccessionFixture()) {
            for (var id : List.of("a", "b", "c", "d", "e"))
                f.saves.save(processAuthority(), create("save-" + id, address("account", id),
                        document("account", id, id, id.equals("b") || id.equals("d") ? "other" : "reader")).build());
            var deleted = address("account", "c");
            new DocumentDeletes(f.catalog, Clock.systemUTC()).delete(processAuthority(), DeleteDocumentRequest.newBuilder()
                    .setAddress(deleted).setExpected(at(1)).setOperationId("delete").build());
            f.saves.save(processAuthority(), create("other-account", address("other", "z"), document("other", "z", "z", "reader")).build());
            f.stack.reset();
            var reader = new RepositoryCaller("reader", false, Set.of("account"),
                    Set.of(Principal.newBuilder().setIdentityType("service").setIdentity("reader").build()));
            var lists = new DocumentLists(f.catalog);
            var request = ListDocumentsRequest.newBuilder().setAccountId("account").setPageSize(1).build();
            var page = lists.list(reader, request);
            assertThat(page.getDocumentsList()).extracting(d -> d.getAddress().getDocId()).containsExactly("a");
            assertThat(page.getNextPageToken()).isNotEmpty();
            var token = CatalogPageToken.parseFrom(java.util.Base64.getUrlDecoder().decode(page.getNextPageToken()));
            assertThat(token.getAfter().getDocId()).isEqualTo("a");
            var last = lists.list(reader, request.toBuilder().setPageToken(page.getNextPageToken()).build());
            assertThat(last.getDocumentsList()).extracting(d -> d.getAddress().getDocId()).containsExactly("e");
            assertThat(last.getNextPageToken()).isEmpty();
            assertThat(lists.list(reader, request.toBuilder().setPageSize(10).setIncludeTombstoned(true).build()).getDocumentsList())
                    .extracting(d -> d.getAddress().getDocId()).containsExactly("a", "c", "e");
            assertThat(lists.list(reader, request.toBuilder().setGraphId("absent").build()).getDocumentsCount()).isZero();
            assertThat(lists.list(reader, request.toBuilder().setGraphAddressId("absent").build()).getDocumentsCount()).isZero();
            assertCode(Status.Code.INVALID_ARGUMENT, () -> lists.list(reader, request.toBuilder().setIncludeTombstoned(true).setPageToken(page.getNextPageToken()).build()));
            assertCode(Status.Code.INVALID_ARGUMENT, () -> lists.list(reader, request.toBuilder().setPageToken("not a token").build()));
            assertCode(Status.Code.PERMISSION_DENIED, () -> lists.list(reader, request.toBuilder().setAccountId("other").build()));
            assertThat(f.stack.opens.get()).isZero();
            assertThat(f.stack.puts.get()).isZero();
            assertThat(DocumentSaves.VALIDATOR.validate(page).valid()).isTrue();
        }
    }

    @Test void addressOrderAndProjectionSurviveRestart() throws Exception {
        try (var f = new AccessionFixture()) {
            var addresses = List.of(address("a", "z").toBuilder().setGraphId("g2").build(),
                    address("a", "é").toBuilder().setGraphId("g1").build(),
                    address("a", "a").toBuilder().setGraphId("g1").build());
            for (int i = 0; i < addresses.size(); i++)
                f.saves.save(processAuthority(), create("save-" + i, addresses.get(i), document("a", addresses.get(i).getDocId(), "title", "reader")).build());
            try (var restarted = f.restarted()) {
                var lists = new DocumentLists(restarted.catalog);
                var request = ListDocumentsRequest.newBuilder().setAccountId("a").setPageSize(1);
                var seen = new ArrayList<ai.protomolt.proto.repo.v1.NodeAddress>();
                do {
                    var page = lists.list(processAuthority(), request.build());
                    page.getDocumentsList().forEach(d -> seen.add(d.getAddress()));
                    request.setPageToken(page.getNextPageToken());
                } while (!request.getPageToken().isEmpty());
                assertThat(seen).containsExactly(addresses.get(2), addresses.get(1), addresses.get(0));
                var rows = restarted.catalog.documents("a", "g1", "", false, java.util.Optional.empty(), 10);
                assertThat(rows).hasSize(2);
                assertThat(rows.getFirst().entry().getOwnership()).isEqualTo(ownership("a", "reader"));
                assertThat(rows.getFirst().entry().getDocId()).isEqualTo("a");
                assertThat(rows.getFirst().committedAt()).isEqualTo(restarted.catalog.manifest(addresses.get(2), 1).orElseThrow().getCommittedAt());
            }
        }
    }
    @Test void nextPageUsesCurrentAclRatherThanAnEarlierProjection() throws Exception {
        try (var f = new AccessionFixture()) {
            for (var id : List.of("a", "b")) f.saves.save(processAuthority(), create("save-" + id, address("a", id), document("a", id, id, "reader")).build());
            var reader = new RepositoryCaller("reader", false, Set.of("a"),
                    Set.of(Principal.newBuilder().setIdentityType("service").setIdentity("reader").build()));
            var lists = new DocumentLists(f.catalog);
            var request = ListDocumentsRequest.newBuilder().setAccountId("a").setPageSize(1).build();
            var first = lists.list(reader, request);
            var replacement = document("a", "b", "changed", "other").toBuilder().clearBlobBag().clearParserResults().build();
            f.saves.save(processAuthority(), SaveDocumentRequest.newBuilder().setOperationId("restrict")
                    .setTarget(DocumentTarget.newBuilder().setAddress(address("a", "b"))).setExpected(at(1))
                    .setDocument(replacement).addWrites(put(DocumentPart.DOCUMENT_PART_CORE)).setOwner(intakeOwner()).build());
            var next = lists.list(reader, request.toBuilder().setPageToken(first.getNextPageToken()).build());
            assertThat(next.getDocumentsCount()).isZero();
            assertThat(next.getNextPageToken()).isEmpty();
        }
    }

}
