package ai.protomolt.proto.repo.accession;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.catalog.Catalog;
import ai.protomolt.proto.repo.stack.Stack;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.StoredManifest;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The repair outcome a catalog reports when a document is purged between the two
 * repair transactions. The real catalog cannot be driven into that window on demand,
 * so a stand-in answers the second manifest lookup with nothing.
 */
class CatalogEntryRebuildTest {
    private static final NodeAddress ADDRESS = NodeAddress.newBuilder()
            .setAccountId("acct").setGraphId("graph").setGraphAddressId("ds").setDocId("doc").build();
    private static final StoredManifest TOMBSTONE = StoredManifest.newBuilder().setRevision(1).setTombstone(true).build();

    @Test void purgeBetweenTheTypeRepairAndTheEntryRewriteIsReportedAsPurged() {
        var calls = new ArrayList<String>();
        var catalog = catalog(calls, List.of(Optional.of(TOMBSTONE), Optional.empty()));

        var result = new CatalogEntryRebuild(catalog, reads(catalog)).revision(ADDRESS, 1, true);

        assertThat(result.tombstone()).isTrue();
        assertThat(result.revisionTypeRewritten()).isTrue();
        assertThat(result.currentEntryRewritten()).isFalse();
        assertThat(result.purgedBeforeEntryRepair()).isTrue();
        assertThat(calls).containsExactly("manifest", "rewriteRevisionType", "rewriteEntry", "manifest");
    }

    @Test void aRevisionThatIsNoLongerCurrentIsNotReportedAsPurged() {
        var calls = new ArrayList<String>();
        var catalog = catalog(calls, List.of(Optional.of(TOMBSTONE), Optional.of(TOMBSTONE)));

        var result = new CatalogEntryRebuild(catalog, reads(catalog)).revision(ADDRESS, 1, true);

        assertThat(result.revisionTypeRewritten()).isTrue();
        assertThat(result.currentEntryRewritten()).isFalse();
        assertThat(result.purgedBeforeEntryRepair()).isFalse();
    }

    @Test void auditOnlyDoesNotLookTheManifestUpASecondTime() {
        var calls = new ArrayList<String>();
        var catalog = catalog(calls, List.of(Optional.of(TOMBSTONE)));

        var result = new CatalogEntryRebuild(catalog, reads(catalog)).revision(ADDRESS, 1, false);

        assertThat(result.purgedBeforeEntryRepair()).isFalse();
        assertThat(calls).containsExactly("manifest");
    }

    /** A catalog whose manifest lookups answer in the given order and whose entry rewrite is turned down. */
    private static Catalog catalog(List<String> calls, List<Optional<StoredManifest>> manifests) {
        var lookups = new ArrayList<>(manifests);
        return (Catalog) Proxy.newProxyInstance(Catalog.class.getClassLoader(), new Class<?>[] {Catalog.class},
                (proxy, method, args) -> {
                    calls.add(method.getName());
                    return switch (method.getName()) {
                        case "manifest" -> {
                            assertThat(args[0]).isEqualTo(ADDRESS);
                            assertThat(args[1]).isEqualTo(1L);
                            yield lookups.remove(0);
                        }
                        case "rewriteRevisionType" -> true;
                        case "rewriteEntry" -> false;
                        default -> throw new UnsupportedOperationException("Unexpected Catalog call: " + method.getName());
                    };
                });
    }

    /** Reads are only consulted for a revision with a CORE part; a tombstone has none. */
    private static DocumentReads reads(Catalog catalog) {
        var stack = (Stack) Proxy.newProxyInstance(Stack.class.getClassLoader(), new Class<?>[] {Stack.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("stackId")) return "stack";
                    throw new UnsupportedOperationException("Unexpected Stack call: " + method.getName());
                });
        return new DocumentReads(catalog, new DocumentSaves.Drives("drive", Map.of("drive", stack)),
                DocumentSchemaAdmission.Limits.SERVICE_DEFAULTS, bytes -> () -> {}, Clock.systemUTC());
    }
}
