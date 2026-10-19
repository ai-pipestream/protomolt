package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.v1.ParseStatus;
import ai.protomolt.proto.repo.v1.ParserDocument;
import ai.protomolt.proto.repo.v1.ParserResult;
import com.google.protobuf.Any;
import com.google.protobuf.Int64Value;
import io.grpc.Status;
import java.time.Clock;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Fable's slice 1 review round 2, follow-up to #334, extended by #355: host caps that are
 * not byte budgets. Each cap refuses with RESOURCE_EXHAUSTED and no recorded outcome on a
 * host without room, and the identical request commits as revision 1 on a host whose
 * configured limits have room for it.
 */
class HostCapsIT {
    private AccessionFixture f;
    private String account;

    @BeforeEach void open() throws Exception {
        f = new AccessionFixture();
        account = "acct-" + UUID.randomUUID();
    }

    @AfterEach void close() { f.close(); }

    /** Two Any roots in one PARSED fragment on a host whose root cap is one, then the same request on the default host. */
    @Test void aRootCountCapIsNotARecordedRejection() {
        var shape = Any.pack(Int64Value.of(7));
        var base = document(account, "doc-roots", "roots");
        var doc = base.toBuilder()
                .putParserResults("tika", base.getParserResultsOrThrow("tika").toBuilder()
                        .setDocument(ParserDocument.newBuilder().setShape(shape)).build())
                .putParserResults("docling", ParserResult.newBuilder().setParserName("docling").setParserVersion("1")
                        .setStatus(ParseStatus.PARSE_STATUS_OK)
                        .setDocument(ParserDocument.newBuilder().setShape(shape)).build())
                .build();
        var oneRoot = new DocumentSchemaAdmission.Limits(16, 64L * 1024 * 1024, 1, 4L * 1024 * 1024, 32,
                32L * 1024 * 1024, 64 * 1024 * 1024);
        var limited = new DocumentSaves(f.catalog, f.drives, resolver(), oneRoot, bytes -> () -> {}, Clock.systemUTC());
        var address = address(account, "doc-roots");
        var request = create("op-roots", address, doc).build();
        int puts = f.stack.puts.get();

        assertThatThrownBy(() -> limited.save(processAuthority(), request)).isInstanceOfSatisfying(AccessionException.class,
                failure -> assertThat(failure.code()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED));
        assertThat(f.catalog.outcome(account, "op-roots")).as("the operation ID is still usable").isEmpty();
        assertThat(f.catalog.current(address)).isEmpty();
        assertThat(f.stack.puts.get()).isEqualTo(puts);
        assertThat(f.saves.save(processAuthority(), request).getAccession().getRevision()).isEqualTo(1);
    }

    /** 10,001 parser entries: over host 1's configured cap, within the second host's configured room. */
    @Test void parserEntryCapDoesNotClaimTheOperation() {
        var doc = document(account, "doc-parsers", "parsers").toBuilder().clearParserResults();
        for (int i = 0; i < 10_001; i++) {
            var name = "parser-" + i;
            doc.putParserResults(name, ParserResult.newBuilder().setParserName(name).setParserVersion("1")
                    .setStatus(ParseStatus.PARSE_STATUS_OK).build());
        }
        assertHostCapRetriesOnARoomyHost("op-parsers", doc.build(),
                limits(1_000_000, 64, 10_000), limits(1_000_000, 64, 20_000));
    }

    /** Over a million wire values in one Struct list: over host 1's cap, within the second host's room. */
    @Test void wireValueCapDoesNotClaimTheOperation() {
        var doc = document(account, "doc-values", "values").toBuilder();
        var value = com.google.protobuf.Value.newBuilder().setNumberValue(7).build();
        var list = com.google.protobuf.ListValue.newBuilder()
                .addAllValues(java.util.Collections.nCopies(500_001, value));
        doc.getSearchMetadataBuilder().setCustomFields(com.google.protobuf.Struct.newBuilder()
                .putFields("values", com.google.protobuf.Value.newBuilder().setListValue(list).build()));
        assertHostCapRetriesOnARoomyHost("op-values", doc.build(),
                limits(1_000_000, 64, 10_000), limits(2_000_000, 64, 10_000));
    }

    /** A Struct nested ten wire levels deep: over host 1's depth cap, within the second host's depth 64. */
    @Test void wireDepthCapDoesNotClaimTheOperation() {
        var leaf = com.google.protobuf.Value.newBuilder().setNumberValue(1).build();
        var level = com.google.protobuf.Value.newBuilder().setStructValue(com.google.protobuf.Struct.newBuilder()
                .putFields("level", leaf)).build();
        var deep = com.google.protobuf.Value.newBuilder().setStructValue(com.google.protobuf.Struct.newBuilder()
                .putFields("level", level)).build();
        var doc = document(account, "doc-depth", "depth").toBuilder();
        doc.getSearchMetadataBuilder().setCustomFields(com.google.protobuf.Struct.newBuilder()
                .putFields("nested", deep).build());
        assertHostCapRetriesOnARoomyHost("op-depth", doc.build(),
                limits(1_000_000, 8, 10_000), limits(1_000_000, 64, 10_000));
    }

    /** The fixture's byte and count budgets with the wire caps under test. */
    private static DocumentSchemaAdmission.Limits limits(long maxWireValues, int maxWireDepth, int maxParserEntries) {
        return new DocumentSchemaAdmission.Limits(16, 64L * 1024 * 1024, 64, 4L * 1024 * 1024, 32,
                32L * 1024 * 1024, 64 * 1024 * 1024, maxWireValues, maxWireDepth, maxParserEntries);
    }

    /**
     * Host 1 has no room for the candidate: RESOURCE_EXHAUSTED, no recorded outcome and no
     * stack writes. A second host configured with room commits the identical request as
     * revision 1 under the same operation ID.
     */
    private void assertHostCapRetriesOnARoomyHost(String operation, ai.protomolt.proto.repo.v1.Document doc,
            DocumentSchemaAdmission.Limits noRoom, DocumentSchemaAdmission.Limits room) {
        var cramped = new DocumentSaves(f.catalog, f.drives, resolver(), noRoom, bytes -> () -> {}, Clock.systemUTC());
        var roomy = new DocumentSaves(f.catalog, f.drives, resolver(), room, bytes -> () -> {}, Clock.systemUTC());
        var target = address(account, doc.getDocId());
        var request = create(operation, target, doc).build();
        int puts = f.stack.puts.get();
        assertThatThrownBy(() -> cramped.save(processAuthority(), request))
                .isInstanceOfSatisfying(AccessionException.class,
                        failure -> assertThat(failure.code()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED));
        assertThat(f.catalog.outcome(account, operation)).as("the operation ID is still usable").isEmpty();
        assertThat(f.catalog.current(target)).isEmpty();
        assertThat(f.stack.puts.get()).isEqualTo(puts);
        var committed = roomy.save(processAuthority(), request);
        assertThat(committed.getAccession().getRevision()).isEqualTo(1);
        assertThat(committed.getReplayed()).as("the retry commits fresh, not as a replay").isFalse();
    }

}
