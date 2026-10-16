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

/** Fable's slice 1 review round 2, follow-up to #334: host caps that are not byte budgets. */
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
    @Test void parserEntryCapDoesNotClaimTheOperation() {
        var doc = document(account, "doc-parsers", "parsers").toBuilder().clearParserResults();
        for (int i = 0; i < 10_001; i++) {
            var name = "parser-" + i;
            doc.putParserResults(name, ParserResult.newBuilder().setParserName(name).setParserVersion("1")
                    .setStatus(ParseStatus.PARSE_STATUS_OK).build());
        }
        assertHostCapLeavesOperationUsable("op-parsers", doc.build());
    }

    @Test void wireValueCapDoesNotClaimTheOperation() {
        var doc = document(account, "doc-values", "values").toBuilder();
        var value = com.google.protobuf.Value.newBuilder().setNumberValue(7).build();
        var list = com.google.protobuf.ListValue.newBuilder()
                .addAllValues(java.util.Collections.nCopies(500_001, value));
        doc.getSearchMetadataBuilder().setCustomFields(com.google.protobuf.Struct.newBuilder()
                .putFields("values", com.google.protobuf.Value.newBuilder().setListValue(list).build()));
        assertHostCapLeavesOperationUsable("op-values", doc.build());
    }

    private void assertHostCapLeavesOperationUsable(String operation, ai.protomolt.proto.repo.v1.Document doc) {
        var target = address(account, doc.getDocId());
        int puts = f.stack.puts.get();
        assertThatThrownBy(() -> f.saves.save(processAuthority(), create(operation, target, doc).build()))
                .isInstanceOfSatisfying(AccessionException.class,
                        failure -> assertThat(failure.code()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED));
        assertThat(f.catalog.outcome(account, operation)).isEmpty();
        assertThat(f.catalog.current(target)).isEmpty();
        assertThat(f.stack.puts.get()).isEqualTo(puts);
        assertThat(f.saves.save(processAuthority(), create(operation, target,
                document(account, doc.getDocId(), "smaller candidate")).build()).getAccession().getRevision()).isEqualTo(1);
    }

}
