package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import io.grpc.Status;
import java.time.Clock;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Regression from Fable's slice 1 review, issue #334. */
class AdmissionByteBudgetIT {
    private AccessionFixture f;
    private String account;

    @BeforeEach void open() throws Exception {
        f = new AccessionFixture();
        account = "acct-" + UUID.randomUUID();
    }

    @AfterEach void close() { f.close(); }

    @Test void aHostByteLimitIsNotARecordedRejection() {
        var small = new DocumentSchemaAdmission.Limits(16, 64, 64, 4L * 1024 * 1024, 32,
                32L * 1024 * 1024, 64 * 1024 * 1024);
        var limited = new DocumentSaves(f.catalog, f.drives, resolver(), small, bytes -> () -> {}, Clock.systemUTC());
        var address = address(account, "doc-bytes");
        var request = create("op-bytes", address, document(account, "doc-bytes", "b")).build();

        assertThatThrownBy(() -> limited.save(processAuthority(), request)).isInstanceOfSatisfying(AccessionException.class,
                failure -> assertThat(failure.code()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED));
        assertThat(f.catalog.outcome(account, "op-bytes")).as("the operation ID is still usable").isEmpty();
        assertThat(f.saves.save(processAuthority(), request).getAccession().getRevision()).isEqualTo(1);
    }

    @Test void decodedByteLimitsAreUnrecordedAtRootNestedAndBetweenRootBoundaries() {
        var scalar = com.google.protobuf.Any.pack(com.google.protobuf.Int64Value.of(7));
        var nested = com.google.protobuf.Any.pack(
                ai.protomolt.proto.repo.v1.ParserDocument.newBuilder().setShape(scalar).build());
        var anyDefinition = ai.protomolt.proto.repo.admission.BuiltinDocumentSchema.definition(
                ai.protomolt.proto.repo.v1.ParserDocument.getDescriptor());
        ai.protomolt.proto.repo.admission.DocumentSchemaAdmission.Resolver resolver = selection ->
                selection.typeUrl().equals(nested.getTypeUrl()) ? anyDefinition : resolver().select(selection);
        for (int kind = 0; kind < 3; kind++) {
            var address = address(account, "decoded-" + kind);
            var doc = document(account, address.getDocId(), "decoded").toBuilder()
                    .setStructuredData(kind == 1 ? nested : scalar);
            if (kind == 2) doc.putParserResults("tika", doc.getParserResultsOrThrow("tika").toBuilder()
                    .setDocument(ai.protomolt.proto.repo.v1.ParserDocument.newBuilder().setShape(scalar)).build());
            int limit = kind == 0 ? scalar.getValue().size() - 1
                    : kind == 1 ? nested.getValue().size() : scalar.getValue().size();
            var small = new DocumentSchemaAdmission.Limits(16, 64L * 1024 * 1024, 64, 4L * 1024 * 1024,
                    32, 32L * 1024 * 1024, limit);
            var limited = new DocumentSaves(f.catalog, f.drives, resolver, small, bytes -> () -> {}, Clock.systemUTC());
            var adequate = new DocumentSaves(f.catalog, f.drives, resolver, limits(), bytes -> () -> {}, Clock.systemUTC());
            var request = create("decoded-" + kind, address, doc.build()).build();
            int puts = f.stack.puts.get();
            assertThatThrownBy(() -> limited.save(processAuthority(), request))
                    .isInstanceOfSatisfying(AccessionException.class, failure -> {
                        assertThat(failure.code()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
                        assertThat(failure.detail()).isEmpty();
                    });
            assertThat(f.catalog.outcome(account, request.getOperationId())).isEmpty();
            assertThat(f.catalog.current(address)).isEmpty();
            assertThat(f.stack.puts.get()).isEqualTo(puts);
            assertThat(adequate.save(processAuthority(), request).getAccession().getRevision()).isEqualTo(1);
        }
    }
}
