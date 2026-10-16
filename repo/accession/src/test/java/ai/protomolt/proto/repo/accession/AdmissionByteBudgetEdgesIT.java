package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.v2.*;
import ai.protomolt.proto.repo.v2.DocumentPart;
import io.grpc.Status;
import java.time.Clock;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * #334 from the outside: the fragment byte limit is a total over every fragment admission
 * decodes, written or sourced, with an exact boundary; exceeding it is RESOURCE_EXHAUSTED,
 * unrecorded, and uploads nothing.
 */
class AdmissionByteBudgetEdgesIT {
    private AccessionFixture f;
    private String account;

    @BeforeEach void open() throws Exception {
        f = new AccessionFixture();
        account = "acct-" + UUID.randomUUID();
    }

    @AfterEach void close() { f.close(); }

    // Each fragment fits on its own; only their sum crosses the limit, and the sum itself is allowed.
    @Test void theLimitIsATotalOverFragmentsWithAnInclusiveBoundary() {
        var probe = address(account, "doc-p");
        f.saves.save(processAuthority(), create("op-probe", probe, document(account, "doc-p", "same")).build());
        long total = 0, largest = 0;
        for (var part : f.catalog.manifest(probe, 1).orElseThrow().getPartsList()) {
            total += part.getEntry().getSizeBytes();
            largest = Math.max(largest, part.getEntry().getSizeBytes());
        }
        assertThat(largest).isLessThan(total);
        f.stack.reset();

        var over = address(account, "doc-o");
        var request = create("op-over", over, document(account, "doc-o", "same")).build();
        assertExhausted(saves(total - 1), request);
        assertThat(f.catalog.outcome(account, "op-over")).isEmpty();
        assertThat(f.stack.puts.get()).isZero();

        var exact = saves(total).save(processAuthority(), request);
        assertThat(exact.getAccession().getRevision()).isEqualTo(1);
        assertThat(exact.getReplayed()).as("the earlier refusal claimed nothing").isFalse();
    }

    // Sourced bytes are decoded by admission, so they count; a too-large source is refused before upload.
    @Test void sourcedBytesCountTowardTheLimit() {
        var source = address(account, "doc-s");
        f.saves.save(processAuthority(), create("op-source", source, document(account, "doc-s", "source")).build());
        var stored = f.catalog.manifest(source, 1).orElseThrow();
        long parsed = stored.getPartsList().stream().filter(p -> p.getEntry().getSelector().getPart()
                == DocumentPart.DOCUMENT_PART_PARSED).findFirst().orElseThrow().getEntry().getSizeBytes();
        f.stack.reset();

        var request = SaveDocumentRequest.newBuilder().setOperationId("op-sourced")
                .setTarget(DocumentTarget.newBuilder().setAddress(source)).setExpected(at(1)).setOwner(intakeOwner())
                .addSources(PartSource.newBuilder().setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_PARSED))
                        .setFromAddress(source).setFromRevision(1)).build();
        assertExhausted(saves(parsed - 1), request);
        assertThat(f.stack.opens.get()).as("the source was fetched and measured").isEqualTo(1);
        assertThat(f.stack.puts.get()).isZero();
        assertThat(f.catalog.outcome(account, "op-sourced")).isEmpty();
        assertThat(saves(parsed).save(processAuthority(), request).getAccession().getRevision()).isEqualTo(2);
    }

    // A host limit is not the caller's fault: a retry under adequate limits commits, and replays then.
    @Test void aRetryAfterTheHostLimitIsRaisedCommitsAndThenReplays() {
        var address = address(account, "doc-r");
        var request = create("op-retry", address, document(account, "doc-r", "r")).build();
        assertExhausted(saves(16), request);
        var saved = f.saves.save(processAuthority(), request);
        assertThat(f.saves.save(processAuthority(), request)).isEqualTo(saved.toBuilder().setReplayed(true).build());
        assertThat(saves(16).save(processAuthority(), request).getReplayed())
                .as("a replay answers from the catalog without admitting bytes again").isTrue();
    }

    private DocumentSaves saves(long maxFragmentBytes) {
        var limits = new DocumentSchemaAdmission.Limits(16, maxFragmentBytes, 64, 4L * 1024 * 1024, 32,
                32L * 1024 * 1024, 64 * 1024 * 1024);
        return new DocumentSaves(f.catalog, f.drives, resolver(), limits, bytes -> () -> {}, Clock.systemUTC());
    }

    private static void assertExhausted(DocumentSaves saves, SaveDocumentRequest request) {
        assertThatThrownBy(() -> saves.save(processAuthority(), request)).isInstanceOfSatisfying(AccessionException.class,
                failure -> {
                    assertThat(failure.code()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
                    assertThat(failure.detail()).isEmpty();
                });
    }
}
