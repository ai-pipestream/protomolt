package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.admission.DocumentAdmissionReservations;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.GetDocumentRequest;
import ai.protomolt.proto.repo.v2.PartSelection;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.ReadDocumentRequest;
import ai.protomolt.proto.repo.v2.ReadDocumentResponse;
import ai.protomolt.proto.repo.v2.StackRef;
import ai.protomolt.proto.repo.v2.StoredManifest;
import io.grpc.Status;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.DriverManager;
import java.time.Clock;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The read byte budget on the unary and bulk read paths (issues #320, #423):
 * {@code GetDocument} and the per-item bulk {@code ReadDocuments} read reserve each
 * part's catalog length before opening or allocating anything, a tampered oversized
 * catalog length is refused RESOURCE_EXHAUSTED without a single stack open, every
 * reservation is released after success and after failure, and a read the remaining
 * budget does not cover fails fast instead of buffering.
 */
class UnaryReadBudgetIT {
    private static final long TAMPERED_LENGTH = 1536L * 1024 * 1024;

    private AccessionFixture f;
    private String account;

    @BeforeEach void open() throws Exception {
        f = new AccessionFixture();
        account = "acct-" + UUID.randomUUID();
    }

    @AfterEach void close() { f.close(); }

    @Test void getDocumentRefusesATamperedCatalogLengthWithoutOpeningTheStack() throws Exception {
        var address = address(account, "doc-unary-tampered");
        f.saves.save(processAuthority(), create("op-1", address,
                document(account, "doc-unary-tampered", "title")).build());
        corruptCorePartLength(TAMPERED_LENGTH);

        var opens = new AtomicInteger();
        var held = new AtomicLong();
        var reads = reads(countingStack(opens, new AtomicInteger(), new AtomicLong()), tracking(held));

        assertThatThrownBy(() -> reads.getDocument(processAuthority(), coreGet(address)))
                .isInstanceOfSatisfying(AccessionException.class, failure ->
                        assertThat(failure.code()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED));
        assertThat(opens.get()).as("the tampered length never reaches an open stream or a buffer").isZero();
        assertThat(held.get()).as("the budget is whole again after the refused read").isZero();
    }

    @Test void bulkReadRefusesATamperedCatalogLengthWithoutOpeningTheStack() throws Exception {
        var address = address(account, "doc-bulk-tampered");
        f.saves.save(processAuthority(), create("op-1", address,
                document(account, "doc-bulk-tampered", "title")).build());
        corruptCorePartLength(TAMPERED_LENGTH);

        var opens = new AtomicInteger();
        var held = new AtomicLong();
        var reads = reads(countingStack(opens, new AtomicInteger(), new AtomicLong()), tracking(held));

        var frames = new ArrayList<ReadDocumentResponse>();
        assertThatThrownBy(() -> reads.readDocument(processAuthority(), coreRead(address), frames::add))
                .isInstanceOfSatisfying(AccessionException.class, failure ->
                        assertThat(failure.code()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED));
        assertThat(opens.get()).as("the tampered length never reaches an open stream or a buffer").isZero();
        assertThat(held.get()).as("the budget is whole again after the refused read").isZero();
        assertThat(frames).noneMatch(ReadDocumentResponse::hasPartData);
        assertThat(frames).noneMatch(ReadDocumentResponse::hasComplete);
    }

    @Test void aRefusedUnaryReservationIsResourceExhaustedBeforeTheStackIsOpened() {
        var address = address(account, "doc-unary-exhausted");
        f.saves.save(processAuthority(), create("op-1", address,
                document(account, "doc-unary-exhausted", "title")).build());

        var opens = new AtomicInteger();
        var reads = reads(countingStack(opens, new AtomicInteger(), new AtomicLong()), bytes -> {
            throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED,
                    "read reservation exhausted");
        });

        assertThatThrownBy(() -> reads.getDocument(processAuthority(), coreGet(address)))
                .isInstanceOfSatisfying(AccessionException.class, failure ->
                        assertThat(failure.code()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED));
        assertThat(opens.get()).as("a refused reservation opens nothing and buffers nothing").isZero();
    }

    @Test void getDocumentHoldsEveryPartReservationUntilTheResponseIsBuiltThenReleases() {
        var address = address(account, "doc-unary-release");
        f.saves.save(processAuthority(), create("op-1", address,
                document(account, "doc-unary-release", "title")).build());
        long partTotal = presentPartTotal(f.catalog.manifest(address, 1).orElseThrow());
        assertThat(partTotal).isGreaterThan(0);

        var held = new AtomicLong();
        var maxHeld = new AtomicLong();
        var reads = reads(f.stack, bytes -> {
            maxHeld.accumulateAndGet(held.addAndGet(bytes), Math::max);
            return () -> held.addAndGet(-bytes);
        });

        var response = reads.getDocument(processAuthority(),
                GetDocumentRequest.newBuilder().setAddress(address).build());

        assertThat(response.getDocument().getDocId()).isEqualTo("doc-unary-release");
        assertThat(maxHeld.get())
                .as("the merged response aliases every part's bytes, so all part leases are held together")
                .isGreaterThanOrEqualTo(partTotal);
        assertThat(held.get()).as("every reservation is released once the response is handed off").isZero();
    }

    @Test void bulkReadHoldsOnePartReservationAtATimeAndReleasesAfterEachPart() {
        var address = address(account, "doc-bulk-one-at-a-time");
        f.saves.save(processAuthority(), create("op-1", address,
                document(account, "doc-bulk-one-at-a-time", "title")).build());
        var stored = f.catalog.manifest(address, 1).orElseThrow();
        assertThat(stored.getPartsCount()).isGreaterThan(1);
        var lengths = new java.util.HashMap<PartSelector, Long>();
        for (var part : stored.getPartsList())
            lengths.put(part.getEntry().getSelector(), part.getRef().getLength());

        var held = new AtomicLong();
        var reads = reads(f.stack, tracking(held));

        // At frame time the part's decode reservations are already closed, so the bytes
        // held are exactly the emitting part's own lease, never earlier parts' leases.
        var frames = new ArrayList<ReadDocumentResponse>();
        var heldAtComplete = new AtomicLong(-1);
        reads.readDocument(processAuthority(),
                ReadDocumentRequest.newBuilder().setAddress(address).build(), frame -> {
                    frames.add(frame);
                    if (frame.hasPartData())
                        assertThat(held.get())
                                .as("one part reservation at a time, for the emitting part only")
                                .isEqualTo(lengths.get(frame.getPartData().getSelector()));
                    if (frame.hasComplete()) heldAtComplete.set(held.get());
                });

        assertThat(frames.getLast().hasComplete()).isTrue();
        assertThat(frames).anyMatch(ReadDocumentResponse::hasPartData);
        assertThat(heldAtComplete.get()).as("every part lease is released before completion").isZero();
        assertThat(held.get()).as("every reservation is released after the read").isZero();
    }

    @Test void concurrentReadsBeyondTheBudgetFailFastAndSucceedOnceBytesReturn() {
        var address = address(account, "doc-concurrent");
        f.saves.save(processAuthority(), create("op-1", address,
                document(account, "doc-concurrent", "title")).build());
        long coreLength = f.catalog.manifest(address, 1).orElseThrow().getPartsList().stream()
                .filter(part -> part.getEntry().getSelector().getPart() == DocumentPart.DOCUMENT_PART_CORE)
                .findFirst().orElseThrow().getRef().getLength();

        // The same fail-fast semantics as the host's ReadByteBudget: no queueing, no partial grants.
        // The headroom covers the retained-schema decode reservations a successful read also takes.
        long budget = 2 * coreLength + 8L * 1024 * 1024;
        var remaining = new AtomicLong(budget);
        DocumentAdmissionReservations reservations = bytes -> {
            long left = remaining.addAndGet(-bytes);
            if (left < 0) {
                remaining.addAndGet(bytes);
                throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED,
                        "Read buffer budget exhausted: " + bytes + " requested");
            }
            return () -> remaining.addAndGet(bytes);
        };
        var opens = new AtomicInteger();
        var reads = reads(countingStack(opens, new AtomicInteger(), new AtomicLong()), reservations);

        // A concurrent read holds all but half a part of the budget: this read must fail fast.
        var concurrent = reservations.reserve(budget - coreLength / 2);
        int opensBefore = opens.get();
        assertThatThrownBy(() -> reads.getDocument(processAuthority(), coreGet(address)))
                .isInstanceOfSatisfying(AccessionException.class, failure ->
                        assertThat(failure.code()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED));
        assertThat(opens.get()).as("the refused read opens nothing").isEqualTo(opensBefore);

        // Once the concurrent read returns its bytes, the same read succeeds.
        concurrent.close();
        var response = reads.getDocument(processAuthority(), coreGet(address));
        assertThat(response.getDocument().getDocId()).isEqualTo("doc-concurrent");
        assertThat(remaining.get()).as("the budget is whole again after the read").isEqualTo(budget);
    }

    // ------------------------------------------------------------------ helpers

    private DocumentReads reads(ai.protomolt.proto.repo.stack.Stack stack,
            DocumentAdmissionReservations reservations) {
        return new DocumentReads(f.catalog,
                new DocumentSaves.Drives("default", java.util.Map.of("default", stack)),
                limits(), reservations, Clock.systemUTC());
    }

    private static DocumentAdmissionReservations tracking(AtomicLong held) {
        return bytes -> {
            held.addAndGet(bytes);
            return () -> held.addAndGet(-bytes);
        };
    }

    private static long presentPartTotal(StoredManifest stored) {
        return stored.getPartsList().stream().mapToLong(part -> part.getRef().getLength()).sum();
    }

    private static GetDocumentRequest coreGet(ai.protomolt.proto.repo.v1.NodeAddress address) {
        return GetDocumentRequest.newBuilder().setAddress(address)
                .setSelection(PartSelection.newBuilder().addSelectors(
                        PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE)))
                .build();
    }

    private static ReadDocumentRequest coreRead(ai.protomolt.proto.repo.v1.NodeAddress address) {
        return ReadDocumentRequest.newBuilder().setAddress(address)
                .setSelection(PartSelection.newBuilder().addSelectors(
                        PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE)))
                .build();
    }

    /** Rewrites revision 1's stored manifest with a corrupt oversized CORE part length. */
    private void corruptCorePartLength(long length) throws Exception {
        try (var connection = DriverManager.getConnection(f.url,
                AccessionFixture.POSTGRES.getUsername(), AccessionFixture.POSTGRES.getPassword())) {
            byte[] bytes;
            try (var select = connection.prepareStatement("SELECT manifest FROM catalog_revisions WHERE revision = 1");
                 var rows = select.executeQuery()) {
                assertThat(rows.next()).isTrue();
                bytes = rows.getBytes(1);
            }
            var stored = StoredManifest.parseFrom(bytes).toBuilder();
            for (var part : stored.getPartsBuilderList()) {
                if (part.getEntry().getSelector().getPart() == DocumentPart.DOCUMENT_PART_CORE) {
                    part.getRefBuilder().setLength(length);
                    part.getEntryBuilder().setSizeBytes(length);
                }
            }
            var corrupt = stored.build().toByteString();
            connection.createStatement().execute(
                    "ALTER TABLE catalog_revisions DISABLE TRIGGER catalog_revisions_immutable");
            try (var update = connection.prepareStatement(
                    "UPDATE catalog_revisions SET manifest = ?, manifest_sha256 = ? WHERE revision = 1")) {
                update.setBytes(1, corrupt.toByteArray());
                update.setString(2, DocumentSaves.sha256(corrupt));
                assertThat(update.executeUpdate()).isEqualTo(1);
            } finally {
                connection.createStatement().execute(
                        "ALTER TABLE catalog_revisions ENABLE TRIGGER catalog_revisions_immutable");
            }
        }
    }

    private ai.protomolt.proto.repo.stack.Stack countingStack(AtomicInteger opens, AtomicInteger closes,
            AtomicLong bytesRead) {
        return new CountingReadStack(f.stack, bytesRead, opens, closes);
    }

    /** Forwards to the fixture stack, counting opened streams, closed streams, and bytes read. */
    private static final class CountingReadStack implements ai.protomolt.proto.repo.stack.Stack {
        private final ai.protomolt.proto.repo.stack.Stack delegate;
        private final AtomicLong bytesRead;
        private final AtomicInteger opens;
        private final AtomicInteger closes;

        CountingReadStack(ai.protomolt.proto.repo.stack.Stack delegate, AtomicLong bytesRead,
                AtomicInteger opens, AtomicInteger closes) {
            this.delegate = delegate;
            this.bytesRead = bytesRead;
            this.opens = opens;
            this.closes = closes;
        }

        @Override public String stackId() { return delegate.stackId(); }
        @Override public StackRef put(InputStream body, long expectedLength) throws IOException {
            return delegate.put(body, expectedLength);
        }
        @Override public StackRef put(byte[] body) throws IOException { return delegate.put(body); }
        @Override public InputStream open(StackRef ref) throws IOException {
            var stream = delegate.open(ref);
            opens.incrementAndGet();
            return new FilterInputStream(stream) {
                @Override public int read() throws IOException {
                    int b = super.read();
                    if (b >= 0) bytesRead.incrementAndGet();
                    return b;
                }
                @Override public int read(byte[] b, int off, int len) throws IOException {
                    int n = super.read(b, off, len);
                    if (n > 0) bytesRead.addAndGet(n);
                    return n;
                }
                @Override public void close() throws IOException {
                    closes.incrementAndGet();
                    super.close();
                }
            };
        }
        @Override public InputStream openRange(StackRef ref, long offset, long length) throws IOException {
            return delegate.openRange(ref, offset, length);
        }
        @Override public long head(StackRef ref) throws IOException { return delegate.head(ref); }
        @Override public void delete(StackRef ref) throws IOException { delegate.delete(ref); }
        @Override public void close() throws IOException { delegate.close(); }
    }
}
