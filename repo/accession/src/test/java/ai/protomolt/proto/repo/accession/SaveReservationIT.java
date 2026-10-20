package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.admission.DocumentAdmissionReservations;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.stack.Stack;
import ai.protomolt.proto.repo.stack.StackDataLoss;
import ai.protomolt.proto.repo.v2.StackRef;
import io.grpc.Status;
import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Save-side reservation and streaming-put coverage: unary saves reserve
 * serialized bytes for the duration of each stack write, an over-budget save
 * is RESOURCE_EXHAUSTED like the read path, leases release on completion and
 * on failure, and the write streams through the stack with its digest
 * verified against the bytes the save hashed.
 */
class SaveReservationIT {
    private AccessionFixture f;
    private String account;

    @BeforeEach void open() throws Exception {
        f = new AccessionFixture();
        account = "acct-" + UUID.randomUUID();
    }

    @AfterEach void close() { f.close(); }

    /** Fail-fast budget with lease accounting, shaped like the host's ReadByteBudget. */
    private static final class TrackingBudget implements DocumentAdmissionReservations {
        final long budget;
        final AtomicLong outstanding = new AtomicLong();
        final AtomicLong peak = new AtomicLong();

        TrackingBudget(long budget) { this.budget = budget; }

        @Override public Lease reserve(long bytes) {
            long now = outstanding.addAndGet(bytes);
            if (now > budget) {
                outstanding.addAndGet(-bytes);
                throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED,
                        "Save buffer budget exhausted: " + bytes + " bytes requested");
            }
            peak.accumulateAndGet(now, Math::max);
            var released = new AtomicBoolean();
            return () -> {
                if (released.compareAndSet(false, true)) outstanding.addAndGet(-bytes);
            };
        }
    }

    @Test void anOverBudgetSaveIsResourceExhaustedBeforeAnyStackWrite() {
        var budget = new TrackingBudget(8);
        var limited = new DocumentSaves(f.catalog, f.drives, resolver(), limits(), budget, Clock.systemUTC());
        var address = address(account, "doc-over");
        var request = create("op-over", address, document(account, "doc-over", "t")).build();
        int puts = f.stack.puts.get();

        assertThatThrownBy(() -> limited.save(processAuthority(), request))
                .isInstanceOfSatisfying(AccessionException.class,
                        failure -> assertThat(failure.code()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED));
        assertThat(f.stack.puts.get()).as("no stack write happened").isEqualTo(puts);
        assertThat(budget.outstanding.get()).as("the refused reservation holds nothing").isZero();
        assertThat(f.catalog.outcome(account, "op-over")).as("the operation ID is still usable").isEmpty();
    }

    @Test void aSuccessfulSaveReservesDuringItsWritesAndReleasesEverything() {
        var budget = new TrackingBudget(Long.MAX_VALUE);
        var saves = new DocumentSaves(f.catalog, f.drives, resolver(), limits(), budget, Clock.systemUTC());
        var address = address(account, "doc-ok");
        var request = create("op-ok", address, document(account, "doc-ok", "t")).build();

        assertThat(saves.save(processAuthority(), request).getAccession().getRevision()).isEqualTo(1);
        assertThat(budget.peak.get()).as("the writes reserved real bytes").isPositive();
        assertThat(budget.outstanding.get()).as("every lease is released").isZero();
    }

    @Test void aFailedStackWriteReleasesItsLease() {
        var budget = new TrackingBudget(Long.MAX_VALUE);
        var saves = new DocumentSaves(f.catalog, f.drives, resolver(), limits(), budget, Clock.systemUTC());
        f.stack.afterPut = () -> { throw new IOException("backing store down"); };
        var address = address(account, "doc-fail");
        var request = create("op-fail", address, document(account, "doc-fail", "t")).build();

        assertThatThrownBy(() -> saves.save(processAuthority(), request))
                .isInstanceOfSatisfying(AccessionException.class,
                        failure -> assertThat(failure.code()).isEqualTo(Status.Code.UNAVAILABLE));
        assertThat(budget.outstanding.get()).as("the failed write's lease is released").isZero();
        f.stack.reset();
    }

    @Test void aFragmentPastThePartBufferStreamsAsMultipartAndReadsBackVerified() throws Exception {
        // 9 MiB of title pushes the serialized core fragment past the stack's
        // 8 MiB part buffer, so the put takes the multipart streaming path.
        var budget = new TrackingBudget(Long.MAX_VALUE);
        var saves = new DocumentSaves(f.catalog, f.drives, resolver(), limits(), budget, Clock.systemUTC());
        String big = "x".repeat(9 * 1024 * 1024);
        var address = address(account, "doc-big");
        var request = create("op-big", address, document(account, "doc-big", big)).build();

        assertThat(saves.save(processAuthority(), request).getAccession().getRevision()).isEqualTo(1);
        assertThat(budget.peak.get()).isGreaterThan(9L * 1024 * 1024);
        assertThat(budget.outstanding.get()).isZero();

        var stored = f.catalog.manifest(address, 1).orElseThrow();
        var core = stored.getPartsList().stream()
                .filter(part -> part.hasRef() && part.getRef().getLength() > 9L * 1024 * 1024)
                .findFirst().orElseThrow();
        // The verifying read checks length and SHA-256 over the whole object.
        try (var in = f.stack.open(core.getRef())) {
            long read = 0;
            byte[] buffer = new byte[64 * 1024];
            int r;
            while ((r = in.read(buffer)) >= 0) read += r;
            assertThat(read).isEqualTo(core.getRef().getLength());
        }
    }

    @Test void aStackThatCorruptsTheStreamFailsTheDigestCheck() {
        var corrupting = new CorruptingStack(f.stack);
        var drives = new DocumentSaves.Drives("default", Map.of("default", corrupting));
        var budget = new TrackingBudget(Long.MAX_VALUE);
        var saves = new DocumentSaves(f.catalog, drives, resolver(), limits(), budget, Clock.systemUTC());
        var address = address(account, "doc-corrupt");
        var request = create("op-corrupt", address, document(account, "doc-corrupt", "t")).build();

        assertThatThrownBy(() -> saves.save(processAuthority(), request))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Stack digest differs");
        assertThat(budget.outstanding.get()).as("the lease is released before the digest check").isZero();
    }

    /** Flips the first streamed byte, so the stack's computed digest cannot match. */
    private static final class CorruptingStack implements Stack {
        private final Stack delegate;

        CorruptingStack(Stack delegate) { this.delegate = delegate; }

        @Override public String stackId() { return delegate.stackId(); }

        @Override public StackRef put(InputStream body, long expectedLength) throws IOException {
            byte[] bytes = body.readAllBytes();
            if (bytes.length > 0) bytes[0] ^= 0x1;
            return delegate.put(bytes);
        }

        @Override public StackRef put(byte[] body) throws IOException {
            byte[] corrupted = body.clone();
            if (corrupted.length > 0) corrupted[0] ^= 0x1;
            return delegate.put(corrupted);
        }

        @Override public InputStream open(StackRef ref) throws IOException { return delegate.open(ref); }

        @Override public InputStream openRange(StackRef ref, long offset, long length) throws IOException {
            return delegate.openRange(ref, offset, length);
        }

        @Override public long head(StackRef ref) throws IOException { return delegate.head(ref); }

        @Override public void delete(StackRef ref) throws IOException { delegate.delete(ref); }
    }

    /** The verifying read fails loudly when a reference does not match its bytes. */
    @Test void theVerifyingReadStillRefusesAMismatchedObject() throws Exception {
        // Baseline sanity for the corruption harness: writing corrupted bytes
        // under an honest reference is impossible through DocumentSaves, so
        // prove the stack-side verification instead with a direct read check.
        var address = address(account, "doc-verify");
        var request = create("op-verify", address, document(account, "doc-verify", "t")).build();
        assertThat(f.saves.save(processAuthority(), request).getAccession().getRevision()).isEqualTo(1);
        var stored = f.catalog.manifest(address, 1).orElseThrow();
        var part = stored.getPartsList().stream().filter(p -> p.hasRef()).findFirst().orElseThrow();
        var lying = part.getRef().toBuilder().setSha256("0".repeat(64)).build();
        assertThatThrownBy(() -> {
            try (var in = f.stack.open(lying)) {
                in.readAllBytes();
            }
        }).isInstanceOf(StackDataLoss.class);
    }
}
