package ai.protomolt.proto.repo.admission;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import java.util.ArrayList;
import java.util.Objects;

/** One preparation/result owner; never shared between concurrent checks. */
final class DocumentAdmissionResources implements DocumentAdmissionReservations, AutoCloseable {
    /** Keep host resource failures distinct from invalid retained schema bytes. */
    static final class ReservationFailure extends RuntimeException {
        final RuntimeException original;
        ReservationFailure(RuntimeException original) { super(original); this.original = original; }
    }

    private final DocumentAdmissionReservations reservations;
    private final ArrayList<Lease> retained = new ArrayList<>();
    private boolean closed;

    DocumentAdmissionResources(DocumentAdmissionReservations reservations) {
        this.reservations = Objects.requireNonNull(reservations);
    }

    @Override public Lease reserve(long bytes) {
        requireOpen();
        try {
            return Objects.requireNonNull(reservations.reserve(bytes), "reservation lease");
        } catch (ReservationFailure failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw new ReservationFailure(failure);
        }
    }

    /** Transfer an encoded owner only after the caller has accepted its identity and bounds. */
    DocumentSchemaEvidenceCodec.Encoded retain(DocumentSchemaEvidenceCodec.OwnedEncoded owner) {
        try {
            requireOpen();
            var value = owner.value();
            retained.add(owner::close);
            return value;
        } catch (RuntimeException | Error failure) {
            owner.close();
            throw failure;
        }
    }

    /** Input bytes are borrowed and stable during this copy; only the private copy escapes. */
    ByteString copy(ByteString input, Runnable control) {
        var lease = reserve(input.size());
        boolean transferred = false;
        try {
            control.run();
            var value = UnsafeByteOperations.unsafeWrap(input.toByteArray());
            control.run();
            retained.add(lease);
            transferred = true;
            return value;
        } finally {
            if (!transferred) lease.close();
        }
    }

    private void requireOpen() {
        if (closed) throw new IllegalStateException("Admission resources are closed");
    }

    @Override public void close() {
        if (closed) return;
        closed = true;
        // Lease.close is nonthrowing by contract. Reverse order mirrors acquisition.
        for (int i = retained.size() - 1; i >= 0; i--) retained.get(i).close();
        retained.clear();
    }
}
