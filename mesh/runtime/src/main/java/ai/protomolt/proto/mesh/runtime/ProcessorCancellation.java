package ai.protomolt.proto.mesh.runtime;

import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Cooperative cancellation signal made available to local and remote processors. */
public final class ProcessorCancellation {

    private static final ProcessorCancellation NONE = new ProcessorCancellation(false);

    private final boolean mutable;
    private final AtomicBoolean requested = new AtomicBoolean();
    private final AtomicReference<String> reason = new AtomicReference<>("");

    public ProcessorCancellation() {
        this(true);
    }

    private ProcessorCancellation(boolean mutable) {
        this.mutable = mutable;
    }

    public static ProcessorCancellation none() {
        return NONE;
    }

    public boolean request(String reason) {
        if (!mutable) {
            return false;
        }
        String bounded = Objects.requireNonNull(reason, "reason");
        if (bounded.isBlank() || bounded.length() > 2_048) {
            throw new IllegalArgumentException(
                    "cancellation reason must contain 1 to 2048 characters");
        }
        this.reason.compareAndSet("", bounded);
        return requested.compareAndSet(false, true);
    }

    public boolean requested() {
        return requested.get();
    }

    public String reason() {
        return reason.get();
    }

    public void throwIfRequested() {
        if (requested()) {
            throw new CancellationException(reason());
        }
    }
}
