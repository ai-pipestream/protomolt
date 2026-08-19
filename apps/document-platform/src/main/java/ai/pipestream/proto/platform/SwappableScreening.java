package ai.pipestream.proto.platform;

import ai.pipestream.proto.screening.Screener;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * A screening mount handed to the door before the config lane exists: the
 * door is wired inside the composer boot, while the mount arrives as a
 * config document. Until the mount swaps in, the supplier yields null and
 * the door refuses indexing fail-closed rather than indexing unscreened —
 * exactly the stance the taxonomy gate holds while its data is not yet
 * mounted.
 */
final class SwappableScreening implements Supplier<Screener> {

    private final AtomicReference<Screener> delegate = new AtomicReference<>();

    @Override
    public Screener get() {
        return delegate.get();
    }

    /** Swaps the live mount; the next indexed document sees it. */
    void swap(Screener screener) {
        if (screener == null) {
            throw new IllegalArgumentException("screener must not be null");
        }
        delegate.set(screener);
    }
}
