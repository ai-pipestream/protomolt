package ai.protomolt.proto.actions;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

/** A response gate before transport emission; the first failure remains terminal. */
final class ContractStreamEmitter implements StreamEmitter {
    private final ReentrantLock lock = new ReentrantLock();
    private final StreamEmitter downstream;
    private final Descriptor descriptor;
    private final String action;
    private ActionException failure;
    private RuntimeException uncheckedFailure;
    private boolean finished;

    ContractStreamEmitter(StreamEmitter downstream, Descriptor descriptor, String action) {
        this.downstream = Objects.requireNonNull(downstream, "emitter");
        this.descriptor = descriptor;
        this.action = action;
    }

    @Override
    public void emit(Message message) throws ActionException {
        lock.lock();
        try {
            checkFailureLocked();
            if (finished) {
                throw new ActionException("invalid-response",
                        action + " emitted after stream completion");
            }
            try {
                downstream.emit(CatalogContract.checkedResponse(message, descriptor, action));
            } catch (ActionException e) {
                failure = e;
                throw e;
            } catch (RuntimeException e) {
                uncheckedFailure = e;
                throw e;
            }
        } finally {
            lock.unlock();
        }
    }

    void checkFailure() throws ActionException {
        lock.lock();
        try {
            checkFailureLocked();
        } finally {
            lock.unlock();
        }
    }

    private void checkFailureLocked() throws ActionException {
        if (failure != null) throw failure;
        if (uncheckedFailure != null) throw uncheckedFailure;
    }

    void complete() throws ActionException {
        lock.lock();
        try {
            finished = true;
            checkFailureLocked();
        } finally {
            lock.unlock();
        }
    }

    void finish() {
        lock.lock();
        try {
            finished = true;
        } finally {
            lock.unlock();
        }
    }
}
