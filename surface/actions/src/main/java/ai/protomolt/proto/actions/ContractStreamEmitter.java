package ai.protomolt.proto.actions;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import java.util.Objects;

/** A response gate before transport emission; the first failure remains terminal. */
final class ContractStreamEmitter implements StreamEmitter {
    private final StreamEmitter downstream;
    private final Descriptor descriptor;
    private final String action;
    private ActionException failure;
    private boolean finished;

    ContractStreamEmitter(StreamEmitter downstream, Descriptor descriptor, String action) {
        this.downstream = Objects.requireNonNull(downstream, "emitter");
        this.descriptor = descriptor;
        this.action = action;
    }

    @Override
    public synchronized void emit(Message message) throws ActionException {
        checkFailure();
        if (finished) throw new ActionException("invalid-response", action + " emitted after stream completion");
        try {
            downstream.emit(CatalogContract.checkedResponse(message, descriptor, action));
        } catch (ActionException e) {
            failure = e;
            throw e;
        }
    }

    synchronized void checkFailure() throws ActionException {
        if (failure != null) throw failure;
    }

    synchronized void complete() throws ActionException {
        finished = true;
        checkFailure();
    }

    synchronized void finish() { finished = true; }
}
