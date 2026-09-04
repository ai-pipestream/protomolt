package ai.protomolt.proto.delegation;

import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.delegation.v1.ObservedEvent;

import java.util.List;
import java.util.Objects;

/** Shared base for the delegation catalog actions: the bridge plus event rendering. */
abstract class DelegationAction implements ProtoAction {

    final DelegationBridge bridge;

    DelegationAction(DelegationBridge bridge) {
        this.bridge = Objects.requireNonNull(bridge, "bridge");
    }

    @Override
    public String requiredScope() {
        return Scopes.WORKER_COORDINATE;
    }

    /**
     * The deliverable types the coordinator's live tasks declare.
     *
     * <p>A candidate's {@code result} is an {@code Any} whose type the offer named, so it is
     * not on this process's classpath and the default registry resolves nothing. Every
     * delegation verb publishes the coordinator's own types: the candidate verb needs them to
     * parse a submitted deliverable, and the watch and transcript verbs need them to print a
     * recorded one back.
     */
    @Override
    public com.google.protobuf.util.JsonFormat.TypeRegistry typeRegistry() {
        return bridge.deliverableTypes();
    }

    /** One cursor-addressable transcript event as its declared contract message. */
    static ObservedEvent observed(InProcessDelegationCoordinator.Event event) {
        return ObservedEvent.newBuilder()
                .setCursor(event.cursor())
                .setWorkerId(event.workerId())
                .setTaskId(event.taskId())
                .setLane(event.entry().getLane())
                .setEntry(event.entry())
                .build();
    }

    /** The resumption cursor for a returned run: the last event's, or 0 when empty. */
    static long resumeCursor(List<InProcessDelegationCoordinator.Event> events) {
        return events.isEmpty() ? 0 : events.getLast().cursor();
    }

    /** Maps bridge and coordinator failures onto the stable delegation error codes. */
    static ActionException failure(String workerId, RuntimeException e) {
        String message = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
        if (message.startsWith("worker is not registered")) {
            return DelegationFailures.unknownWorker(workerId);
        }
        if (message.startsWith("worker stream")) {
            return DelegationFailures.streamFailed(workerId, message);
        }
        return DelegationFailures.rejected(message);
    }

    /** Maps coordinator-side failures onto the stable delegation error codes. */
    static ActionException failure(RuntimeException e) {
        return failure("", e);
    }
}
