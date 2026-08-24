package ai.pipestream.proto.delegation;

import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.JsonAction;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.delegation.v1.ObservedEvent;

import java.util.List;
import java.util.Objects;

/** Shared base for the delegation catalog actions: the bridge plus event rendering. */
abstract class DelegationAction implements JsonAction {

    final DelegationBridge bridge;

    DelegationAction(DelegationBridge bridge) {
        this.bridge = Objects.requireNonNull(bridge, "bridge");
    }

    @Override
    public String requiredScope() {
        return Scopes.WORKER_COORDINATE;
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
        return events.isEmpty() ? 0 : events.get(events.size() - 1).cursor();
    }

    /** Maps bridge and coordinator failures onto the stable delegation error codes. */
    static ActionException failure(String workerId, RuntimeException e) {
        String message = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
        if (message.startsWith("worker is not registered")) {
            return DelegationActionJson.unknownWorker(workerId);
        }
        if (message.startsWith("worker stream")) {
            return DelegationActionJson.streamFailed(workerId, message);
        }
        return DelegationActionJson.rejected(message);
    }

    /** Maps coordinator-side failures onto the stable delegation error codes. */
    static ActionException failure(RuntimeException e) {
        return failure("", e);
    }
}
