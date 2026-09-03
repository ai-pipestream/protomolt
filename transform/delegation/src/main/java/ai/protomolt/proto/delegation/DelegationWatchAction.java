package ai.protomolt.proto.delegation;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.delegation.v1.WatchEventsRequest;
import ai.protomolt.proto.delegation.v1.WatchEventsResponse;
import com.google.protobuf.Message;

import com.google.protobuf.Descriptors.Descriptor;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * Long-polls the coordinator's event feed from a caller-owned cursor. The call blocks
 * (safely on a virtual thread) until at least one event appears after the cursor or the
 * timeout elapses, then returns a bounded batch and the resumption cursor. Reconnecting
 * MCP sessions resume from their last cursor with no lost or duplicated frames.
 */
final class DelegationWatchAction extends DelegationAction {

    /** The wait a request that does not choose one gets. */
    static final int DEFAULT_TIMEOUT_MS = 1_000;

    /** The batch size a request that does not choose one gets. */
    static final int DEFAULT_EVENTS_PER_CALL = 64;

    DelegationWatchAction(DelegationBridge bridge) {
        super(bridge);
    }

    @Override
    public String name() {
        return "delegation-watch";
    }

    @Override
    public String description() {
        return "Long-polls the delegation event feed: blocks up to timeoutMs until an event "
                + "appears after 'afterCursor', then returns a bounded batch with the "
                + "resumption cursor. Every frame (offers, progress, checkpoints, messages, "
                + "review verdicts) is one cursor-addressable event; reconnecting sessions "
                + "resume from their last cursor with no lost or duplicated frames.";
    }

    @Override
    public Descriptor requestType() {
        return WatchEventsRequest.getDescriptor();
    }

    @Override
    public Descriptor responseType() {
        return WatchEventsResponse.getDescriptor();
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        WatchEventsRequest request = CatalogContract.as(
                input, WatchEventsRequest.getDefaultInstance(), name());
        // The timeout tracks presence on the wire, because 0 is a legal request (poll and
        // return at once) and must not be read as "the caller said nothing".
        int timeoutMs = request.hasTimeoutMs() ? request.getTimeoutMs() : DEFAULT_TIMEOUT_MS;
        // The batch size has no such collision: 0 is not a legal batch, so it means omitted.
        int maxEvents = request.getMaxEvents() == 0
                ? DEFAULT_EVENTS_PER_CALL
                : request.getMaxEvents();
        // An omitted task id arrives as the empty string, which watches every task.
        String taskId = request.getTaskId().isEmpty() ? null : request.getTaskId();
        long afterCursor = request.getAfterCursor();
        List<InProcessDelegationCoordinator.Event> events;
        try {
            Optional<InProcessDelegationCoordinator.Event> first = bridge.coordinator()
                    .waitForEvent(taskId, afterCursor, Duration.ofMillis(timeoutMs));
            if (first.isEmpty()) {
                return WatchEventsResponse.newBuilder()
                        .setOk(true)
                        .setCursor(afterCursor)
                        .setTimedOut(true)
                        .build();
            }
            events = bridge.coordinator().eventsAfter(taskId, afterCursor);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ActionException("interrupted", "delegation-watch was interrupted");
        } catch (RuntimeException e) {
            throw failure(e);
        }
        boolean truncated = events.size() > maxEvents;
        if (truncated) {
            events = events.subList(0, maxEvents);
        }
        WatchEventsResponse.Builder response = WatchEventsResponse.newBuilder()
                .setOk(true)
                .setCursor(resumeCursor(events))
                .setTruncated(truncated)
                .setTimedOut(false);
        events.forEach(event -> response.addEvents(observed(event)));
        return response.build();
    }
}
