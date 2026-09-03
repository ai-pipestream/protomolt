package ai.protomolt.proto.delegation;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.delegation.v1.ReadTranscriptRequest;
import ai.protomolt.proto.delegation.v1.ReadTranscriptResponse;
import com.google.protobuf.Message;

import com.google.protobuf.Descriptors.Descriptor;
import java.util.List;

/** Reads a bounded slice of the recorded delegation transcript from a cursor. */
final class DelegationTranscriptAction extends DelegationAction {

    /** The slice size a request that does not choose one gets. */
    static final int DEFAULT_ENTRIES_PER_CALL = 100;

    DelegationTranscriptAction(DelegationBridge bridge) {
        super(bridge);
    }

    @Override
    public String name() {
        return "delegation-transcript";
    }

    @Override
    public String description() {
        return "Reads the recorded delegation transcript from a cursor: every accepted frame "
                + "in recorded order, optionally restricted to one task. Bounded per call; "
                + "follow the returned cursor while 'truncated' is true.";
    }

    @Override
    public Descriptor requestType() {
        return ReadTranscriptRequest.getDescriptor();
    }

    @Override
    public Descriptor responseType() {
        return ReadTranscriptResponse.getDescriptor();
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        ReadTranscriptRequest request = CatalogContract.as(
                input, ReadTranscriptRequest.getDefaultInstance(), name());
        // An omitted slice size arrives as 0, which the request's ignore_if_zero rule lets
        // through so this default can apply; 0 itself is not a legal slice.
        int maxEntries = request.getMaxEntries() == 0
                ? DEFAULT_ENTRIES_PER_CALL
                : request.getMaxEntries();
        // An omitted task id arrives as the empty string, which reads every task.
        String taskId = request.getTaskId().isEmpty() ? null : request.getTaskId();
        List<InProcessDelegationCoordinator.Event> events;
        try {
            events = bridge.coordinator().eventsAfter(taskId, request.getAfterCursor());
        } catch (RuntimeException e) {
            throw failure(e);
        }
        boolean truncated = events.size() > maxEntries;
        if (truncated) {
            events = events.subList(0, maxEntries);
        }
        ReadTranscriptResponse.Builder response = ReadTranscriptResponse.newBuilder()
                .setOk(true)
                .setCursor(resumeCursor(events))
                .setTruncated(truncated);
        events.forEach(event -> response.addEvents(observed(event)));
        return response.build();
    }
}
