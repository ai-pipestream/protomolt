package ai.protomolt.proto.delegation;

import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.delegation.v1.DeliverableContract;
import ai.protomolt.proto.delegation.v1.ObservedEvent;
import ai.protomolt.proto.delegation.v1.ReadTranscriptResponse;
import ai.protomolt.proto.delegation.v1.WatchEventsResponse;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Renders each recorded candidate with the contract offered for its own attempt. */
final class DelegationEventJson {
    private record Attempt(String taskId, int number) { }

    private DelegationEventJson() { }

    static ObjectNode render(Message reply, String verb,
            InProcessDelegationCoordinator coordinator) throws ActionException {
        List<ObservedEvent> events;
        Message header;
        if (reply instanceof WatchEventsResponse watch) {
            events = watch.getEventsList();
            header = watch.toBuilder().clearEvents().build();
        } else if (reply instanceof ReadTranscriptResponse transcript) {
            events = transcript.getEventsList();
            header = transcript.toBuilder().clearEvents().build();
        } else {
            throw new ActionException("internal-error", verb + " returned an unexpected reply");
        }
        JsonFormat.TypeRegistry empty = JsonFormat.TypeRegistry.getEmptyTypeRegistry();
        ObjectNode json = CatalogContract.toReply(header, verb, empty);
        ArrayNode rendered = json.putArray("events");
        boolean hasTypedCandidate = events.stream().anyMatch(event ->
                event.getEntry().hasWorkerFrame()
                        && event.getEntry().getWorkerFrame().hasCompletion()
                        && event.getEntry().getWorkerFrame().getCompletion().hasResult());
        Map<Attempt, DeliverableContract> contracts = hasTypedCandidate
                ? historicalOffers(coordinator) : Map.of();
        for (ObservedEvent event : events) {
            JsonFormat.TypeRegistry registry = empty;
            if (event.getEntry().hasWorkerFrame()
                    && event.getEntry().getWorkerFrame().hasCompletion()
                    && event.getEntry().getWorkerFrame().getCompletion().hasResult()) {
                int attempt = event.getEntry().getWorkerFrame().getCompletion().getAttempt();
                DeliverableContract contract = contracts.get(
                        new Attempt(event.getTaskId(), attempt));
                if (contract == null) {
                    throw new ActionException("internal-error", verb
                            + " cannot find the recorded offer for candidate at cursor "
                            + event.getCursor());
                }
                try {
                    registry = DeliverableContracts.typeRegistry(contract);
                } catch (IllegalArgumentException invalid) {
                    throw new ActionException("internal-error", verb
                            + " cannot link the recorded offer at cursor "
                            + event.getCursor() + ": " + invalid.getMessage());
                }
            }
            rendered.add(CatalogContract.toReply(event, verb, registry));
        }
        return json;
    }

    private static Map<Attempt, DeliverableContract> historicalOffers(
            InProcessDelegationCoordinator coordinator) {
        Map<Attempt, DeliverableContract> contracts = new HashMap<>();
        for (InProcessDelegationCoordinator.Event event : coordinator.eventsAfter(null, 0)) {
            if (!event.entry().hasCoordinatorFrame()
                    || !event.entry().getCoordinatorFrame().hasOffer()) {
                continue;
            }
            var offer = event.entry().getCoordinatorFrame().getOffer();
            if (offer.getSpec().hasContract()) {
                contracts.put(new Attempt(event.taskId(), offer.getAttempt()),
                        offer.getSpec().getContract());
            }
        }
        return contracts;
    }
}
