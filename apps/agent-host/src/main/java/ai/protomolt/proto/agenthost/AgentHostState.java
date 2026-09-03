package ai.protomolt.proto.agenthost;

import java.util.List;

/** Durable cursor, provider identity, and partially executed command batch. */
record AgentHostState(String identity, AgentRole role, String provider, String workspace,
                      long cursor, String providerSessionId, boolean bootstrapped,
                      PendingTurn pending) {

    record PendingTurn(long targetCursor, List<Long> eventCursors,
                       List<AgentTurn.Command> commands, int nextCommand,
                       boolean bootstrap) {
        PendingTurn {
            eventCursors = List.copyOf(eventCursors);
            commands = List.copyOf(commands);
            if (targetCursor < 0 || nextCommand < 0 || nextCommand > commands.size()) {
                throw new IllegalArgumentException("invalid pending turn position");
            }
        }

        PendingTurn advanced() {
            return new PendingTurn(targetCursor, eventCursors, commands, nextCommand + 1,
                    bootstrap);
        }
    }

    AgentHostState {
        if (identity == null || identity.isBlank() || role == null
                || provider == null || provider.isBlank() || workspace == null
                || workspace.isBlank() || cursor < 0) {
            throw new IllegalArgumentException("invalid agent host state");
        }
        providerSessionId = providerSessionId == null ? "" : providerSessionId;
    }

    static AgentHostState initial(String identity, AgentRole role, String provider,
                                  String workspace) {
        return new AgentHostState(identity, role, provider, workspace, 0, "", false, null);
    }

    AgentHostState withProviderSession(String sessionId) {
        return new AgentHostState(identity, role, provider, workspace, cursor, sessionId,
                bootstrapped, pending);
    }

    AgentHostState withPending(PendingTurn turn) {
        return new AgentHostState(identity, role, provider, workspace, cursor,
                providerSessionId, bootstrapped, turn);
    }

    AgentHostState commandAdvanced() {
        return withPending(pending.advanced());
    }

    AgentHostState completePending() {
        return new AgentHostState(identity, role, provider, workspace,
                pending.targetCursor(), providerSessionId,
                bootstrapped || pending.bootstrap(), null);
    }

    AgentHostState withCursor(long nextCursor) {
        return new AgentHostState(identity, role, provider, workspace, nextCursor,
                providerSessionId, bootstrapped, pending);
    }

    /**
     * Drops everything that indexes into a coordinator transcript: the cursor, any partly
     * executed batch, and the record of having bootstrapped. The provider session is kept,
     * because it belongs to the agent rather than to the coordinator that forgot it.
     */
    AgentHostState withoutTranscriptPosition() {
        return new AgentHostState(identity, role, provider, workspace, 0,
                providerSessionId, false, null);
    }
}
