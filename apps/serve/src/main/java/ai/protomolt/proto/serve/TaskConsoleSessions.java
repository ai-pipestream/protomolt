package ai.protomolt.proto.serve;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.authz.CallerResolver;
import ai.protomolt.proto.authz.ConsoleSessions;

import java.time.Duration;
import java.util.Set;

/** The task console's session settings over the shared console-session mechanism. */
final class TaskConsoleSessions {

    static final String COOKIE = "__Host-protomolt_task_session";

    /** The console login token's identity: task steering, never the operator. */
    static final Caller CONSOLE = Caller.scoped("task-console",
            Set.of(Scopes.WORKER_COORDINATE));

    private TaskConsoleSessions() {
    }

    static ConsoleSessions open() {
        return ConsoleSessions.open(COOKIE);
    }

    /**
     * With a resolver, a credential the access policy names also logs in, and the session
     * is bound to that principal for its whole lifetime; the console login token binds to
     * {@link #CONSOLE}, never to the operator.
     */
    static ConsoleSessions secured(String loginToken, Duration ttl,
                                   CallerResolver resolver) {
        return ConsoleSessions.secured(COOKIE, ttl, resolver, loginToken, CONSOLE);
    }
}
