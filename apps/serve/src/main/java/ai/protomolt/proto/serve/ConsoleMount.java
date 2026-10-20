package ai.protomolt.proto.serve;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.authz.CallerResolver;
import ai.protomolt.proto.authz.ConsoleSessions;
import ai.protomolt.proto.delegation.DelegationBridge;
import ai.protomolt.proto.server.jdk.JdkProtoRestServer;
import ai.protomolt.proto.workflow.RecordSigning;
import java.util.function.IntSupplier;

/**
 * The browser surfaces of the serve wiring: the console UI, the task-session login and the
 * bounded task API behind it, the correction console, and the workflow-launch console, plus
 * the console's same-origin proxies to the registry and back onto this server.
 *
 * <p>On an open node (no API token) everything mounts, with the session store open or secured
 * by the console login token. On a token-guarded node the surfaces are refused with a disabled
 * notice unless a console login token is configured, in which case they mount behind scoped
 * browser sessions and only the proxies stay disabled.
 */
final class ConsoleMount {

    private ConsoleMount() {
    }

    /**
     * Mounts the console family onto the HTTP server before it starts. {@code registryPort}
     * and {@code servePort} are read lazily by the proxies because the serve port is only known
     * after the HTTP server starts.
     */
    static void mount(JdkProtoRestServer http, String apiToken,
                      ProtoMoltServe.TaskConsoleOptions taskConsole, CallerResolver callers,
                      DelegationBridge bridge, CorrectionStarter starter, ActionCatalog catalog,
                      WorkflowAuthoringMount.Prepared authoring,
                      IntSupplier registryPort, IntSupplier servePort) {
        ConsoleSessions taskSessions = taskConsole == null
                ? TaskConsoleSessions.open()
                : TaskConsoleSessions.secured(taskConsole.loginToken(), taskConsole.sessionTtl(),
                        callers);
        if (apiToken == null || taskConsole != null) {
            http.withContext("/console", new ConsoleHandler())
                    .withContext("/api/task-session",
                            new TaskConsoleSessionHandler(taskSessions))
                    .withContext("/api/tasks",
                            new TaskConsoleApiHandler(bridge, taskSessions,
                                    RecordSigning.fromEnvironment()));
            if (starter != null) {
                http.withContext("/api/correction",
                        new CorrectionConsoleApiHandler(catalog, taskSessions));
            }
            if (authoring != null && taskSessions.requiresLogin()) {
                http.withContext("/api/workflow-launch",
                        new WorkflowLaunchConsoleApiHandler(catalog, taskSessions));
                http.withContext("/api/workflow-authoring",
                        WorkflowLaunchConsoleApiHandler.authoringEntry(catalog, taskSessions));
            }
        }
        if (apiToken == null) {
            http.withContext("/api/protomolt", new ApiProxyHandler("/api/protomolt",
                        registryPort,
                        "no registry is running; start with --registry-git or --demo"))
                    .withContext("/api/serve", new ApiProxyHandler("/api/serve",
                        servePort, "server is still starting"));
        } else {
            DisabledSurfaceHandler disabled = new DisabledSurfaceHandler(
                    "this browser surface is disabled when --api-token is set; "
                            + "use the task console or an authenticated protocol client");
            if (taskConsole == null) {
                http.withContext("/console", disabled)
                        .withContext("/api/task-session", disabled)
                        .withContext("/api/tasks", disabled);
            }
            http.withContext("/api/protomolt", disabled)
                    .withContext("/api/serve", disabled);
        }
    }
}
