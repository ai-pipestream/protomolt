package ai.protomolt.proto.workflow.authoring;

/**
 * Trusted host extension selected by operator configuration, never by an author.
 * Providers are loaded from the runtime classpath using {@link java.util.ServiceLoader}.
 * The host must find exactly one provider with the configured ID and refuse startup
 * for an absent/duplicate ID or null template. No permissive default is implied.
 */
public interface WorkflowSourceTemplateProvider {
    /** Stable nonempty provider ID used in deployment configuration. */
    String id();

    /** Restriction run after generic policy preflight and before intent reservation. */
    WorkflowCandidatePreparer.SourceTemplate template();
}
