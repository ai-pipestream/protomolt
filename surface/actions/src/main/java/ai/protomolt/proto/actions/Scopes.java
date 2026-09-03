package ai.protomolt.proto.actions;

import java.util.Set;

/**
 * The closed authorization-scope vocabulary. Every guarded operation names exactly one of
 * these; a policy or caller naming anything else is refused by name, so a typo is a loud
 * failure rather than a silently dead grant. The design of record is
 * {@code docs/design/authorization-scopes.md}.
 */
public final class Scopes {

    /** Schema reads and every pure computation over caller-supplied or registered schemas. */
    public static final String SCHEMA_READ = "schema-read";

    /** Registry mutation: publishing subjects and configuration, federation sync that pushes. */
    public static final String SCHEMA_WRITE = "schema-write";

    /** Calling other services through the platform: invocation, jobs, model inference. */
    public static final String SERVICE_INVOKE = "service-invoke";

    /** Workflow and pipeline execution and their evidence verbs. */
    public static final String WORKFLOW_RUN = "workflow-run";

    /** Artifact reads and writes outside a workflow run's own recording. */
    public static final String ARTIFACT_ACCESS = "artifact-access";

    /** The delegation and mesh coordination surfaces. */
    public static final String WORKER_COORDINATE = "worker-coordinate";

    /** Querying a search service. */
    public static final String SEARCH_QUERY = "search-query";

    /** A search service's workflow-driven indexing, deletion, and replay verbs. */
    public static final String SEARCH_INDEX = "search-index";

    /** Querying a metrics service: describing mappings and running aggregate queries. */
    public static final String METRICS_QUERY = "metrics-query";

    /** Rebuilding a metrics service's rollup tables. */
    public static final String METRICS_REBUILD = "metrics-rebuild";

    /** The whole vocabulary; membership here is what "a known scope" means. */
    public static final Set<String> VOCABULARY = Set.of(
            SCHEMA_READ, SCHEMA_WRITE, SERVICE_INVOKE, WORKFLOW_RUN, ARTIFACT_ACCESS,
            WORKER_COORDINATE, SEARCH_QUERY, SEARCH_INDEX, METRICS_QUERY, METRICS_REBUILD);

    private Scopes() {
    }
}
