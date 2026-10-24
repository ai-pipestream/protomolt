package ai.protomolt.proto.serve;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.grpc.policy.OutboundChannelPolicy;
import ai.protomolt.proto.inference.structured.StructuredGenerator;
import ai.protomolt.proto.jobs.service.WorkflowRunsConfig;
import ai.protomolt.proto.jobs.service.events.WorkflowRunEventRelay;
import ai.protomolt.proto.jobs.service.store.JdbcWorkflowRunStore;
import ai.protomolt.proto.jobs.service.store.WorkflowRunDatabase;
import ai.protomolt.proto.jobs.service.store.WorkflowRunStoreConfig;
import ai.protomolt.proto.jobs.service.worker.WorkflowRunWorker;
import ai.protomolt.proto.workflow.WorkflowRepository;
import ai.protomolt.proto.workflow.WorkflowRunner;
import java.lang.management.ManagementFactory;

/**
 * The workflow-runs fleet of the serve wiring: the JDBC store (which boots and Flyway-migrates
 * before the catalog so the jobs verbs serve from the same truth the worker fleet executes
 * from), the worker fleet, and the optional Kafka event relay. Without a broker the workers
 * run verb-submitted jobs with no event relay and no request topic, useful for a single-box
 * deployment; with one, lifecycle events publish to the events topic and the request topic is
 * consumed.
 */
final class JobsFleetMount {

    private JobsFleetMount() {
    }

    /**
     * The opened fleet. {@code relay} is null in a broker-less deployment. Ownership passes to
     * the caller: on a failed serve start the caller closes worker, relay, then database.
     *
     * @param database the migrated workflow-run database
     * @param store the run store the catalog verbs and the worker share
     * @param config the resolved runs configuration (name, leases, topics, broker)
     * @param runner the runner executing claimed work
     * @param worker the worker fleet; started once the catalog is bound
     * @param relay the Kafka lifecycle-event relay, or null without a broker
     */
    record Prepared(WorkflowRunDatabase database, JdbcWorkflowRunStore store,
                    WorkflowRunsConfig config, WorkflowRunner runner, WorkflowRunWorker worker,
                    WorkflowRunEventRelay relay) {
    }

    /**
     * Boots the database and assembles the fleet from the jobs options. A failure partway
     * closes what was opened, in the same order a failed serve start would.
     */
    static Prepared prepare(ProtoMoltServe.JobsOptions jobs, ActionContext context,
                            WorkflowRepository workflows, StructuredGenerator structured,
                            OutboundChannelPolicy outboundPolicy) {
        WorkflowRunDatabase database = null;
        WorkflowRunWorker worker = null;
        WorkflowRunEventRelay relay = null;
        try {
            database = new WorkflowRunDatabase(
                    new WorkflowRunStoreConfig(jobs.jdbcUrl(), jobs.username(), jobs.password()));
            JdbcWorkflowRunStore store = new JdbcWorkflowRunStore(database);
            String requestTopic = jobs.kafkaBootstrap() == null
                    ? null
                    : jobs.requestTopic() != null ? jobs.requestTopic() : "workflow-run-requests";
            WorkflowRunsConfig config = new WorkflowRunsConfig(
                    "serve-" + ManagementFactory.getRuntimeMXBean().getName(),
                    jobs.workers(), WorkflowRunsConfig.DEFAULT_LEASE_DURATION,
                    WorkflowRunsConfig.DEFAULT_POLL_INTERVAL,
                    WorkflowRunsConfig.DEFAULT_BACKOFF_BASE_SECONDS,
                    WorkflowRunsConfig.DEFAULT_MAX_ATTEMPTS, jobs.targetConcurrency(),
                    requestTopic, WorkflowRunsConfig.DEFAULT_EVENTS_TOPIC, jobs.kafkaBootstrap(),
                    null);
            WorkflowRunner runner = new WorkflowRunner(outboundPolicy, structured);
            worker = new WorkflowRunWorker(store, context, workflows, runner, config);
            if (jobs.kafkaBootstrap() != null) {
                var producer = WorkflowRunEventRelay.newProducer(jobs.kafkaBootstrap(), null);
                try {
                    relay = new WorkflowRunEventRelay(store, producer,
                            config.eventsTopic(), config.pollInterval(), 100);
                } catch (RuntimeException e) {
                    closeQuietly(producer);
                    throw e;
                }
            }
            return new Prepared(database, store, config, runner, worker, relay);
        } catch (RuntimeException e) {
            closeQuietly(worker);
            closeQuietly(relay);
            closeQuietly(database);
            throw e;
        }
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception ignored) {
                // best-effort teardown on a failed fleet boot
            }
        }
    }
}
