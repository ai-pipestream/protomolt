package ai.protomolt.proto.mesh.runtime;

/** Published identity carried into remote work and recovery evidence. */
record FlowRunMetadata(String workflowVersion, long deploymentRevision) {
    static FlowRunMetadata transientRun() {
        return new FlowRunMetadata("", 0);
    }
}
