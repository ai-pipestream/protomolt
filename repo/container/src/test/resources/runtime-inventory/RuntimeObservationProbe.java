package ai.protomolt.proto.repo.container.ledger;

import java.nio.file.Path;

/** Runs in a separate ordinary Java process using only built JARs. */
public final class RuntimeObservationProbe {
    public static void main(String[] args) throws Exception {
        var observation = DocumentAssessmentRuntimeObserver.observe(Path.of(args[0]), () -> {});
        var identity = observation.identity(() -> {});
        if (!identity.getValidationProfile().equals("protomolt-retained-schema-admission/v1")
                || !identity.getCatalogConfiguration().equals("empty-taxonomy-and-postal/v1")
                || identity.getImplementationArtifactsCount() < 1 || !identity.hasJvm()
                || !identity.getJvm().getVersion().contains(Runtime.version().toString())) throw new AssertionError("Wrong observed identity");
        if (!Class.forName("ai.protomolt.proto.repo.admission.AdmissionProbe").getMethod("run").invoke(null).equals(5))
            throw new AssertionError("Real validator probe failed");
        ObservedAssessmentProbe.run(observation);
        var thread = Thread.currentThread();
        var previous = thread.getContextClassLoader();
        try {
            thread.setContextClassLoader(ClassLoader.getPlatformClassLoader());
            mustRefuse(() -> observation.identity(() -> {}));
            mustRefuse(() -> DocumentAssessmentRuntimeObserver.observe(Path.of(args[0]), () -> {}));
        } finally { thread.setContextClassLoader(previous); }
        String classpath = System.getProperty("java.class.path");
        try {
            System.setProperty("java.class.path", classpath + java.io.File.pathSeparator + "changed.jar");
            mustRefuse(() -> observation.identity(() -> {}));
        } finally { System.setProperty("java.class.path", classpath); }
        var cancelled = new java.util.concurrent.CancellationException("stop");
        try {
            observation.identity(() -> { throw cancelled; });
            throw new AssertionError("Cancellation ignored");
        } catch (java.util.concurrent.CancellationException expected) {
            if (expected != cancelled) throw new AssertionError("Cancellation replaced");
        }
        if (!observation.identity(() -> {}).equals(identity)) throw new AssertionError("Observation changed after retry");
        try { Class.forName("org.junit.jupiter.api.Test"); throw new AssertionError("Test library leaked into process"); }
        catch (ClassNotFoundException expected) { }
        System.out.println("OBSERVED " + identity.getImplementationArtifactsCount() + " artifacts; validation and context guards passed");
    }
    private interface Action { void run() throws Exception; }
    private static void mustRefuse(Action action) throws Exception {
        try { action.run(); throw new AssertionError("Runtime context change accepted"); }
        catch (IllegalStateException expected) { }
    }
}
