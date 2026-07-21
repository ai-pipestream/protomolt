package ai.pipestream.proto.grpc.validate;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * The fan-out behind the listener interface. Every listener on the classpath hears every event,
 * and a listener that throws is contained: the exception does not reach the interceptor that
 * raised the event, and the listeners after it still get called.
 */
class GrpcValidationMetricsListenersTest {

    @TempDir
    Path serviceRoot;

    /** Records every event it is told about, in order. */
    public static final class Recording implements GrpcValidationMetricsListener {

        static final List<String> EVENTS = new ArrayList<>();

        @Override
        public void onValidated(String side, String method, String type) {
            EVENTS.add("validated:" + side + ":" + method + ":" + type);
        }

        @Override
        public void onRejected(String side, String method, String type, List<String> ruleIds) {
            EVENTS.add("rejected:" + side + ":" + method + ":" + type + ":" + ruleIds);
        }

        @Override
        public void onQualityScored(String method, String type, double composite,
                                    Map<String, Double> dimensions) {
            EVENTS.add("scored:" + method + ":" + type + ":" + composite + ":" + dimensions);
        }

        @Override
        public void onQualityRejected(String method, String type, double composite) {
            EVENTS.add("qualityRejected:" + method + ":" + type + ":" + composite);
        }
    }

    /** Throws on every event, and counts how often it was called anyway. */
    public static final class Throwing implements GrpcValidationMetricsListener {

        static int calls;

        @Override
        public void onValidated(String side, String method, String type) {
            calls++;
            throw new IllegalStateException("listener is broken");
        }

        @Override
        public void onRejected(String side, String method, String type, List<String> ruleIds) {
            calls++;
            throw new IllegalStateException("listener is broken");
        }

        @Override
        public void onQualityScored(String method, String type, double composite,
                                    Map<String, Double> dimensions) {
            calls++;
            throw new IllegalStateException("listener is broken");
        }

        @Override
        public void onQualityRejected(String method, String type, double composite) {
            calls++;
            throw new IllegalStateException("listener is broken");
        }
    }

    @Test
    void everyEventReachesEveryListenerOnTheClasspath() throws IOException {
        Recording.EVENTS.clear();
        GrpcValidationMetricsListener fanOut =
                GrpcValidationMetricsListeners.load(loaderExposing(Recording.class));

        fanOut.onValidated(GrpcValidationMetricsListener.SIDE_SERVER, "svc/M", "test.Ping");
        fanOut.onRejected(GrpcValidationMetricsListener.SIDE_CLIENT, "svc/M", "test.Ping",
                List.of("string.min_len"));
        fanOut.onQualityScored("svc/M", "test.Ping", 0.75, Map.of("detailed", 0.75));
        fanOut.onQualityRejected("svc/M", "test.Ping", 0.25);

        assertThat(Recording.EVENTS).containsExactly(
                "validated:server:svc/M:test.Ping",
                "rejected:client:svc/M:test.Ping:[string.min_len]",
                "scored:svc/M:test.Ping:0.75:{detailed=0.75}",
                "qualityRejected:svc/M:test.Ping:0.25");
    }

    /**
     * The documented guarantee: a listener observes, it does not participate. A throw must not
     * propagate to the caller — which is an interceptor mid-call — and must not stop the
     * remaining listeners from being told.
     */
    @Test
    void aThrowingListenerNeitherFailsTheCallNorStopsTheFanOut() throws IOException {
        Recording.EVENTS.clear();
        Throwing.calls = 0;
        // Throwing is listed first, so the recording listener only hears anything if the fan-out
        // carries on past the failure.
        GrpcValidationMetricsListener fanOut = GrpcValidationMetricsListeners.load(
                loaderExposing(Throwing.class, Recording.class));

        assertThatCode(() -> {
            fanOut.onValidated(GrpcValidationMetricsListener.SIDE_SERVER, "svc/M", "test.Ping");
            fanOut.onRejected(GrpcValidationMetricsListener.SIDE_SERVER, "svc/M", "test.Ping",
                    List.of("r1"));
            fanOut.onQualityScored("svc/M", "test.Ping", 0.5, Map.of("d", 0.5));
            fanOut.onQualityRejected("svc/M", "test.Ping", 0.5);
        }).doesNotThrowAnyException();

        assertThat(Recording.EVENTS).containsExactly(
                "validated:server:svc/M:test.Ping",
                "rejected:server:svc/M:test.Ping:[r1]",
                "scored:svc/M:test.Ping:0.5:{d=0.5}",
                "qualityRejected:svc/M:test.Ping:0.5");
        // "will keep being called": containment is not a circuit breaker.
        assertThat(Throwing.calls).isEqualTo(4);
    }

    /** No providers on the classpath: the shared no-op, not a fan-out over an empty list. */
    @Test
    void noProvidersYieldsANoOpListener() throws IOException {
        GrpcValidationMetricsListener none =
                GrpcValidationMetricsListeners.load(loaderExposing());

        assertThat(none).isNotInstanceOf(GrpcValidationMetricsListeners.class);
        assertThat(GrpcValidationMetricsListeners.load(loaderExposing())).isSameAs(none);
    }

    /**
     * A class loader that declares the given classes as providers. They resolve through the
     * parent, so the loaded instances are of the very classes named here.
     */
    private ClassLoader loaderExposing(Class<?>... providers) throws IOException {
        Path services = serviceRoot.resolve(String.valueOf(providers.length))
                .resolve("META-INF/services");
        Files.createDirectories(services);
        Files.writeString(services.resolve(GrpcValidationMetricsListener.class.getName()),
                Arrays.stream(providers).map(Class::getName)
                        .collect(Collectors.joining("\n")));
        URL root = services.getParent().getParent().toUri().toURL();
        return new URLClassLoader(new URL[]{root},
                GrpcValidationMetricsListenersTest.class.getClassLoader());
    }
}
