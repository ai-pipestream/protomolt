package ai.protomolt.proto.kafka.serde;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The ServiceLoader fan-out behind {@link SerdeMetricsListeners}, exercised with controlled
 * classloaders: no providers means a no-op, and a provider whose class cannot load is skipped
 * without costing the providers beside it — the classpath a broken plugin jar produces.
 */
class SerdeMetricsListenersTest {

    @Test
    void noProvidersMeansANoOpListener() throws Exception {
        try (URLClassLoader empty = new URLClassLoader(new URL[0], null)) {
            SerdeMetricsListener listener = SerdeMetricsListeners.load(empty);
            // Every event kind lands nowhere, and nothing throws.
            listener.onSerialized("t", "a.B");
            listener.onDeserialized("t", "a.B");
            listener.onValidationRejected("t", "a.B", true, List.of("string.min_len"));
            listener.onTypeRefused("t", SerdeMetricsListener.REASON_WRONG_TYPE);
            listener.onRegistryFallback();
            listener.onQualityScored("t", "a.B", 0.5, Map.of("titled", 1.0));
            listener.onQualityRejected("t", "a.B", 0.5);
        }
    }

    /**
     * A services file naming a class that does not exist raises ServiceConfigurationError from
     * the loader; the load must skip it and still fan out to the providers that did load —
     * including the hostile one, whose throw must stay contained.
     */
    @Test
    void aProviderThatCannotLoadIsSkipped(@TempDir Path dir) throws Exception {
        Path services = Files.createDirectories(dir.resolve("META-INF/services"));
        Files.writeString(services.resolve(SerdeMetricsListener.class.getName()),
                "ai.protomolt.proto.kafka.serde.RecordingMetricsListener\n"
                        + "no.such.BogusListener\n");
        try (URLClassLoader loader = new URLClassLoader(
                new URL[]{dir.toUri().toURL()}, getClass().getClassLoader())) {
            SerdeMetricsListener loaded = SerdeMetricsListeners.load(loader);

            RecordingMetricsListener.reset();
            loaded.onSerialized("topic", "a.B");

            assertThat(RecordingMetricsListener.EVENTS).contains("serialized topic a.B");
        }
    }
}
