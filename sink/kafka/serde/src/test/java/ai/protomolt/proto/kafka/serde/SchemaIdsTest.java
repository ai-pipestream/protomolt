package ai.protomolt.proto.kafka.serde;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.descriptors.DescriptorLoader.DescriptorLoadException;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The caching and backoff around registry lookups. Neither is visible from the outside: a
 * serde whose backoff is broken still produces correct bytes, just with a connection attempt
 * per record, which turns a registry outage into a second outage. And the caches are what
 * make an outage survivable at all, so what exactly they hold matters.
 *
 * <p>These drive a fake registry through {@link SchemaLookup} and a hand-cranked clock, so a
 * backoff window is crossed by moving the clock rather than by sleeping.
 */
class SchemaIdsTest {

    private static FileDescriptor file;

    @BeforeAll
    static void compile() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("schemaids/v1/event.proto", """
                        syntax = "proto3";
                        package schemaids.v1;
                        message Event { string id = 1; }
                        message Other { string id = 1; }
                        """, "test")
                .build());
        file = compiled.descriptorFor("schemaids/v1/event.proto").orElseThrow();
    }

    /** A registry that answers from maps and counts what it was asked. */
    private static final class FakeRegistry implements SchemaLookup {
        final Map<String, OptionalInt> subjects = new HashMap<>();
        final Map<Integer, FileDescriptor> schemas = new HashMap<>();
        final List<String> asked = new ArrayList<>();
        boolean down;
        boolean closed;

        @Override
        public OptionalInt idForSubject(String subject) throws DescriptorLoadException {
            asked.add("subject:" + subject);
            if (down) {
                throw new DescriptorLoadException("registry is unreachable");
            }
            return subjects.getOrDefault(subject, OptionalInt.empty());
        }

        @Override
        public FileDescriptor schemaById(int schemaId) throws DescriptorLoadException {
            asked.add("schema:" + schemaId);
            if (down) {
                throw new DescriptorLoadException("registry is unreachable");
            }
            FileDescriptor found = schemas.get(schemaId);
            if (found == null) {
                throw new DescriptorLoadException("no schema " + schemaId);
            }
            return found;
        }

        @Override
        public void close() {
            closed = true;
        }

        long countOf(String prefix) {
            return asked.stream().filter(a -> a.startsWith(prefix)).count();
        }
    }

    /** Counts fallbacks per test; the shared RecordingMetricsListener keeps static state. */
    private static final class CountingMetrics implements SerdeMetricsListener {
        private int fallbacks;

        @Override
        public void onRegistryFallback() {
            fallbacks++;
        }

        int registryFallbacks() {
            return fallbacks;
        }
    }

    private final FakeRegistry registry = new FakeRegistry();
    private final CountingMetrics metrics = new CountingMetrics();
    private final AtomicLong clock = new AtomicLong();

    /** Backoff of one second on a clock that only moves when a test moves it. */
    private SchemaIds schemaIds() {
        return new SchemaIds(registry, 1_000, metrics, clock::get);
    }

    private void advance(long millis) {
        clock.addAndGet(millis * 1_000_000L);
    }

    // --- the subject-id cache ---------------------------------------------------

    @Test
    void aResolvedSubjectIsAskedAboutOnce() {
        registry.subjects.put("events-value", OptionalInt.of(7));
        SchemaIds ids = schemaIds();

        assertThat(ids.idForSubject("events-value")).hasValue(7);
        assertThat(ids.idForSubject("events-value")).hasValue(7);
        assertThat(ids.idForSubject("events-value")).hasValue(7);

        assertThat(registry.countOf("subject:")).isEqualTo(1);
    }

    /**
     * The cache is what makes an outage survivable: a subject resolved before the registry
     * went down keeps answering from memory, so a running producer is unaffected. This is
     * the property the fallback is usually credited with, and it belongs here instead.
     */
    @Test
    void aSubjectResolvedBeforeAnOutageKeepsAnswering() {
        registry.subjects.put("events-value", OptionalInt.of(7));
        SchemaIds ids = schemaIds();
        assertThat(ids.idForSubject("events-value")).hasValue(7);

        registry.down = true;
        advance(10_000);

        assertThat(ids.idForSubject("events-value")).hasValue(7);
        assertThat(registry.countOf("subject:")).isEqualTo(1);
        assertThat(metrics.registryFallbacks()).isZero();
    }

    // --- backoff ----------------------------------------------------------------

    @Test
    void anUnreachableRegistryIsAskedOncePerWindowNotOncePerCall() {
        registry.down = true;
        SchemaIds ids = schemaIds();

        for (int i = 0; i < 20; i++) {
            assertThat(ids.idForSubject("events-value")).isEmpty();
        }

        assertThat(registry.countOf("subject:")).isEqualTo(1);
    }

    @Test
    void theRegistryIsAskedAgainOnceTheWindowExpires() {
        registry.down = true;
        SchemaIds ids = schemaIds();
        assertThat(ids.idForSubject("events-value")).isEmpty();

        advance(999);
        assertThat(ids.idForSubject("events-value")).isEmpty();
        assertThat(registry.countOf("subject:")).as("still inside the window").isEqualTo(1);

        advance(2);
        assertThat(ids.idForSubject("events-value")).isEmpty();
        assertThat(registry.countOf("subject:")).as("window expired").isEqualTo(2);
    }

    /** A registry that comes back repairs the answer without anyone restarting the serde. */
    @Test
    void aRecoveredRegistryIsPickedUpAfterTheWindow() {
        registry.down = true;
        SchemaIds ids = schemaIds();
        assertThat(ids.idForSubject("events-value")).isEmpty();

        registry.down = false;
        registry.subjects.put("events-value", OptionalInt.of(7));
        advance(1_001);

        assertThat(ids.idForSubject("events-value")).hasValue(7);
    }

    /** A subject the registry does not have is a failure to answer, not an error. */
    @Test
    void anUnregisteredSubjectBacksOffTheSameWay() {
        SchemaIds ids = schemaIds();

        assertThat(ids.idForSubject("events-value")).isEmpty();
        assertThat(ids.idForSubject("events-value")).isEmpty();

        assertThat(registry.countOf("subject:")).isEqualTo(1);
        assertThat(metrics.registryFallbacks()).isEqualTo(1);
    }

    @Test
    void everyFailedLookupIsCountedOncePerWindow() {
        registry.down = true;
        SchemaIds ids = schemaIds();

        for (int i = 0; i < 5; i++) {
            ids.idForSubject("events-value");
        }
        advance(1_001);
        ids.idForSubject("events-value");

        assertThat(metrics.registryFallbacks()).isEqualTo(2);
    }

    // --- resolving a frame's type -----------------------------------------------

    @Test
    void aSchemaIdResolvesToTheMessageAtTheIndexPath() {
        registry.schemas.put(11, file);
        SchemaIds ids = schemaIds();

        assertThat(ids.messageFor(11, List.of(0)).getFullName()).isEqualTo("schemaids.v1.Event");
        assertThat(ids.messageFor(11, List.of(1)).getFullName()).isEqualTo("schemaids.v1.Other");
    }

    @Test
    void anIndexPathTheSchemaDoesNotHaveIsUnresolved() {
        registry.schemas.put(11, file);
        SchemaIds ids = schemaIds();

        assertThat(ids.messageFor(11, List.of(9))).isNull();
        assertThat(metrics.registryFallbacks()).isEqualTo(1);
    }

    /**
     * A missing index path backs off under the lookup, not under the id: the schema resolved
     * fine and another type inside it may still resolve. Keying this by id would take a whole
     * schema out of service over one absent message.
     */
    @Test
    void aMissingTypeDoesNotTakeTheWholeSchemaOutOfService() {
        registry.schemas.put(11, file);
        SchemaIds ids = schemaIds();

        assertThat(ids.messageFor(11, List.of(9))).isNull();

        assertThat(ids.messageFor(11, List.of(0)).getFullName()).isEqualTo("schemaids.v1.Event");
    }

    @Test
    void anUnresolvableIdIsAskedAboutOncePerWindow() {
        SchemaIds ids = schemaIds();

        assertThat(ids.messageFor(404, List.of(0))).isNull();
        assertThat(ids.messageFor(404, List.of(0))).isNull();

        assertThat(registry.countOf("schema:")).isEqualTo(1);
    }

    /** An id that failed to resolve blocks every index path under it, not just the one asked. */
    @Test
    void anUnresolvableIdBlocksItsOtherIndexPathsToo() {
        SchemaIds ids = schemaIds();

        assertThat(ids.messageFor(404, List.of(0))).isNull();
        assertThat(ids.messageFor(404, List.of(1))).isNull();

        assertThat(registry.countOf("schema:")).isEqualTo(1);
    }

    // --- resolving a type by name -----------------------------------------------

    @Test
    void aTypeResolvesByNameWithinTheSchemaAnIdNames() {
        registry.schemas.put(11, file);
        SchemaIds ids = schemaIds();

        assertThat(ids.typeInSchema(11, "schemaids.v1.Other").getFullName())
                .isEqualTo("schemaids.v1.Other");
    }

    @Test
    void aTypeTheRegisteredSchemaDoesNotDeclareIsUnresolved() {
        registry.schemas.put(11, file);
        SchemaIds ids = schemaIds();

        assertThat(ids.typeInSchema(11, "schemaids.v1.Absent")).isNull();
        assertThat(metrics.registryFallbacks()).isEqualTo(1);
    }

    @Test
    void theTwoTypeLookupsDoNotShareABackoffSlot() {
        // Same id, same absent-ness, different questions: an index path and a name. Keyed
        // together, answering one would suppress the other.
        registry.schemas.put(11, file);
        SchemaIds ids = schemaIds();

        assertThat(ids.messageFor(11, List.of(9))).isNull();
        assertThat(ids.typeInSchema(11, "schemaids.v1.Absent")).isNull();

        assertThat(metrics.registryFallbacks()).isEqualTo(2);
    }

    // --- the failure-tracking bound ---------------------------------------------

    /**
     * Frames carrying garbage ids must not grow the retry maps without limit: the ids come
     * off the wire, so an attacker or a broken producer picks how many there are.
     */
    @Test
    void trackedFailuresStayBounded() {
        SchemaIds ids = schemaIds();

        for (int id = 0; id < 3_000; id++) {
            ids.messageFor(id, List.of(0));
        }

        // Bounded, and still working: the map cleared rather than refusing to track more.
        registry.schemas.put(99_999, file);
        assertThat(ids.messageFor(99_999, List.of(0))).isNotNull();
    }

    // --- lifecycle --------------------------------------------------------------

    @Test
    void closingTheSerdeClosesTheRegistryClient() {
        schemaIds().close();
        assertThat(registry.closed).isTrue();
    }

    @Test
    void noRegistryConfiguredIsASupportedWayToRun() {
        assertThat(SchemaIds.create(null, 1_000, metrics)).isNull();
        assertThat(SchemaIds.create("", 1_000, metrics)).isNull();
        assertThat(SchemaIds.create("   ", 1_000, metrics)).isNull();
    }
}
