package ai.pipestream.proto.registry;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Store contract exercised against every {@link SchemaRegistryStore} implementation: version
 * numbering, content-identity idempotency, reference enforcement, compatibility-mode fallback,
 * write-gate wiring and compile verification. Implementation-specific behavior (git commits,
 * locking, refresh) lives in the concrete subclasses.
 */
abstract class SchemaRegistryStoreContractTest {

    static final String CORE_SUBJECT = "common/v1/core.proto";
    static final String USER_SUBJECT = "common/v1/user.proto";

    static final String CORE_PROTO = """
            syntax = "proto3";
            package common.v1;
            message Core {
              string id = 1;
            }
            """;

    static final String CORE_PROTO_V2 = """
            syntax = "proto3";
            package common.v1;
            message Core {
              string id = 1;
              string name = 2;
            }
            """;

    static final String USER_PROTO = """
            syntax = "proto3";
            package common.v1;
            import "common/v1/core.proto";
            message User {
              Core core = 1;
            }
            """;

    private final List<SchemaRegistryStore> opened = new ArrayList<>();

    /** A fresh store with the given (possibly {@code null}) write gate. */
    protected abstract SchemaRegistryStore create(SchemaRegistryStore.WriteGate gate) throws Exception;

    SchemaRegistryStore store() throws Exception {
        return store(null);
    }

    SchemaRegistryStore store(SchemaRegistryStore.WriteGate gate) throws Exception {
        SchemaRegistryStore store = create(gate);
        opened.add(store);
        return store;
    }

    @AfterEach
    void closeStores() {
        opened.forEach(SchemaRegistryStore::close);
        opened.clear();
    }

    // ---------------------------------------------------------------- versions & lookups

    @Test
    void freshStoreIsEmptyWithBackwardGlobalMode() throws Exception {
        SchemaRegistryStore store = store();
        assertThat(store.subjects()).isEmpty();
        assertThat(store.versions("nope")).isEmpty();
        assertThat(store.version("nope", 1)).isEmpty();
        assertThat(store.latest("nope")).isEmpty();
        assertThat(store.byGlobalId(1)).isEmpty();
        assertThat(store.globalCompatibilityMode()).isEqualTo("BACKWARD");
        assertThat(store.compatibilityMode("nope")).isEmpty();
    }

    @Test
    void versionsArePerSubjectOneBasedAndAscending() throws Exception {
        SchemaRegistryStore store = store();
        StoredSchema v1 = store.register(CORE_SUBJECT, CORE_PROTO, List.of());
        StoredSchema v2 = store.register(CORE_SUBJECT, CORE_PROTO_V2, List.of());
        StoredSchema other = store.register("other.proto", CORE_PROTO, List.of());

        assertThat(v1.version()).isEqualTo(1);
        assertThat(v2.version()).isEqualTo(2);
        assertThat(other.version()).isEqualTo(1);
        assertThat(store.versions(CORE_SUBJECT)).containsExactly(1, 2);
        assertThat(store.subjects()).containsExactly(CORE_SUBJECT, "other.proto");
        assertThat(store.latest(CORE_SUBJECT)).contains(v2);
        assertThat(store.version(CORE_SUBJECT, 1)).contains(v1);
        assertThat(store.version(CORE_SUBJECT, 3)).isEmpty();
    }

    @Test
    void globalIdsAreUniqueAndMonotonicAcrossSubjects() throws Exception {
        SchemaRegistryStore store = store();
        StoredSchema a = store.register("a.proto", CORE_PROTO, List.of());
        StoredSchema b = store.register("b.proto", CORE_PROTO, List.of());
        StoredSchema a2 = store.register("a.proto", CORE_PROTO_V2, List.of());

        assertThat(List.of(a.globalId(), b.globalId(), a2.globalId())).doesNotHaveDuplicates();
        assertThat(b.globalId()).isGreaterThan(a.globalId());
        assertThat(a2.globalId()).isGreaterThan(b.globalId());
        assertThat(store.byGlobalId(b.globalId())).contains(b);
        assertThat(store.byGlobalId(a2.globalId())).contains(a2);
    }

    @Test
    void identicalContentRegistersIdempotently() throws Exception {
        SchemaRegistryStore store = store();
        StoredSchema first = store.register(CORE_SUBJECT, CORE_PROTO, List.of());
        StoredSchema again = store.register(CORE_SUBJECT, CORE_PROTO, List.of());

        assertThat(again).isEqualTo(first);
        assertThat(store.versions(CORE_SUBJECT)).containsExactly(1);
    }

    @Test
    void sameTextWithDifferentReferencesIsANewVersion() throws Exception {
        SchemaRegistryStore store = store();
        store.register(CORE_SUBJECT, CORE_PROTO, List.of());
        store.register(CORE_SUBJECT, CORE_PROTO_V2, List.of());
        StoredSchema plain = store.register(USER_SUBJECT, USER_PROTO,
                List.of(new SchemaReference(CORE_SUBJECT, CORE_SUBJECT, 1)));
        StoredSchema rewired = store.register(USER_SUBJECT, USER_PROTO,
                List.of(new SchemaReference(CORE_SUBJECT, CORE_SUBJECT, 2)));

        assertThat(plain.version()).isEqualTo(1);
        assertThat(rewired.version()).isEqualTo(2);
    }

    @Test
    void contentHashIsStableSha256OverTextAndReferences() throws Exception {
        SchemaRegistryStore store = store();
        StoredSchema stored = store.register(CORE_SUBJECT, CORE_PROTO, List.of());
        assertThat(stored.contentHash())
                .hasSize(64)
                .isEqualTo(SchemaContents.contentHash(CORE_PROTO, List.of()));
    }

    @Test
    void findByContentMatchesTextAndReferencesExactly() throws Exception {
        SchemaRegistryStore store = store();
        store.register(CORE_SUBJECT, CORE_PROTO, List.of());
        List<SchemaReference> refs = List.of(new SchemaReference(CORE_SUBJECT, CORE_SUBJECT, 1));
        StoredSchema user = store.register(USER_SUBJECT, USER_PROTO, refs);

        assertThat(store.findByContent(USER_SUBJECT, USER_PROTO, refs)).contains(user);
        assertThat(store.findByContent(USER_SUBJECT, USER_PROTO, List.of())).isEmpty();
        assertThat(store.findByContent(USER_SUBJECT, USER_PROTO + "//x\n", refs)).isEmpty();
        assertThat(store.findByContent("unknown", CORE_PROTO, List.of())).isEmpty();
    }

    // ---------------------------------------------------------------- reference enforcement

    @Test
    void registeringWithUnknownReferenceThrowsAndStoresNothing() throws Exception {
        SchemaRegistryStore store = store();
        SchemaReference dangling = new SchemaReference(CORE_SUBJECT, CORE_SUBJECT, 1);

        assertThatThrownBy(() -> store.register(USER_SUBJECT, USER_PROTO, List.of(dangling)))
                .isInstanceOf(ReferenceNotFoundException.class)
                .hasMessageContaining(CORE_SUBJECT)
                .extracting(e -> ((ReferenceNotFoundException) e).reference())
                .isEqualTo(dangling);
        assertThat(store.subjects()).isEmpty();
    }

    @Test
    void referenceMustExistWithTheExactVersion() throws Exception {
        SchemaRegistryStore store = store();
        store.register(CORE_SUBJECT, CORE_PROTO, List.of());

        assertThatThrownBy(() -> store.register(USER_SUBJECT, USER_PROTO,
                List.of(new SchemaReference(CORE_SUBJECT, CORE_SUBJECT, 2))))
                .isInstanceOf(ReferenceNotFoundException.class)
                .hasMessageContaining("version 2");
    }

    @Test
    void referencedSchemaResolvesDuringCompileVerification() throws Exception {
        SchemaRegistryStore store = store();
        store.register(CORE_SUBJECT, CORE_PROTO, List.of());
        StoredSchema user = store.register(USER_SUBJECT, USER_PROTO,
                List.of(new SchemaReference(CORE_SUBJECT, CORE_SUBJECT, 1)));

        assertThat(user.references()).containsExactly(new SchemaReference(CORE_SUBJECT, CORE_SUBJECT, 1));
    }

    // ---------------------------------------------------------------- compile verification

    @Test
    void unparseableSchemaTextIsRejected() throws Exception {
        SchemaRegistryStore store = store();
        assertThatThrownBy(() -> store.register(CORE_SUBJECT, "this is not proto {", List.of()))
                .isInstanceOf(InvalidSchemaException.class)
                .hasMessageContaining(CORE_SUBJECT);
        assertThat(store.subjects()).isEmpty();
    }

    @Test
    void schemaImportingAnUndeclaredFileIsRejected() throws Exception {
        SchemaRegistryStore store = store();
        // user.proto imports core.proto but declares no reference for it: unlinkable.
        assertThatThrownBy(() -> store.register(USER_SUBJECT, USER_PROTO, List.of()))
                .isInstanceOf(InvalidSchemaException.class);
        assertThat(store.subjects()).isEmpty();
    }

    // ---------------------------------------------------------------- compatibility modes

    @Test
    void subjectModeFallsBackToGlobalUntilSet() throws Exception {
        SchemaRegistryStore store = store();
        assertThat(store.compatibilityMode(CORE_SUBJECT)).isEmpty();

        store.setGlobalCompatibilityMode("FULL");
        assertThat(store.globalCompatibilityMode()).isEqualTo("FULL");
        assertThat(store.compatibilityMode(CORE_SUBJECT)).isEmpty();

        store.setCompatibilityMode(CORE_SUBJECT, "FORWARD_TRANSITIVE");
        assertThat(store.compatibilityMode(CORE_SUBJECT)).contains("FORWARD_TRANSITIVE");
        assertThat(store.compatibilityMode("other")).isEmpty();
        assertThat(store.globalCompatibilityMode()).isEqualTo("FULL");
    }

    @Test
    void invalidCompatibilityModesAreRejected() throws Exception {
        SchemaRegistryStore store = store();
        assertThatThrownBy(() -> store.setGlobalCompatibilityMode("SIDEWAYS"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> store.setCompatibilityMode(CORE_SUBJECT, "backward"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(store.globalCompatibilityMode()).isEqualTo("BACKWARD");
    }

    // ---------------------------------------------------------------- write gate

    @Test
    void writeGateSeesEffectiveModeAndFullAscendingHistory() throws Exception {
        RecordingGate gate = new RecordingGate();
        SchemaRegistryStore store = store(gate);

        StoredSchema v1 = store.register(CORE_SUBJECT, CORE_PROTO, List.of());
        store.register(CORE_SUBJECT, CORE_PROTO_V2, List.of());
        assertThat(gate.calls).hasSize(2);
        assertThat(gate.calls.get(0).mode()).isEqualTo("BACKWARD"); // global default
        assertThat(gate.calls.get(0).history()).isEmpty();
        assertThat(gate.calls.get(1).history()).containsExactly(v1);
        assertThat(gate.calls.get(1).schemaText()).isEqualTo(CORE_PROTO_V2);

        store.setCompatibilityMode(CORE_SUBJECT, "FULL");
        store.register(CORE_SUBJECT, CORE_PROTO_V2 + "// v3\n", List.of());
        assertThat(gate.calls.getLast().mode()).isEqualTo("FULL");
        assertThat(gate.calls.getLast().history()).hasSize(2);
    }

    @Test
    void writeGateViolationsBecomeIncompatibleRegistrationException() throws Exception {
        RecordingGate gate = new RecordingGate();
        SchemaRegistryStore store = store(gate);
        store.register(CORE_SUBJECT, CORE_PROTO, List.of());

        gate.violations = List.of("field 1 changed type", "field 2 removed");
        assertThatThrownBy(() -> store.register(CORE_SUBJECT, CORE_PROTO_V2, List.of()))
                .isInstanceOf(IncompatibleRegistrationException.class)
                .hasMessageContaining("field 1 changed type")
                .extracting(e -> ((IncompatibleRegistrationException) e).violations())
                .isEqualTo(gate.violations);
        assertThat(store.versions(CORE_SUBJECT)).containsExactly(1);
    }

    @Test
    void writeGateIsSkippedWhenEffectiveModeIsNone() throws Exception {
        RecordingGate gate = new RecordingGate();
        gate.violations = List.of("would reject everything");
        SchemaRegistryStore store = store(gate);

        store.setCompatibilityMode(CORE_SUBJECT, "NONE");
        store.register(CORE_SUBJECT, CORE_PROTO, List.of());
        assertThat(gate.calls).isEmpty();

        // A different subject falls back to global BACKWARD and hits the gate.
        assertThatThrownBy(() -> store.register("other.proto", CORE_PROTO, List.of()))
                .isInstanceOf(IncompatibleRegistrationException.class);
    }

    @Test
    void writeGateIsNotInvokedForIdenticalContent() throws Exception {
        RecordingGate gate = new RecordingGate();
        SchemaRegistryStore store = store(gate);
        store.register(CORE_SUBJECT, CORE_PROTO, List.of());

        gate.violations = List.of("boom");
        StoredSchema again = store.register(CORE_SUBJECT, CORE_PROTO, List.of());
        assertThat(again.version()).isEqualTo(1);
        assertThat(gate.calls).hasSize(1); // only the first registration
    }

    // ---------------------------------------------------------------- stub gate

    static final class RecordingGate implements SchemaRegistryStore.WriteGate {

        record Call(String subject, String mode, List<StoredSchema> history,
                    String schemaText, List<SchemaReference> references) {
        }

        final List<Call> calls = new CopyOnWriteArrayList<>();
        volatile List<String> violations = List.of();

        @Override
        public List<String> validate(String subject, String mode, List<StoredSchema> history,
                                     String schemaText, List<SchemaReference> references,
                                     SchemaRegistryStore store) {
            calls.add(new Call(subject, mode, List.copyOf(history), schemaText, List.copyOf(references)));
            return violations;
        }
    }
}
