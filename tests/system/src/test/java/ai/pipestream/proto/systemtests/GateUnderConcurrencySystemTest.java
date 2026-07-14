package ai.pipestream.proto.systemtests;

import ai.pipestream.proto.compat.CompatibilityChecker;
import ai.pipestream.proto.compat.CompatibilityMode;
import ai.pipestream.proto.compat.CompatibilityResult;
import ai.pipestream.proto.registry.CompatibilityWriteGate;
import ai.pipestream.proto.registry.GitSchemaRegistryStore;
import ai.pipestream.proto.registry.IncompatibleRegistrationException;
import ai.pipestream.proto.registry.StoredSchema;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Eight threads race evolutions of one subject through {@link GitSchemaRegistryStore} guarded
 * by {@link CompatibilityWriteGate} under the default BACKWARD mode. Half the candidates are
 * compatible (each adds a distinct new field, and removals of each other's additions are
 * wire-clean, so they stay compatible under any interleaving); half change the wire type of
 * field 1 and can never pass against any accepted predecessor. Afterwards the stored history
 * itself is the evidence: it must be linear with no gaps, every consecutive pair must re-check
 * as BACKWARD-compatible, and every rejected candidate must have surfaced as
 * {@link IncompatibleRegistrationException} carrying the {@code FIELD_TYPE_CHANGED} rule and
 * must be absent from the store.
 */
class GateUnderConcurrencySystemTest {

    private static final String SUBJECT = "race/v1/doc.proto";
    private static final int THREADS = 8;

    private static final String BASE_SCHEMA = """
            syntax = "proto3";
            package race.v1;
            message Doc {
              int32 value = 1;
            }
            """;

    private static final List<String> INCOMPATIBLE_TYPES =
            List.of("string", "sint32", "double", "fixed64");

    @TempDir
    Path tempDir;

    private final CompatibilityChecker checker = CompatibilityChecker.create();
    private final ProtoSourceCompiler compiler = new ProtoSourceCompiler();

    @Test
    void racingEvolutionsYieldALinearHistoryWherePairsAreCompatibleAndRejectionsAreTyped()
            throws Exception {
        try (GitSchemaRegistryStore store = GitSchemaRegistryStore.builder()
                .repositoryDir(tempDir.resolve("registry-repo"))
                .writeGate(new CompatibilityWriteGate())
                .build()) {
            assertThat(store.globalCompatibilityMode()).isEqualTo("BACKWARD");
            StoredSchema baseline = store.register(SUBJECT, BASE_SCHEMA, List.of());
            assertThat(baseline.version()).isEqualTo(1);

            List<String> compatibleCandidates = new ArrayList<>();
            List<String> incompatibleCandidates = new ArrayList<>();
            for (int worker = 0; worker < THREADS; worker++) {
                if (worker % 2 == 0) {
                    compatibleCandidates.add(compatibleCandidate(worker));
                } else {
                    incompatibleCandidates.add(incompatibleCandidate(worker));
                }
            }

            List<Outcome> outcomes = race(store,
                    concat(compatibleCandidates, incompatibleCandidates));

            // Every compatible candidate registered; every incompatible one threw the typed
            // exception carrying the rule that fired.
            List<Outcome> accepted = outcomes.stream().filter(o -> o.stored() != null).toList();
            List<Outcome> rejected = outcomes.stream().filter(o -> o.failure() != null).toList();
            assertThat(accepted).hasSize(compatibleCandidates.size());
            assertThat(rejected).hasSize(incompatibleCandidates.size());
            assertThat(accepted).extracting(o -> o.schemaText())
                    .containsExactlyInAnyOrderElementsOf(compatibleCandidates);
            for (Outcome outcome : rejected) {
                assertThat(outcome.failure())
                        .isInstanceOf(IncompatibleRegistrationException.class);
                IncompatibleRegistrationException incompatible =
                        (IncompatibleRegistrationException) outcome.failure();
                assertThat(incompatible.violations())
                        .isNotEmpty()
                        .anySatisfy(violation -> assertThat(violation)
                                .contains("FIELD_TYPE_CHANGED")
                                .contains("race.v1.Doc.value"));
            }

            // The history is linear with no gaps: 1 (baseline) through 1 + accepted count.
            List<Integer> versions = store.versions(SUBJECT);
            List<Integer> expected = new ArrayList<>();
            for (int v = 1; v <= 1 + compatibleCandidates.size(); v++) {
                expected.add(v);
            }
            assertThat(versions).isEqualTo(expected);

            // Identity is unique across the accepted set (baseline included).
            Set<Integer> globalIds = new HashSet<>();
            Set<Integer> acceptedVersions = new HashSet<>();
            globalIds.add(baseline.globalId());
            for (Outcome outcome : accepted) {
                assertThat(globalIds.add(outcome.stored().globalId()))
                        .as("global id %d must be unique", outcome.stored().globalId()).isTrue();
                assertThat(acceptedVersions.add(outcome.stored().version()))
                        .as("version %d must be unique", outcome.stored().version()).isTrue();
            }
            assertThat(acceptedVersions).doesNotContain(1);

            // Post-hoc gate audit: replay the checker over the final stored history — every
            // consecutive pair must be BACKWARD-compatible, whatever order the race produced.
            for (int version = 2; version <= versions.size(); version++) {
                StoredSchema previous = store.version(SUBJECT, version - 1).orElseThrow();
                StoredSchema current = store.version(SUBJECT, version).orElseThrow();
                CompatibilityResult recheck = checker.check(compile(previous.schemaText()),
                        compile(current.schemaText()), CompatibilityMode.BACKWARD);
                assertThat(recheck.isCompatible())
                        .as("v%d -> v%d must re-check compatible: %s", version - 1, version,
                                recheck.violations())
                        .isTrue();
            }

            // Nothing rejected ever reached storage.
            for (String candidate : incompatibleCandidates) {
                assertThat(store.findByContent(SUBJECT, candidate, List.of())).isEmpty();
            }
        }
    }

    // ------------------------------------------------------------------ helpers

    /** Adds a distinct field; removals of other threads' additions are wire-clean, so these
     * remain BACKWARD-compatible against any accepted predecessor. */
    private static String compatibleCandidate(int worker) {
        return BASE_SCHEMA.replace("int32 value = 1;",
                "int32 value = 1;\n  int64 extra_" + worker + " = " + (10 + worker) + ";");
    }

    /** Changes field 1's wire type — incompatible with every accepted predecessor, all of
     * which keep {@code int32 value = 1}. */
    private static String incompatibleCandidate(int worker) {
        String newType = INCOMPATIBLE_TYPES.get((worker / 2) % INCOMPATIBLE_TYPES.size());
        return BASE_SCHEMA.replace("int32 value = 1;", newType + " value = 1;");
    }

    private record Outcome(String schemaText, StoredSchema stored, RuntimeException failure) {
    }

    /** All candidates start on one latch; each records its stored schema or its exception. */
    private static List<Outcome> race(GitSchemaRegistryStore store, List<String> candidates)
            throws Exception {
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(candidates.size());
        try {
            List<Future<Outcome>> futures = new ArrayList<>();
            for (String candidate : candidates) {
                futures.add(pool.submit((Callable<Outcome>) () -> {
                    start.await();
                    try {
                        return new Outcome(candidate,
                                store.register(SUBJECT, candidate, List.of()), null);
                    } catch (RuntimeException e) {
                        return new Outcome(candidate, null, e);
                    }
                }));
            }
            start.countDown();
            List<Outcome> outcomes = new ArrayList<>();
            for (Future<Outcome> future : futures) {
                outcomes.add(future.get(120, TimeUnit.SECONDS));
            }
            return outcomes;
        } finally {
            pool.shutdownNow();
            assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static List<String> concat(List<String> first, List<String> second) {
        List<String> all = new ArrayList<>(first);
        all.addAll(second);
        return all;
    }

    private FileDescriptorSet compile(String schemaText) throws Exception {
        return compiler.compile(ProtoSourceSet.builder()
                .add(SUBJECT, schemaText, "post-hoc-audit")
                .build()).descriptorSet();
    }
}
