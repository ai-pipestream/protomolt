package ai.protomolt.proto.compat;

import ai.protomolt.proto.compat.ActionEnvelopeSnapshot.ActionEnvelopeEntry;
import ai.protomolt.proto.compat.RestRouteSnapshot.RestRouteEntry;
import ai.protomolt.proto.descriptors.StabilityTier;
import ai.protomolt.proto.sources.CompiledProtos;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies the checked-in five-surface compatibility baseline under {@code compatibility/baseline/}
 * against the live repository surfaces and ensures breaking mutations fail closed while
 * additive changes pass.
 */
class CompatibilityBaselineSystemTest {

    @Test
    void checkedInBaselineMatchesLiveRepositoryAndEnforcesNoPrematureReleaseGuarantee() throws Exception {
        Path baselineDir = repositoryRoot().resolve("compatibility/baseline");
        assertThat(baselineDir.resolve(ReleaseBaselineGate.MANIFEST_FILE)).exists();
        assertThat(baselineDir.resolve(ReleaseBaselineGate.JAVA_API_FILE)).exists();
        assertThat(baselineDir.resolve(ReleaseBaselineGate.PROTOBUF_DESCRIPTORS_FILE)).exists();
        assertThat(baselineDir.resolve(ReleaseBaselineGate.PROTOBUF_STABILITY_FILE)).exists();
        assertThat(baselineDir.resolve(ReleaseBaselineGate.STORED_FORMATS_FILE)).exists();
        assertThat(baselineDir.resolve(ReleaseBaselineGate.REST_ROUTES_FILE)).exists();
        assertThat(baselineDir.resolve(ReleaseBaselineGate.ACTION_ENVELOPES_FILE)).exists();

        ReleaseBaselineBundle recorded = ReleaseBaselineGate.load(baselineDir);
        assertThat(recorded.manifest().status())
                .isEqualTo(ReleaseBaselineStatus.UNRELEASED_INITIAL_SNAPSHOT);
        assertThat(recorded.manifest().releaseTag()).isEmpty();
        assertThat(recorded.manifest().status().hasExternalReleaseGuarantee()).isFalse();

        assertThat(recorded.javaApi().classes()).isNotEmpty();
        assertThat(recorded.protobuf().descriptorSet().getFileCount()).isGreaterThan(0);
        assertThat(recorded.storedFormats().formats()).hasSizeGreaterThanOrEqualTo(5);
        assertThat(recorded.restRoutes().routes()).isNotEmpty();
        assertThat(recorded.actionEnvelopes().actions()).hasSize(44);

        ReleaseBaselineBundle live = CompatibilityBaselineGenerator.captureLiveBundle(recorded.manifest());
        ReleaseBaselineReport report = ReleaseBaselineGate.evaluate(recorded, live);
        report.throwIfIncompatible();
        assertThat(report.isCompatible()).isTrue();
        assertThat(report.changes()).isEmpty();
    }

    @Test
    void additiveChangesToLiveBundlePassWithoutRegeneratingBaselineWhileBreakingChangesFail() throws Exception {
        Path baselineDir = repositoryRoot().resolve("compatibility/baseline");
        ReleaseBaselineBundle recorded = ReleaseBaselineGate.load(baselineDir);
        ReleaseBaselineBundle live = CompatibilityBaselineGenerator.captureLiveBundle(recorded.manifest());

        // 1. Additive change: add a new REST route and a new action verb without touching baseline files
        List<RestRouteEntry> augmentedRoutes = new ArrayList<>(live.restRoutes().routes());
        augmentedRoutes.add(new RestRouteEntry(
                "schema-registry:v2.preview",
                "/v2/preview",
                List.of("GET"),
                List.of(),
                List.of(),
                "",
                "application/json",
                "",
                "PreviewResponse",
                List.of(200),
                false,
                StabilityTier.STABLE));

        List<ActionEnvelopeEntry> augmentedActions = new ArrayList<>(live.actionEnvelopes().actions());
        augmentedActions.add(new ActionEnvelopeEntry(
                "preview-compatibility",
                "schema:read",
                false,
                "ai.protomolt.PreviewRequest",
                "ai.protomolt.PreviewResponse",
                List.of(),
                Map.of("subject", "TYPE_STRING"),
                Map.of("ok", "TYPE_BOOL"),
                StabilityTier.STABLE));

        ReleaseBaselineBundle additiveCandidate = new ReleaseBaselineBundle(
                live.manifest(),
                live.javaApi(),
                live.protobuf(),
                live.storedFormats(),
                new RestRouteSnapshot(augmentedRoutes),
                new ActionEnvelopeSnapshot(augmentedActions));

        ReleaseBaselineReport additiveReport = ReleaseBaselineGate.evaluate(recorded, additiveCandidate);
        assertThat(additiveReport.isCompatible()).isTrue();
        assertThat(additiveReport.unwaivedViolations()).isEmpty();
        assertThat(additiveReport.changes()).hasSize(2);

        // 2. Breaking change: drop an existing STABLE action from the live catalog
        List<ActionEnvelopeEntry> reducedActions = live.actionEnvelopes().actions().subList(
                1, live.actionEnvelopes().actions().size());
        ReleaseBaselineBundle breakingCandidate = new ReleaseBaselineBundle(
                live.manifest(),
                live.javaApi(),
                live.protobuf(),
                live.storedFormats(),
                live.restRoutes(),
                new ActionEnvelopeSnapshot(reducedActions));

        ReleaseBaselineReport breakingReport = ReleaseBaselineGate.evaluate(recorded, breakingCandidate);
        assertThat(breakingReport.isCompatible()).isFalse();
        assertThatThrownBy(breakingReport::throwIfIncompatible)
                .isInstanceOf(IncompatibleSurfaceException.class)
                .hasMessageContaining(ActionEnvelopeCompatibilityChecker.ACTION_REMOVED);
    }

    @Test
    void publishedInventoryIncludesCanonicalContractsAndRejectsDocumentServiceBreak() throws Exception {
        ReleaseBaselineBundle recorded = ReleaseBaselineGate.load(
                repositoryRoot().resolve("compatibility/baseline"));
        List<String> descriptorPaths = recorded.protobuf().descriptorSet().getFileList().stream()
                .map(FileDescriptorProto::getName)
                .toList();
        assertThat(descriptorPaths).contains(
                "ai/protomolt/proto/repo/v2/document_service.proto",
                "ai/protomolt/proto/repo/v2/manifest.proto",
                "ai/protomolt/proto/repo/v1/drive.proto",
                "ai/protomolt/proto/repo/archive/v1/archive_service.proto",
                "ai/protomolt/proto/metric/v1/metric_service.proto",
                "ai/protomolt/proto/emit/v1/renderer.proto",
                "ai/protomolt/proto/authz/v1/access_policy.proto");

        FileDescriptorSet.Builder brokenBuilder = recorded.protobuf().descriptorSet().toBuilder();
        for (int i = 0; i < brokenBuilder.getFileCount(); i++) {
            FileDescriptorProto file = brokenBuilder.getFile(i);
            if (file.getName().equals("ai/protomolt/proto/repo/v2/document_service.proto")) {
                brokenBuilder.setFile(i, file.toBuilder()
                        .setService(0, file.getService(0).toBuilder().clearMethod())
                        .build());
            }
        }
        FileDescriptorSet brokenDescriptors = brokenBuilder.build();
        ReleaseBaselineBundle broken = new ReleaseBaselineBundle(
                recorded.manifest(),
                recorded.javaApi(),
                ProtobufSurfaceSnapshot.of(brokenDescriptors),
                recorded.storedFormats(),
                recorded.restRoutes(),
                recorded.actionEnvelopes());
        ReleaseBaselineReport report = ReleaseBaselineGate.evaluate(recorded, broken);
        assertThat(report.unwaivedViolations())
                .anyMatch(change -> change.ruleId().equals(ChangeRules.METHOD_REMOVED));
        assertThatThrownBy(report::throwIfIncompatible)
                .isInstanceOf(IncompatibleSurfaceException.class);
    }

    @Test
    void publishedInventoryCompilesEveryMainSourceProto() throws Exception {
        CompiledProtos compiled = CompatibilityBaselineGenerator.compilePublishedDescriptors();
        assertThat(compiled.descriptorFor("ai/protomolt/proto/repo/v2/document_service.proto"))
                .isPresent();
        assertThat(compiled.descriptorFor("ai/protomolt/proto/repo/v2/manifest.proto"))
                .isPresent();
        assertThat(compiled.descriptorFor("ai/protomolt/proto/metric/v1/metric_service.proto"))
                .isPresent();
        assertThat(compiled.descriptorFor("ai/protomolt/proto/emit/v1/renderer.proto"))
                .isPresent();
    }

    private static Path repositoryRoot() throws IOException {
        Path current = Path.of("").toAbsolutePath().normalize();
        while (current != null && !Files.exists(current.resolve("settings.gradle"))) {
            current = current.getParent();
        }
        if (current == null) {
            throw new IOException("could not locate repository root from "
                    + Path.of("").toAbsolutePath());
        }
        return current;
    }
}
