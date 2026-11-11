package ai.protomolt.proto.compat;

import ai.protomolt.proto.compat.ActionEnvelopeSnapshot.ActionEnvelopeEntry;
import ai.protomolt.proto.compat.JavaApiSnapshot.ClassEntry;
import ai.protomolt.proto.compat.RestRouteSnapshot.RestRouteEntry;
import ai.protomolt.proto.compat.StoredFormatSnapshot.StoredFormatEntry;
import ai.protomolt.proto.descriptors.StabilityTier;
import ai.protomolt.proto.sources.CompiledProtos;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies the checked-in five-surface compatibility baseline under {@code compatibility/baseline/}
 * against the live repository surfaces and ensures breaking mutations, stability-map tampering,
 * and unused waivers fail closed while additive changes pass.
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

        assertThat(recorded.javaApi().classes()).hasSizeGreaterThanOrEqualTo(1_576);
        List<String> javaClassNames = recorded.javaApi().classes().stream()
                .map(ClassEntry::className)
                .toList();
        assertThat(javaClassNames).contains(
                "ai.protomolt.proto.compat.CompatibilityChecker",
                "ai.protomolt.proto.compat.CompatibilityChecker$Builder",
                "ai.protomolt.proto.grpc.policy.OutboundChannelPolicy$Builder",
                "ai.protomolt.proto.emit.parquet.ParquetEmitter",
                "ai.protomolt.proto.emit.renderer.GrpcRendererProvider",
                "ai.protomolt.proto.registry.service.SchemaRegistryServer",
                "ai.protomolt.proto.registry.service.SchemaRegistryServer$RouteContract",
                "ai.protomolt.proto.repo.v2.DocumentServiceGrpc");
        assertThat(javaClassNames)
                .noneMatch(name -> name.contains(".samples.")
                        || name.contains(".examples.")
                        || name.contains(".correction."));

        assertThat(recorded.protobuf().descriptorSet().getFileCount()).isGreaterThanOrEqualTo(138);
        assertThat(recorded.protobuf().elementStability()).hasSizeGreaterThanOrEqualTo(9_974);

        assertThat(recorded.storedFormats().formats()).hasSizeGreaterThanOrEqualTo(21);
        assertThat(recorded.storedFormats().formats().stream()
                .map(StoredFormatEntry::formatId)
                .toList())
                .contains(
                        "repo.document.v1",
                        "repo.drive.v1",
                        "repo.manifest.accession.v2",
                        "repo.manifest.document.v2",
                        "repo.catalog.entry.v2",
                        "repo.archive.v1",
                        "authz.access-policy.v1",
                        "receipt.signed-work-record.v1",
                        "receipt.trust-snapshot.v1",
                        "grpc.service-profile.v1",
                        "mesh.cluster.event-log.v1",
                        "delegation.encrypted-repository-state.v1",
                        "workflow.definition.v1",
                        "workflow.run-evidence.v1");

        assertThat(recorded.restRoutes().routes()).hasSizeGreaterThanOrEqualTo(98);
        assertThat(recorded.restRoutes().routes().stream()
                .map(RestRouteEntry::routeId)
                .toList())
                .contains(
                        "schema-registry:native.descriptor-set",
                        "repo-http:documents.upload",
                        "repo-http:archive.upload",
                        "intake-http:intake.upload")
                .anyMatch(id -> id.startsWith("grpc-json:"));

        assertThat(recorded.actionEnvelopes().actions()).hasSizeGreaterThanOrEqualTo(89);
        assertThat(recorded.actionEnvelopes().actions().stream()
                .map(ActionEnvelopeEntry::name)
                .toList())
                .contains(
                        "delegation-offer",
                        "query-metrics",
                        "mesh-node-register",
                        "publish-config",
                        "registry-sync",
                        "search",
                        "replay-documents",
                        "pull-jdbc",
                        "pull-s3");

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
    void publishedInventoryCompilesBomModuleProtosAndExcludesUnpublishedFixtures() throws Exception {
        CompiledProtos compiled = CompatibilityBaselineGenerator.compilePublishedDescriptors();
        assertThat(compiled.descriptorFor("ai/protomolt/proto/repo/v2/document_service.proto"))
                .isPresent();
        assertThat(compiled.descriptorFor("ai/protomolt/proto/repo/v2/manifest.proto"))
                .isPresent();
        assertThat(compiled.descriptorFor("ai/protomolt/proto/metric/v1/metric_service.proto"))
                .isPresent();
        assertThat(compiled.descriptorFor("ai/protomolt/proto/emit/v1/renderer.proto"))
                .isPresent();
        assertThat(compiled.descriptorFor("ai/protomolt/proto/meta/v1/stability.proto"))
                .isPresent();
        assertThat(compiled.descriptorSet().getFileList().stream()
                .map(FileDescriptorProto::getName)
                .toList())
                .doesNotContain(
                        "demo_shop.proto",
                        "demo/search/v1/demo_search.proto",
                        "ai/protomolt/proto/samples/authoring/v1/authoring_fixture.proto",
                        "ai/protomolt/proto/samples/starter/v1/coordination_report.proto",
                        "ai/protomolt/proto/correction/v1/correction.proto",
                        "court/v1/opinion_metadata.proto")
                .noneMatch(name -> name.contains("/examples/")
                        || name.contains("/correction/"));
    }

    @Test
    void tamperingWithProtobufStabilityJsonOrAddingUnusedWaiverFailsClosed(@TempDir Path tempDir)
            throws Exception {
        Path baselineDir = repositoryRoot().resolve("compatibility/baseline");
        for (String file : List.of(
                ReleaseBaselineGate.MANIFEST_FILE,
                ReleaseBaselineGate.JAVA_API_FILE,
                ReleaseBaselineGate.PROTOBUF_DESCRIPTORS_FILE,
                ReleaseBaselineGate.PROTOBUF_STABILITY_FILE,
                ReleaseBaselineGate.STORED_FORMATS_FILE,
                ReleaseBaselineGate.REST_ROUTES_FILE,
                ReleaseBaselineGate.ACTION_ENVELOPES_FILE)) {
            Files.copy(baselineDir.resolve(file), tempDir.resolve(file));
        }

        String originalStability = Files.readString(
                tempDir.resolve(ReleaseBaselineGate.PROTOBUF_STABILITY_FILE),
                StandardCharsets.UTF_8);
        int firstStable = originalStability.indexOf("\"STABLE\"");
        assertThat(firstStable).isNotNegative();
        String tamperedStability = originalStability.substring(0, firstStable)
                + "\"EVOLVING\""
                + originalStability.substring(firstStable + "\"STABLE\"".length());
        Files.writeString(
                tempDir.resolve(ReleaseBaselineGate.PROTOBUF_STABILITY_FILE),
                tamperedStability,
                StandardCharsets.UTF_8);

        assertThatThrownBy(() -> ReleaseBaselineGate.load(tempDir))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(ReleaseBaselineGate.PROTOBUF_STABILITY_FILE);

        ReleaseBaselineBundle recorded = ReleaseBaselineGate.load(baselineDir);
        ReleaseBaselineManifest manifestWithUnusedWaiver = new ReleaseBaselineManifest(
                recorded.manifest().schemaVersion(),
                recorded.manifest().status(),
                recorded.manifest().releaseVersion(),
                recorded.manifest().releaseTag(),
                recorded.manifest().baselineNote(),
                List.of(new CompatibilityWaiver(
                        "W-UNUSED-SYSTEM-TEST",
                        CompatibilitySurface.ACTION_ENVELOPE,
                        ActionEnvelopeCompatibilityChecker.ACTION_REMOVED,
                        "nonexistent-action",
                        "#827",
                        "unused waiver must fail closed")));
        ReleaseBaselineBundle recordedWithUnusedWaiver = new ReleaseBaselineBundle(
                manifestWithUnusedWaiver,
                recorded.javaApi(),
                recorded.protobuf(),
                recorded.storedFormats(),
                recorded.restRoutes(),
                recorded.actionEnvelopes());

        ReleaseBaselineReport unusedReport = ReleaseBaselineGate.evaluate(recordedWithUnusedWaiver, recorded);
        assertThat(unusedReport.isCompatible()).isFalse();
        assertThat(unusedReport.unusedWaivers()).hasSize(1);
        assertThatThrownBy(unusedReport::throwIfIncompatible)
                .isInstanceOf(IncompatibleSurfaceException.class)
                .hasMessageContaining("W-UNUSED-SYSTEM-TEST");
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
