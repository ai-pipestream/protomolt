package ai.protomolt.proto.compat;

import ai.protomolt.proto.compat.ActionEnvelopeSnapshot.ActionEnvelopeEntry;
import ai.protomolt.proto.compat.JavaApiSnapshot.ClassEntry;
import ai.protomolt.proto.compat.JavaApiSnapshot.MethodEntry;
import ai.protomolt.proto.compat.RestRouteSnapshot.RestRouteEntry;
import ai.protomolt.proto.descriptors.StabilityTier;
import ai.protomolt.proto.grpc.policy.OutboundChannelPolicy;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.DescriptorProtos.ServiceDescriptorProto;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies the checked-in five-surface compatibility baseline under {@code compatibility/baseline/}
 * against the live repository surfaces and ensures breaking mutations across all canonical
 * contract families fail closed while additive changes pass.
 */
class CompatibilityBaselineSystemTest {

    @Test
    void checkedInBaselineMatchesLiveRepositoryAndCoversCompletePublishedContractInventory() throws Exception {
        Path root = repositoryRoot();
        Path baselineDir = root.resolve("compatibility/baseline");
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

        // Verify that every published .proto file under src/main/proto/ai/protomolt/proto/** is present
        Set<String> recordedProtoFiles = recorded.protobuf().descriptorSet().getFileList().stream()
                .map(FileDescriptorProto::getName)
                .collect(Collectors.toSet());
        Set<String> expectedPublishedProtoFiles = discoverPublishedProtoFiles(root);
        assertThat(expectedPublishedProtoFiles)
                .contains(
                        "ai/protomolt/proto/repo/v2/document_service.proto",
                        "ai/protomolt/proto/repo/v2/manifest.proto",
                        "ai/protomolt/proto/repo/v1/manifest.proto",
                        "ai/protomolt/proto/repo/v1/drive.proto",
                        "ai/protomolt/proto/repo/archive/v1/archive_service.proto",
                        "ai/protomolt/proto/repo/archive/v1/archive.proto",
                        "ai/protomolt/proto/repo/archive/v1/entry.proto",
                        "ai/protomolt/proto/metric/v1/metric_service.proto",
                        "ai/protomolt/proto/metric/v1/metric.proto",
                        "ai/protomolt/proto/authz/v1/access_policy.proto",
                        "ai/protomolt/proto/emit/v1/renderer.proto");
        assertThat(recordedProtoFiles).containsAll(expectedPublishedProtoFiles);

        // Verify canonical gRPC services are tracked as STABLE in protobuf stability metadata
        assertThat(recorded.protobuf().elementStability())
                .containsEntry("ai.protomolt.proto.repo.v2.DocumentService", StabilityTier.STABLE)
                .containsEntry("ai.protomolt.proto.repo.archive.v1.ArchiveService", StabilityTier.STABLE)
                .containsEntry("ai.protomolt.proto.repo.v1.DriveService", StabilityTier.STABLE)
                .containsEntry("ai.protomolt.proto.metric.v1.MetricService", StabilityTier.STABLE)
                .containsEntry("ai.protomolt.proto.emit.v1.RendererService", StabilityTier.STABLE)
                .containsEntry("ai.protomolt.proto.authz.v1.AccessPolicy", StabilityTier.STABLE)
                .containsEntry("ai.protomolt.proto.grpc.service.v1.ProtoMoltService", StabilityTier.STABLE);

        assertThat(recorded.javaApi().classes()).hasSizeGreaterThan(400);
        assertThat(recorded.javaApi().classesByName())
                .containsKey(OutboundChannelPolicy.class.getName())
                .containsKey(OutboundChannelPolicy.Builder.class.getName());
        ClassEntry builderEntry = recorded.javaApi().classesByName()
                .get(OutboundChannelPolicy.Builder.class.getName());
        assertThat(builderEntry.methods())
                .extracting(MethodEntry::signatureKey)
                .contains("maxActiveChannels(int)");

        assertThat(recorded.storedFormats().formats()).hasSize(16);
        assertThat(recorded.restRoutes().routes()).isNotEmpty();
        assertThat(recorded.actionEnvelopes().actions()).hasSize(61);

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
    void breakingChangesInDocumentArchiveDriveMetricAndRendererContractsFailClosed() throws Exception {
        Path baselineDir = repositoryRoot().resolve("compatibility/baseline");
        ReleaseBaselineBundle recorded = ReleaseBaselineGate.load(baselineDir);

        FileDescriptorSet mutatedDescriptors = mutateCanonicalContracts(
                recorded.protobuf().descriptorSet());
        ProtobufSurfaceSnapshot mutatedProtobuf = new ProtobufSurfaceSnapshot(
                mutatedDescriptors,
                recorded.protobuf().elementStability());

        ReleaseBaselineBundle breakingCandidate = new ReleaseBaselineBundle(
                recorded.manifest(),
                recorded.javaApi(),
                mutatedProtobuf,
                recorded.storedFormats(),
                recorded.restRoutes(),
                recorded.actionEnvelopes());

        ReleaseBaselineReport report = ReleaseBaselineGate.evaluate(recorded, breakingCandidate);
        assertThat(report.isCompatible()).isFalse();
        assertThatThrownBy(report::throwIfIncompatible)
                .isInstanceOf(IncompatibleSurfaceException.class)
                .hasMessageContaining("ai.protomolt.proto.repo.v2.DocumentService.SaveDocument")
                .hasMessageContaining("ai.protomolt.proto.repo.v2.SaveDocumentRequest.target")
                .hasMessageContaining("ai.protomolt.proto.repo.archive.v1.ArchiveService.PutEntry")
                .hasMessageContaining("ai.protomolt.proto.repo.archive.v1.PutEntryRequest.address")
                .hasMessageContaining("ai.protomolt.proto.repo.v1.DriveService.CreateDrive")
                .hasMessageContaining("ai.protomolt.proto.repo.v1.Drive.drive_id")
                .hasMessageContaining("ai.protomolt.proto.metric.v1.MetricService.QueryMetrics")
                .hasMessageContaining("ai.protomolt.proto.metric.v1.QueryMetricsRequest.mapping_subject")
                .hasMessageContaining("ai.protomolt.proto.emit.v1.RendererService.Render")
                .hasMessageContaining("ai.protomolt.proto.authz.v1.AccessPolicy.principals");
    }

    @Test
    void breakingChangesInPublicNestedJavaApiFailClosed() throws Exception {
        Path baselineDir = repositoryRoot().resolve("compatibility/baseline");
        ReleaseBaselineBundle recorded = ReleaseBaselineGate.load(baselineDir);

        String targetNestedClass = OutboundChannelPolicy.Builder.class.getName();
        List<ClassEntry> mutatedClasses = new ArrayList<>();
        for (ClassEntry entry : recorded.javaApi().classes()) {
            if (!entry.className().equals(targetNestedClass)) {
                mutatedClasses.add(entry);
                continue;
            }
            List<MethodEntry> filteredMethods = entry.methods().stream()
                    .filter(m -> !m.signatureKey().equals("maxActiveChannels(int)"))
                    .toList();
            mutatedClasses.add(new ClassEntry(
                    entry.className(),
                    entry.kind(),
                    entry.visibility(),
                    entry.finalType(),
                    entry.abstractType(),
                    entry.superclass(),
                    entry.interfaces(),
                    entry.stability(),
                    entry.constructors(),
                    filteredMethods,
                    entry.fields()));
        }

        ReleaseBaselineBundle breakingCandidate = new ReleaseBaselineBundle(
                recorded.manifest(),
                new JavaApiSnapshot(mutatedClasses),
                recorded.protobuf(),
                recorded.storedFormats(),
                recorded.restRoutes(),
                recorded.actionEnvelopes());

        ReleaseBaselineReport report = ReleaseBaselineGate.evaluate(recorded, breakingCandidate);
        assertThat(report.isCompatible()).isFalse();
        assertThatThrownBy(report::throwIfIncompatible)
                .isInstanceOf(IncompatibleSurfaceException.class)
                .hasMessageContaining(JavaApiCompatibilityChecker.JAVA_METHOD_REMOVED)
                .hasMessageContaining(targetNestedClass + "#maxActiveChannels(int)");
    }

    @Test
    void inspectClassResourceFailsLoudlyWhenClassLoadingOrLinkageFails() {
        NoClassDefFoundError linkageFailure =
                new NoClassDefFoundError("missing published API dependency");
        ClassLoader brokenLoader = new ClassLoader(null) {
            @Override
            public Class<?> loadClass(String name) {
                throw linkageFailure;
            }

            @Override
            protected Class<?> loadClass(String name, boolean resolve) {
                throw linkageFailure;
            }
        };

        Map<String, FileDescriptor> files = new TreeMap<>();
        Map<String, Class<?>> classes = new LinkedHashMap<>();

        assertThatThrownBy(() -> CompatibilityBaselineGenerator.inspectClassResource(
                "ai/protomolt/proto/grpc/policy/OutboundChannelPolicy.class",
                brokenLoader,
                files,
                classes))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ai/protomolt/proto/grpc/policy/OutboundChannelPolicy.class")
                .hasRootCause(linkageFailure);
    }

    private static FileDescriptorSet mutateCanonicalContracts(FileDescriptorSet original) {
        FileDescriptorSet.Builder builder = FileDescriptorSet.newBuilder();
        for (FileDescriptorProto file : original.getFileList()) {
            switch (file.getName()) {
                case "ai/protomolt/proto/repo/v2/document_service.proto" ->
                        builder.addFile(removeMessageField(
                                removeServiceMethod(file, "DocumentService", "SaveDocument"),
                                "SaveDocumentRequest",
                                "target"));
                case "ai/protomolt/proto/repo/archive/v1/archive_service.proto" ->
                        builder.addFile(removeMessageField(
                                removeServiceMethod(file, "ArchiveService", "PutEntry"),
                                "PutEntryRequest",
                                "address"));
                case "ai/protomolt/proto/repo/v1/drive.proto" ->
                        builder.addFile(removeMessageField(
                                removeServiceMethod(file, "DriveService", "CreateDrive"),
                                "Drive",
                                "drive_id"));
                case "ai/protomolt/proto/metric/v1/metric_service.proto" ->
                        builder.addFile(removeMessageField(
                                removeServiceMethod(file, "MetricService", "QueryMetrics"),
                                "QueryMetricsRequest",
                                "mapping_subject"));
                case "ai/protomolt/proto/emit/v1/renderer.proto" ->
                        builder.addFile(removeServiceMethod(file, "RendererService", "Render"));
                case "ai/protomolt/proto/authz/v1/access_policy.proto" ->
                        builder.addFile(removeMessageField(file, "AccessPolicy", "principals"));
                default -> builder.addFile(file);
            }
        }
        return builder.build();
    }

    private static FileDescriptorProto removeServiceMethod(
            FileDescriptorProto file, String serviceName, String methodName) {
        FileDescriptorProto.Builder fileBuilder = file.toBuilder().clearService();
        for (ServiceDescriptorProto service : file.getServiceList()) {
            if (!service.getName().equals(serviceName)) {
                fileBuilder.addService(service);
                continue;
            }
            ServiceDescriptorProto.Builder serviceBuilder = service.toBuilder().clearMethod();
            service.getMethodList().stream()
                    .filter(method -> !method.getName().equals(methodName))
                    .forEach(serviceBuilder::addMethod);
            fileBuilder.addService(serviceBuilder.build());
        }
        return fileBuilder.build();
    }

    private static FileDescriptorProto removeMessageField(
            FileDescriptorProto file, String messageName, String fieldName) {
        FileDescriptorProto.Builder fileBuilder = file.toBuilder().clearMessageType();
        for (DescriptorProto message : file.getMessageTypeList()) {
            if (!message.getName().equals(messageName)) {
                fileBuilder.addMessageType(message);
                continue;
            }
            DescriptorProto.Builder messageBuilder = message.toBuilder().clearField();
            message.getFieldList().stream()
                    .filter(field -> !field.getName().equals(fieldName))
                    .forEach(messageBuilder::addField);
            fileBuilder.addMessageType(messageBuilder.build());
        }
        return fileBuilder.build();
    }

    private static Set<String> discoverPublishedProtoFiles(Path root) throws IOException {
        Set<String> protoFiles = new TreeSet<>();
        String marker = "/src/main/proto/";
        Files.walkFileTree(root, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                String rel = root.relativize(dir).toString().replace('\\', '/');
                if (rel.equals(".git")
                        || rel.equals(".gradle")
                        || rel.endsWith("/build")
                        || rel.startsWith("apps/")
                        || rel.startsWith("samples")
                        || rel.startsWith("examples/")) {
                    return FileVisitResult.SKIP_SUBTREE;
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                String rel = "/" + root.relativize(file).toString().replace('\\', '/');
                int idx = rel.indexOf(marker);
                if (idx >= 0 && rel.endsWith(".proto")) {
                    String protoRel = rel.substring(idx + marker.length());
                    if (protoRel.startsWith("ai/protomolt/proto/")) {
                        protoFiles.add(protoRel);
                    }
                }
                return FileVisitResult.CONTINUE;
            }
        });
        return protoFiles;
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
