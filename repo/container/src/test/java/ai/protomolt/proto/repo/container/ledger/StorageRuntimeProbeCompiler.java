package ai.protomolt.proto.repo.container.ledger;

import java.nio.file.*;
import java.util.*;
import static org.assertj.core.api.Assertions.*;

/** Compiles test probes against the observed production JAR set, without ambient test classes. */
final class StorageRuntimeProbeCompiler {
    record Compiled(Path bundle, String classpath, Path probe) {}
    static Compiled compile(Path directory) throws Exception {
        String bundleProperty = System.getProperty("protomolt.test.admissionRuntimeBundle");
        String hostProperty = System.getProperty("protomolt.test.storageHostClasspath");
        assertThat(bundleProperty).as("Run admissionStorageTest or admissionHistoricalRuntimeTest").isNotBlank();
        assertThat(hostProperty).isNotBlank();
        var bundle = Path.of(bundleProperty);
        var inventory = DocumentRuntimeInventory.read(bundle, () -> {});
        var jars = new LinkedHashMap<String, Path>();
        inventory.identities().forEach(artifact -> jars.put(artifact.getArtifactSha256(),
                bundle.resolve("artifacts/" + artifact.getArtifactSha256() + ".jar")));
        // The production host shares admission dependencies. Include each exact
        // artifact once, by bytes; conflicting class providers still fail observation.
        for (String entry : hostProperty.split(java.util.regex.Pattern.quote(java.io.File.pathSeparator))) {
            var path = Path.of(entry);
            var identity = DocumentRuntimeArtifact.observe("storage-host", path,
                    new DocumentRuntimeArtifact.Limits(Files.size(path), 65536, 64), () -> {});
            jars.putIfAbsent(identity.getArtifactSha256(), path);
        }
        String classpath = String.join(java.io.File.pathSeparator, jars.values().stream().map(Path::toString).toList());
        var classes = Files.createDirectory(directory.resolve("classes"));
        var sources = new ArrayList<String>();
        for (String name : List.of("ManagedHistoricalColdDispatchProbe", "ManagedHistoricalHostProbe", "HistoricalPreparationCorruption", "HistoricalObservedWriter", "HistoricalPublicCommitWinnerProbe", "HistoricalPublicAuthorizationProbe", "HistoricalPinReleaseFault", "HistoricalPublicReplayRefusalProbe", "HistoricalPublicTakeoverProbe", "HistoricalPublicColdDispatchProbe", "HistoricalPublicDispatchProbe", "HistoricalColdRestartProbe", "BoundedDocumentPublicConsumer", "BoundedDocumentPublicFactoryProbe", "BoundedPublicationSchemaShutdownProbe", "BoundedPublicationRpcCancellationProbe", "BoundedPublicationShutdownProbe", "BoundedDocumentRejectionProbe", "BoundedDocumentDelayedWrite", "DocumentDelayedWriteRecoveryProbe", "BoundedDocumentReadGate", "DocumentCleanupRetryProbe", "DocumentCleanupRetentionProbe", "BoundedDocumentWriteFault", "BoundedDocumentRestartProbe", "BoundedDocumentHostProbe", "FencedSchemaWorkerProbe", "JournaledSuccessorPublicationProbe", "ManagedJournaledDrainProbe", "ObservedAssessmentProbe", "AssessmentCreationProbe", "AssessmentCaptureFaultProbe", "AssessmentProviderProbe", "AssessmentMixedReuseProbe", "AssessmentReplayInputsProbe", "AssessmentOperationReplayProbe", "AssessmentOperationReplayHost", "JournaledAssessmentProbe", "AssessmentRejectionProbe", "AssessmentStorageProbe", "AssessmentRestartProbe", "RejectedAssessmentRestartProbe", "RejectedAssessmentExpiryProbe", "RejectedAssessmentSourceProbe", "NativeAssessmentPreparationProbe", "PromotedAssessmentCommitProbe", "AssessmentStageFaultProbe", "NativeAssessmentExecutionProbe", "NativeAssessmentRestartProbe", "NativeAssessmentRuntimeProbe", "NativeSchemaRevisionProbe", "HistoricalAssessmentCreationProbe", "HistoricalCreateCommitFault", "HistoricalPublicationExpiryProbe", "HistoricalPublicationClaimExpiryProbe", "HistoricalPublicationBeforeClaimProbe", "HistoricalPostRollbackPublicationProbe", "HistoricalPublicationRevocationProbe", "HistoricalClaimedMixedPublicationProbe", "HistoricalConcurrentStartProbe", "HistoricalSuccessorCreateProbe", "HistoricalInitialOwnerProbe", "HistoricalUploadFaultProbe", "HistoricalUploadTakeover", "HistoricalRuntimeShutdownProbe", "HistoricalInstalledOwnerProbe", "HistoricalGenerationOverlapProbe", "HistoricalPublicationCommitWinnerProbe", "HistoricalPublicationLosingSuccessorProbe", "HistoricalOwnerReconciliationHost", "HistoricalReconciliationExpiryProbe", "HistoricalCreateAuthorizationProbe", "HistoricalAuthorizationCommitGate", "HistoricalCreateWinnerProbe", "HistoricalMixedOriginContentionProbe", "HistoricalPublicationProbe", "HistoricalMixedPublicationProbe", "NativeHistoricalMaterializationProbe", "NativeHistoricalMaterializationTransportProbe", "NativeHistoricalMaterializationLifecycleProbe")) {
            var source = directory.resolve(name + ".java");
            try (var input = StorageRuntimeProbeCompiler.class.getResourceAsStream("/runtime-inventory/" + name + ".java")) {
                assertThat(input).isNotNull(); Files.copy(input, source);
            }
            sources.add(source.toString());
        }
        var crashSource = directory.resolve("JournaledAssessmentCrashProbe.java");
        try (var input = StorageRuntimeProbeCompiler.class.getResourceAsStream("/runtime-inventory/JournaledAssessmentCrashProbe.java")) {
            assertThat(input).isNotNull(); Files.copy(input, crashSource);
        }
        sources.add(crashSource.toString());
        var arguments = new ArrayList<>(List.of("-proc:none", "-classpath", classpath, "-d", classes.toString()));
        arguments.addAll(sources);
        assertThat(javax.tools.ToolProvider.getSystemJavaCompiler().run(null, null, null, arguments.toArray(String[]::new))).isZero();
        var probe = directory.resolve("storage-probe.jar");
        try (var output = new java.util.jar.JarOutputStream(Files.newOutputStream(probe)); var paths = Files.walk(classes)) {
            for (var file : paths.filter(Files::isRegularFile).sorted().toList()) {
                output.putNextEntry(new java.util.jar.JarEntry(classes.relativize(file).toString().replace(java.io.File.separatorChar, '/')));
                Files.copy(file, output); output.closeEntry();
            }
        }
        return new Compiled(bundle, classpath, probe);
    }
}
