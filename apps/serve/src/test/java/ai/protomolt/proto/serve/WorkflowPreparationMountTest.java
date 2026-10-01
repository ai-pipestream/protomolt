package ai.protomolt.proto.serve;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.delegation.InMemoryTranscriptRepository;
import ai.protomolt.proto.grpc.workflow.ArtifactRepository;
import ai.protomolt.proto.grpc.workflow.FileSystemArtifactRepository;
import ai.protomolt.proto.grpc.workflow.FileSystemRunEvidenceRepository;
import ai.protomolt.proto.grpc.workflow.WorkflowVersionRepository;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.http.openapi.ProtoOpenApiGenerator;
import ai.protomolt.proto.http.rest.ProtoRestMethodRegistry;
import ai.protomolt.proto.jobs.service.store.WorkflowRunStore;
import ai.protomolt.proto.receipt.*;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptanceFixture;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringPolicy;
import ai.protomolt.proto.samples.starter.v1.WorkflowPermittedCall;
import ai.protomolt.proto.workflow.RecordSigning;
import ai.protomolt.proto.workflow.WorkflowRunner;
import ai.protomolt.proto.workflow.authoring.WorkflowCandidatePreparer;
import ai.protomolt.proto.workflow.authoring.WorkflowSourceTemplateProvider;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowCandidateRequest;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationServiceOuterClass;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.security.KeyPair;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkflowPreparationMountTest {
    @TempDir Path temp;

    @Test
    void trustedTemplateProviderMustMatchExactlyOnceAndSigningMustMatchActiveTrust() throws Exception {
        var artifacts = new FileSystemArtifactRepository(temp.resolve("artifacts"));
        var runs = new FileSystemRunEvidenceRepository(temp.resolve("runs"));
        var pair = RecordKeys.generate();
        RecordSigning signing = new RecordSigning("serve-test",
                new RecordSigner("mount-key", pair.getPrivate()));
        var authoring = authoring(artifacts, runs, trust("serve-test", pair.getPublic()));
        var options = new ProtoMoltServe.WorkflowPreparationOptions(temp.resolve("intents"), "starter-v1");

        assertThatThrownBy(() -> WorkflowPreparationMount.prepare(options, authoring, null, List.of(provider())))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("requires receipt signing");
        assertThatThrownBy(() -> WorkflowPreparationMount.prepare(options, authoring, signing, List.of()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("provider is absent");
        assertThatThrownBy(() -> WorkflowPreparationMount.prepare(options, authoring, signing,
                List.of(provider(), provider())))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("duplicate");

        var mounted = WorkflowPreparationMount.prepare(options, authoring, signing, List.of(provider()));
        assertThat(mounted.operations(new InMemoryTranscriptRepository())).isNotNull();

        KeyPair wrongPair = RecordKeys.generate();
        RecordSigning wrongSigner = new RecordSigning("serve-test",
                new RecordSigner("mount-key", wrongPair.getPrivate()));
        var wrong = WorkflowPreparationMount.prepare(options, authoring, wrongSigner, List.of(provider()));
        assertThatThrownBy(() -> wrong.operations(new InMemoryTranscriptRepository()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("signing identity is not trusted");
    }

    @Test
    void preparationOpenApiCarriesRuntimeOnlyBytesAndCelContract() {
        var service = WorkflowPreparationServiceOuterClass.getDescriptor()
                .findServiceByName("WorkflowPreparationService");
        var method = service.findMethodByName("PrepareWorkflowCandidate");
        var registry = new ProtoRestMethodRegistry();
        registry.register(service, method, request -> request, null);
        Map<String, Object> document = new ProtoOpenApiGenerator().generate(registry);
        Map<?, ?> paths = (Map<?, ?>) document.get("paths");
        assertThat(paths.toString()).contains(
                "/grpc-json/WorkflowPreparationService/PrepareWorkflowCandidate");

        Map<?, ?> components = (Map<?, ?>) document.get("components");
        Map<?, ?> schemas = (Map<?, ?>) components.get("schemas");
        String requestType = method.getInputType().getFullName().replace('.', '_');
        String responseType = method.getOutputType().getFullName().replace('.', '_');
        Map<?, ?> request = (Map<?, ?>) schemas.get(requestType);
        Map<?, ?> properties = (Map<?, ?>) request.get("properties");
        Map<?, ?> source = (Map<?, ?>) properties.get("executableSourceJson");
        assertThat(source.get("type")).isEqualTo("string");
        assertThat(source.get("format")).isEqualTo("byte");
        assertThat(source.get("x-protomolt-runtime-rules").toString()).contains("bytes");
        Map<?, ?> response = (Map<?, ?>) schemas.get(responseType);
        assertThat(response.get("x-protomolt-cel")).isNotNull();
    }

    @Test
    void hostPublishesPreparationDescriptorOnlyWhenConfigured() throws Exception {
        var method = ProtoMoltServe.class.getDeclaredMethod("contributedServices",
                boolean.class, boolean.class, boolean.class);
        method.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<ServiceDescriptor> disabled = (List<ServiceDescriptor>) method.invoke(null, false, true, false);
        @SuppressWarnings("unchecked")
        List<ServiceDescriptor> enabled = (List<ServiceDescriptor>) method.invoke(null, false, true, true);
        assertThat(disabled).noneMatch(service -> service.getFullName()
                .equals("ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationService"));
        assertThat(enabled).anyMatch(service -> service.getFullName()
                .equals("ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationService"));
    }

    private WorkflowAuthoringMount.Prepared authoring(ArtifactRepository artifacts,
            FileSystemRunEvidenceRepository runs, TrustSnapshot trust) throws Exception {
        ArtifactReference descriptors = artifacts.save(new byte[]{1}, "application/x-protobuf", false);
        ArtifactReference input = artifacts.save(new byte[]{2}, "application/x-protobuf", false);
        ArtifactReference output = artifacts.save(new byte[]{3}, "application/x-protobuf", false);
        var policy = WorkflowAuthoringPolicy.newBuilder().setDescriptors(descriptors)
                .addFixtures(WorkflowAcceptanceFixture.newBuilder().setName("mount-fixture")
                        .setInput(input).setExpectedOutput(output))
                .addPermittedCalls(WorkflowPermittedCall.newBuilder()
                        .setTarget("fixture:9090").setMethod("fixture.v1.Echo/Run")).build();
        ArtifactReference reference = artifacts.save(policy.toByteArray(), "application/x-protobuf", false);
        return WorkflowAuthoringMount.prepare(new ProtoMoltServe.WorkflowAuthoringOptions(
                        reference.getSha256(), temp.resolve("launch-ledger")), artifacts, runs,
                proxy(WorkflowVersionRepository.class), proxy(WorkflowRunStore.class),
                ActionContext.create(), new WorkflowRunner(), 3, () -> trust);
    }

    private static WorkflowSourceTemplateProvider provider() {
        return new WorkflowSourceTemplateProvider() {
            @Override public String id() { return "starter-v1"; }
            @Override public WorkflowCandidatePreparer.SourceTemplate template() {
                return (workflow, source, policy) -> {};
            }
        };
    }

    private static TrustSnapshot trust(String issuer, java.security.PublicKey publicKey) {
        return TrustSnapshot.newBuilder().addIssuers(TrustedIssuer.newBuilder().setIssuer(issuer)
                .addSubjectKinds(WorkRecords.SUBJECT_KIND_WORKFLOW_RUN)
                .addKeys(TrustedKey.newBuilder().setKeyId("mount-key")
                        .setAlgorithm(SignatureAlgorithm.SIGNATURE_ALGORITHM_ED25519)
                        .setState(KeyState.KEY_STATE_ACTIVE)
                        .setPublicKey(ByteString.copyFrom(RecordKeys.rawPublicKey(publicKey)))))
                .build();
    }

    private static <T> T proxy(Class<T> type) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
                (ignored, method, arguments) -> {
                    Class<?> result = method.getReturnType();
                    if (!result.isPrimitive()) return null;
                    if (result == boolean.class) return false;
                    if (result == int.class) return 0;
                    if (result == long.class) return 0L;
                    return 0;
                }));
    }
}
