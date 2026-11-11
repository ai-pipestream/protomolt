package ai.protomolt.proto.http.rest;

import ai.protomolt.proto.descriptors.ApiStability;
import ai.protomolt.proto.descriptors.StabilityTier;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProtoRestAnnotationRegistrarTest {

    @ProtoRestExposed(summary = "Demo echo service")
    @ProtoApiToken(name = "api_token")
    static final class EchoService {
        @ProtoRestExposed(summary = "Echo a name")
        public Struct echo(Struct request) {
            String name = request.getFieldsOrDefault("name", Value.getDefaultInstance()).getStringValue();
            return Struct.newBuilder()
                    .putFields("message", Value.newBuilder().setStringValue("hello " + name).build())
                    .build();
        }

        public Struct ignored(Struct request) {
            return request;
        }
    }

    @ProtoRestExposed(httpMethods = {"PUT"})
    static final class UpdateService {
        @ProtoRestExposed
        public Struct update(Struct request) {
            return request;
        }

        @ProtoRestExposed(httpMethods = {"PATCH"})
        public Struct patch(Struct request) {
            return request;
        }
    }

    static final class PlainService {
        @ProtoRestExposed
        public Struct go(Struct request) {
            return request;
        }
    }

    @Test
    void registersAnnotatedMethodsOnly() {
        ProtoRestMethodRegistry registry = new ProtoRestMethodRegistry();
        List<ProtoRestMethod> registered = new ProtoRestAnnotationRegistrar(registry)
                .register(new EchoService());

        assertThat(registered).hasSize(1);
        ProtoRestMethod method = registered.getFirst();
        assertThat(method.serviceName()).isEqualTo("Echo");
        assertThat(method.methodName()).isEqualTo("echo");
        assertThat(method.apiToken()).isPresent();
        assertThat(method.summary()).contains("Echo a name");

        assertThat(registry.find("Echo", "echo")).isPresent();
        assertThat(registry.find("Echo", "ignored")).isEmpty();
    }

    @Test
    void inheritsTypeLevelHttpMethodsUnlessMethodOverrides() {
        ProtoRestMethodRegistry registry = new ProtoRestMethodRegistry();
        ProtoRestAnnotationRegistrar registrar = new ProtoRestAnnotationRegistrar(registry);
        registrar.register(new UpdateService());
        registrar.register(new PlainService());

        assertThat(registry.find("Update", "update").orElseThrow().httpMethods())
                .containsExactly("PUT");
        assertThat(registry.find("Update", "update").orElseThrow().allowedHttpVerbs())
                .containsExactly("PUT");
        assertThat(registry.find("Update", "patch").orElseThrow().httpMethods())
                .containsExactly("PATCH");
        // No declared verbs: the declaration stays empty and the gateway allows POST
        // only - the same default the OpenAPI document declares.
        assertThat(registry.find("Plain", "go").orElseThrow().httpMethods()).isEmpty();
        assertThat(registry.find("Plain", "go").orElseThrow().allowedHttpVerbs())
                .isEqualTo(ProtoRestMethod.DEFAULT_HTTP_VERBS)
                .containsExactly("POST");
    }

    static final class CustomPathService {
        @ProtoRestExposed(path = "/fancy/route")
        public Struct go(Struct request) {
            return request;
        }
    }

    @Test
    void methodLevelCustomPathsAreRejectedAtStartup() {
        // Hosts route only {service}/{method}; a per-method path would be published in
        // the OpenAPI contract but 404 at runtime. Fail loudly instead.
        ProtoRestMethodRegistry registry = new ProtoRestMethodRegistry();
        assertThatThrownBy(() ->
                        new ProtoRestAnnotationRegistrar(registry).register(new CustomPathService()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not supported");
    }

    @ApiStability(StabilityTier.EXPERIMENTAL)
    static final class ExperimentalService {
        @ProtoRestExposed
        public Struct inheritedExperimental(Struct request) {
            return request;
        }

        @ProtoRestExposed(stability = StabilityTier.STABLE)
        public Struct promotedStable(Struct request) {
            return request;
        }
    }

    static final class MultiStabilityService {
        @ProtoRestExposed(stability = {StabilityTier.STABLE, StabilityTier.EXPERIMENTAL})
        public Struct invalid(Struct request) {
            return request;
        }
    }

    @Test
    void explicitStableMethodOverridesExperimentalClassWhileUnsetInherits() {
        ProtoRestMethodRegistry registry = new ProtoRestMethodRegistry();
        new ProtoRestAnnotationRegistrar(registry).register(new ExperimentalService());

        assertThat(registry.find("Experimental", "inheritedExperimental").orElseThrow().stability())
                .isEqualTo(StabilityTier.EXPERIMENTAL);
        assertThat(registry.find("Experimental", "promotedStable").orElseThrow().stability())
                .isEqualTo(StabilityTier.STABLE);
    }

    @Test
    void rejectsMultipleStabilityTiersOnProtoRestExposed() {
        ProtoRestMethodRegistry registry = new ProtoRestMethodRegistry();
        assertThatThrownBy(() ->
                        new ProtoRestAnnotationRegistrar(registry).register(new MultiStabilityService()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at most one StabilityTier");
    }
}
