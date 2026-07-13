package ai.pipestream.proto.spring;

import ai.pipestream.proto.cel.CelEvaluator;
import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.json.ProtobufJsonTranscoder;
import ai.pipestream.proto.mapper.ProtoFieldMapper;
import ai.pipestream.proto.rest.ApiTokenRequirement;
import ai.pipestream.proto.rest.ProtoApiTokenValidator;
import ai.pipestream.proto.rest.ProtoRestGateway;
import ai.pipestream.proto.rest.ProtoRestMethod;
import ai.pipestream.proto.rest.ProtoRestMethodRegistry;
import ai.pipestream.proto.rest.UnauthorizedProtoRestException;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProtoToolsAutoConfigurationTest {

    @Test
    void wiresCoreBeansAndGateway() {
        try (AnnotationConfigApplicationContext ctx =
                     new AnnotationConfigApplicationContext(ProtoToolsAutoConfiguration.class)) {
            assertThat(ctx.getBean(DescriptorRegistry.class)).isNotNull();
            assertThat(ctx.getBean(ProtoFieldMapper.class)).isNotNull();
            assertThat(ctx.getBean(CelEvaluator.class)).isNotNull();
            assertThat(ctx.getBean(ProtobufJsonTranscoder.class)).isNotNull();

            ProtoRestMethodRegistry registry = ctx.getBean(ProtoRestMethodRegistry.class);
            registry.register(ProtoRestMethod.builder("EchoService", "Echo", request -> {
                        Struct in = (Struct) request;
                        String name = in.getFieldsOrDefault("name", Value.getDefaultInstance()).getStringValue();
                        return Struct.newBuilder()
                                .putFields("message", Value.newBuilder().setStringValue("hello " + name).build())
                                .build();
                    })
                    .requestType(Struct.class)
                    .build());

            String json = ctx.getBean(ProtoRestGateway.class)
                    .invoke("EchoService", "Echo", "{\"name\":\"Ada\"}");
            assertThat(json).contains("hello Ada");
        }
    }

    @Test
    void defaultGatewayFailsClosedForTokenProtectedMethods() {
        try (AnnotationConfigApplicationContext ctx =
                     new AnnotationConfigApplicationContext(ProtoToolsAutoConfiguration.class)) {
            ctx.getBean(ProtoRestMethodRegistry.class).register(
                    ProtoRestMethod.builder("SecureService", "Op", request -> Struct.getDefaultInstance())
                            .requestType(Struct.class)
                            .apiToken(ApiTokenRequirement.apiKeyHeader("x-api-key"))
                            .build());

            // Even a present, non-blank token must be rejected by the fail-closed default.
            assertThatThrownBy(() -> ctx.getBean(ProtoRestGateway.class).invoke(
                    "SecureService", "Op", "{}", Map.of("x-api-key", "junk-token"), Map.of()))
                    .isInstanceOf(UnauthorizedProtoRestException.class);
        }
    }

    @Test
    void isARealAutoConfigurationWithImportsFile() throws Exception {
        assertThat(ProtoToolsAutoConfiguration.class.isAnnotationPresent(AutoConfiguration.class))
                .isTrue();
        try (InputStream imports = getClass().getClassLoader().getResourceAsStream(
                "META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports")) {
            assertThat(imports)
                    .as("auto-configuration imports file must be on the classpath")
                    .isNotNull();
            assertThat(new String(imports.readAllBytes(), StandardCharsets.UTF_8))
                    .contains(ProtoToolsAutoConfiguration.class.getName());
        }
    }

    @Test
    void autoConfigurationRegistersAllBeans() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ProtoToolsAutoConfiguration.class))
                .run(ctx -> {
                    assertThat(ctx).hasSingleBean(DescriptorRegistry.class);
                    assertThat(ctx).hasSingleBean(ProtoFieldMapper.class);
                    assertThat(ctx).hasSingleBean(CelEvaluator.class);
                    assertThat(ctx).hasSingleBean(ProtobufJsonTranscoder.class);
                    assertThat(ctx).hasSingleBean(ProtoRestMethodRegistry.class);
                    assertThat(ctx).hasSingleBean(ProtoApiTokenValidator.class);
                    assertThat(ctx).hasSingleBean(ProtoRestGateway.class);
                });
    }

    @Test
    void applicationBeansBackOffTheDefaults() {
        CelEvaluator customEvaluator = new CelEvaluator();
        ProtoApiTokenValidator customValidator = (tokenConfig, headers, queryParams) -> Optional.empty();
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ProtoToolsAutoConfiguration.class))
                .withBean("customCelEvaluator", CelEvaluator.class, () -> customEvaluator)
                .withBean("customValidator", ProtoApiTokenValidator.class, () -> customValidator)
                .run(ctx -> {
                    assertThat(ctx.getBean(CelEvaluator.class)).isSameAs(customEvaluator);
                    assertThat(ctx.getBean(ProtoApiTokenValidator.class)).isSameAs(customValidator);

                    // The gateway is wired with the overriding validator: the accept-all
                    // custom validator lets a token-protected call through.
                    ctx.getBean(ProtoRestMethodRegistry.class).register(
                            ProtoRestMethod.builder("SecureService", "Op",
                                            request -> Struct.getDefaultInstance())
                                    .requestType(Struct.class)
                                    .apiToken(ApiTokenRequirement.apiKeyHeader("x-api-key"))
                                    .build());
                    assertThat(ctx.getBean(ProtoRestGateway.class).invoke(
                            "SecureService", "Op", "{}", Map.of("x-api-key", "any"), Map.of()))
                            .isNotNull();
                });
    }
}
