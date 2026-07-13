package ai.pipestream.proto.quarkus;

import ai.pipestream.proto.cel.CelEvaluator;
import ai.pipestream.proto.descriptors.DescriptorLoader;
import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.json.ProtobufJsonTranscoder;
import ai.pipestream.proto.mapper.ProtoFieldMapper;
import ai.pipestream.proto.rest.ApiTokenRequirement;
import ai.pipestream.proto.rest.ProtoApiTokenValidator;
import ai.pipestream.proto.rest.ProtoRestGateway;
import ai.pipestream.proto.rest.ProtoRestMethod;
import ai.pipestream.proto.rest.ProtoRestMethodRegistry;
import ai.pipestream.proto.rest.UnauthorizedProtoRestException;
import ai.pipestream.proto.server.ProtoToolsServerConfig;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.quarkus.arc.DefaultBean;
import jakarta.enterprise.inject.Produces;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProtoToolsProducerTest {

    @Test
    void producesCoreBeansWithNullExtraLoaders() {
        ProtoToolsProducer producer = new ProtoToolsProducer();
        DescriptorRegistry registry = producer.descriptorRegistry(null);
        assertThat(registry).isNotNull();

        ProtoFieldMapper mapper = producer.protoFieldMapper(registry);
        CelEvaluator cel = producer.celEvaluator();
        ProtobufJsonTranscoder transcoder = producer.protobufJsonTranscoder(registry);
        ProtoRestMethodRegistry methods = producer.protoRestMethodRegistry();
        ProtoRestGateway gateway = producer.protoRestGateway(
                methods, transcoder, producer.protoApiTokenValidator());

        assertThat(mapper).isNotNull();
        assertThat(cel).isNotNull();
        assertThat(transcoder).isNotNull();
        assertThat(gateway).isNotNull();

        methods.register(ProtoRestMethod.builder("EchoService", "Echo", request -> {
                    Struct in = (Struct) request;
                    String name = in.getFieldsOrDefault("name", Value.getDefaultInstance()).getStringValue();
                    return Struct.newBuilder()
                            .putFields("message", Value.newBuilder().setStringValue("hello " + name).build())
                            .build();
                })
                .requestType(Struct.class)
                .build());
        assertThat(gateway.invoke("EchoService", "Echo", "{\"name\":\"Ada\"}")).contains("hello Ada");
    }

    @Test
    void defaultTokenValidatorFailsClosed() {
        ProtoToolsProducer producer = new ProtoToolsProducer();
        ProtoRestMethodRegistry methods = producer.protoRestMethodRegistry();
        ProtoRestGateway gateway = producer.protoRestGateway(
                methods,
                producer.protobufJsonTranscoder(producer.descriptorRegistry(null)),
                producer.protoApiTokenValidator());

        methods.register(ProtoRestMethod.builder("SecureService", "Op", request -> Struct.getDefaultInstance())
                .requestType(Struct.class)
                .apiToken(ApiTokenRequirement.apiKeyHeader("x-api-key"))
                .build());

        // Even a present, non-blank token must be rejected by the fail-closed default.
        assertThatThrownBy(() -> gateway.invoke("SecureService", "Op", "{}",
                Map.of("x-api-key", "junk-token"), Map.of()))
                .isInstanceOf(UnauthorizedProtoRestException.class);
    }

    @Test
    void allProducersAreOverridableDefaultBeans() throws Exception {
        for (Method method : ProtoToolsProducer.class.getDeclaredMethods()) {
            if (!method.isAnnotationPresent(Produces.class)) {
                continue;
            }
            assertThat(method.isAnnotationPresent(DefaultBean.class))
                    .as("producer %s must be @DefaultBean so applications can override it",
                            method.getName())
                    .isTrue();
        }
        assertThat(new ProtoToolsProducer().protoToolsServerConfig())
                .isEqualTo(ProtoToolsServerConfig.defaults());
    }

    @Test
    void registryDeduplicatesLoaderInstancesAddedTwice() throws Exception {
        AtomicInteger loads = new AtomicInteger();
        DescriptorLoader countingLoader = new DescriptorLoader() {
            @Override
            public List<FileDescriptor> loadDescriptors() {
                loads.incrementAndGet();
                return List.of();
            }

            @Override
            public FileDescriptor loadDescriptor(String name) {
                return null;
            }

            @Override
            public boolean isAvailable() {
                return true;
            }

            @Override
            public String getLoaderType() {
                return "counting";
            }
        };

        DescriptorRegistry registry = new ProtoToolsProducer().descriptorRegistry(null);
        registry.addLoader(countingLoader);
        // Simulates an extension installer re-adding the same loader bean at startup.
        registry.addLoader(countingLoader);
        registry.autoLoadDescriptors();

        assertThat(loads.get())
                .as("a loader registered twice must only be consulted once")
                .isEqualTo(1);
    }
}
