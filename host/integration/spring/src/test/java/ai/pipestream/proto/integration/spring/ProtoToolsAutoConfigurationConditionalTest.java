package ai.pipestream.proto.integration.spring;

import ai.pipestream.proto.cel.CelEvaluator;
import ai.pipestream.proto.descriptors.DescriptorLoader;
import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.http.json.ProtobufJsonTranscoder;
import ai.pipestream.proto.mapper.ProtoFieldMapper;
import ai.pipestream.proto.http.rest.ProtoApiTokenValidator;
import ai.pipestream.proto.http.rest.ProtoRestGateway;
import ai.pipestream.proto.http.rest.ProtoRestMethodRegistry;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.Ordered;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers the conditional wiring that {@link ProtoToolsAutoConfigurationTest} cannot reach with a
 * full classpath: the {@code @ConditionalOnClass} guards and the ordering of contributed
 * {@link DescriptorLoader} beans.
 */
class ProtoToolsAutoConfigurationConditionalTest {

    @Test
    void restBeansBackOffWhenProtoRestGatewayIsAbsent() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ProtoToolsAutoConfiguration.class))
                .withClassLoader(new FilteredClassLoader(ProtoRestGateway.class))
                .run(ctx -> {
                    assertThat(ctx).doesNotHaveBean(ProtoRestGateway.class);
                    assertThat(ctx).doesNotHaveBean(ProtoRestMethodRegistry.class);
                    assertThat(ctx).doesNotHaveBean(ProtoApiTokenValidator.class);

                    // The non-REST beans are unguarded and must still be present.
                    assertThat(ctx).hasSingleBean(DescriptorRegistry.class);
                    assertThat(ctx).hasSingleBean(ProtoFieldMapper.class);
                    assertThat(ctx).hasSingleBean(ProtobufJsonTranscoder.class);
                });
    }

    @Test
    void celEvaluatorBacksOffWhenCelIsAbsent() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ProtoToolsAutoConfiguration.class))
                .withClassLoader(new FilteredClassLoader(CelEvaluator.class))
                .run(ctx -> {
                    assertThat(ctx).doesNotHaveBean(CelEvaluator.class);
                    assertThat(ctx).hasSingleBean(DescriptorRegistry.class);
                    assertThat(ctx).hasSingleBean(ProtoRestGateway.class);
                });
    }

    @Test
    void loaderBeansAreAddedToTheRegistryInOrder() {
        List<String> consulted = new CopyOnWriteArrayList<>();
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ProtoToolsAutoConfiguration.class))
                .withBean("lateLoader", DescriptorLoader.class,
                        () -> new OrderedRecordingLoader("late", 2, consulted))
                .withBean("earlyLoader", DescriptorLoader.class,
                        () -> new OrderedRecordingLoader("early", 1, consulted))
                .run(ctx -> {
                    ctx.getBean(DescriptorRegistry.class).autoLoadDescriptors();
                    assertThat(consulted)
                            .as("descriptorRegistry must use orderedStream() so @Order is honored")
                            .containsSubsequence("early", "late");
                });
    }

    private static final class OrderedRecordingLoader implements DescriptorLoader, Ordered {
        private final String name;
        private final int order;
        private final List<String> consulted;

        private OrderedRecordingLoader(String name, int order, List<String> consulted) {
            this.name = name;
            this.order = order;
            this.consulted = consulted;
        }

        @Override
        public int getOrder() {
            return order;
        }

        @Override
        public List<FileDescriptor> loadDescriptors() {
            consulted.add(name);
            return List.of();
        }

        @Override
        public FileDescriptor loadDescriptor(String fileName) {
            return null;
        }

        @Override
        public boolean isAvailable() {
            return true;
        }

        @Override
        public String getLoaderType() {
            return name;
        }
    }
}
