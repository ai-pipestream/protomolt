package ai.protomolt.proto.schema.apicurio.deployment;

import ai.protomolt.proto.schema.apicurio.ApicurioDescriptorInstaller;
import ai.protomolt.proto.schema.apicurio.ApicurioDescriptorLoaderProducer;
import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Wiring-contract tests for {@link ProtoToolsApicurioProcessor}: the extension's build steps
 * must stay Quarkus-visible and register exactly the runtime beans, or the extension silently
 * stops contributing its beans at augmentation time.
 */
class ProtoToolsApicurioProcessorWiringTest {

    private final ProtoToolsApicurioProcessor processor = new ProtoToolsApicurioProcessor();

    @Test
    void bothBuildStepsAreAnnotatedForQuarkus() throws Exception {
        Method feature = ProtoToolsApicurioProcessor.class.getDeclaredMethod("feature");
        Method registerBeans = ProtoToolsApicurioProcessor.class.getDeclaredMethod("registerBeans");
        assertThat(feature.isAnnotationPresent(BuildStep.class))
                .as("feature() must be a @BuildStep or the extension has no feature entry")
                .isTrue();
        assertThat(registerBeans.isAnnotationPresent(BuildStep.class))
                .as("registerBeans() must be a @BuildStep or no beans are registered")
                .isTrue();
    }

    @Test
    void featureNameMatchesTheRuntimeArtifact() {
        FeatureBuildItem feature = processor.feature();
        assertThat(feature.getName()).isEqualTo("protomolt-schema-apicurio");
    }

    @Test
    void registersExactlyTheRuntimeBeansAsUnremovable() {
        AdditionalBeanBuildItem beans = processor.registerBeans();
        assertThat(beans.getBeanClasses())
                .containsExactlyInAnyOrder(
                        ApicurioDescriptorLoaderProducer.class.getName(),
                        ApicurioDescriptorInstaller.class.getName());
        assertThat(beans.isRemovable())
                .as("beans must be unremovable: nothing injects them directly, so ArC would "
                        + "otherwise prune the producer and installer")
                .isFalse();
    }

    @Test
    void buildStepsProduceFreshEquivalentItemsOnEveryCall() {
        // Build steps must be pure producers: repeated calls yield equal-valued items.
        assertThat(processor.feature().getName()).isEqualTo(processor.feature().getName());
        assertThat(processor.registerBeans().getBeanClasses())
                .isEqualTo(processor.registerBeans().getBeanClasses());
    }
}
