package ai.protomolt.proto.integration.quarkus.deployment;

import ai.protomolt.proto.integration.quarkus.ProtoToolsProducer;
import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

class ProtoToolsProcessorTest {

    @Test
    void registersFeatureAndReflection() {
        ProtoToolsProcessor processor = new ProtoToolsProcessor();
        FeatureBuildItem feature = processor.feature();
        assertThat(feature.getName()).isEqualTo("protomolt");

        ReflectiveClassBuildItem reflective = processor.registerProducerForReflection();
        assertThat(reflective.getClassNames()).contains(ProtoToolsProducer.class.getName());
    }

    @Test
    void registersProducerAsUnremovableBean() {
        AdditionalBeanBuildItem beans = new ProtoToolsProcessor().registerProducerBean();
        assertThat(beans.getBeanClasses()).containsExactly(ProtoToolsProducer.class.getName());
        assertThat(beans.isRemovable()).isFalse();
    }

    @Test
    void everyProcessorMethodIsAQuarkusBuildStep() {
        // Without @BuildStep Quarkus silently ignores the method and the extension never
        // registers its feature, beans, or reflection metadata.
        assertThat(ProtoToolsProcessor.class.getDeclaredMethods()).isNotEmpty();
        for (Method method : ProtoToolsProcessor.class.getDeclaredMethods()) {
            assertThat(method.isAnnotationPresent(BuildStep.class))
                    .as("processor method %s must be annotated @BuildStep", method.getName())
                    .isTrue();
        }
    }
}
