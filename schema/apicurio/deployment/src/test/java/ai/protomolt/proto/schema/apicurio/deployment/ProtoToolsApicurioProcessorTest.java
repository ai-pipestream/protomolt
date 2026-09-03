package ai.protomolt.proto.schema.apicurio.deployment;

import ai.protomolt.proto.schema.apicurio.ApicurioDescriptorInstaller;
import ai.protomolt.proto.schema.apicurio.ApicurioDescriptorLoaderProducer;
import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ProtoToolsApicurioProcessorTest {

    @Test
    void registersFeatureAndUnremovableBeans() {
        ProtoToolsApicurioProcessor processor = new ProtoToolsApicurioProcessor();
        FeatureBuildItem feature = processor.feature();
        assertThat(feature.getName()).isEqualTo("protomolt-schema-apicurio");

        AdditionalBeanBuildItem beans = processor.registerBeans();
        assertThat(beans.isRemovable()).isFalse();
        assertThat(beans.getBeanClasses())
                .contains(
                        ApicurioDescriptorLoaderProducer.class.getName(),
                        ApicurioDescriptorInstaller.class.getName());
    }
}
