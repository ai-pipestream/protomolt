package ai.protomolt.proto.schema.apicurio.deployment;

import ai.protomolt.proto.schema.apicurio.ApicurioDescriptorInstaller;
import ai.protomolt.proto.schema.apicurio.ApicurioDescriptorLoaderProducer;
import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.FeatureBuildItem;

/** Quarkus deployment processor for the Apicurio descriptor extension. */
public class ProtoToolsApicurioProcessor {

    private static final String FEATURE = "protomolt-schema-apicurio";

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(FEATURE);
    }

    @BuildStep
    AdditionalBeanBuildItem registerBeans() {
        return AdditionalBeanBuildItem.builder()
                .addBeanClasses(
                        ApicurioDescriptorLoaderProducer.class,
                        ApicurioDescriptorInstaller.class)
                .setUnremovable()
                .build();
    }
}
