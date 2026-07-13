package ai.pipestream.proto.schema.apicurio;

import ai.pipestream.proto.descriptors.DescriptorLoader;
import ai.pipestream.proto.descriptors.DescriptorRegistry;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wires {@link ApicurioDescriptorLoader} into any available {@link DescriptorRegistry}
 * and optionally bulk-loads descriptors at startup.
 *
 * <p>When the proteus-quarkus extension supplies the {@link DescriptorRegistry}, that registry
 * has already collected every available {@link DescriptorLoader} bean (including this
 * extension's loader) and its {@code addLoader} is idempotent per loader instance, so the
 * {@code addLoader} call below is a harmless no-op there; against app-provided registries it
 * performs the actual registration.</p>
 */
@ApplicationScoped
public class ApicurioDescriptorInstaller {

    private static final Logger LOG = LoggerFactory.getLogger(ApicurioDescriptorInstaller.class);

    void onStart(
            @Observes StartupEvent event,
            Instance<DescriptorRegistry> registries,
            Instance<DescriptorLoader> loaders,
            ProtoToolsApicurioConfig config) {
        if (!config.enabled() || registries.isUnsatisfied()) {
            return;
        }
        DescriptorRegistry registry = registries.get();
        for (DescriptorLoader loader : loaders) {
            if (!(loader instanceof ApicurioDescriptorLoader apicurio) || !apicurio.isAvailable()) {
                continue;
            }
            registry.addLoader(apicurio);
            LOG.info("Registered {} with DescriptorRegistry (group={})",
                    apicurio.getLoaderType(), apicurio.getGroupId());
            if (config.autoLoadOnStartup()) {
                try {
                    int count = registry.loadFrom(apicurio);
                    LOG.info("Auto-loaded {} descriptors from Apicurio", count);
                } catch (DescriptorLoader.DescriptorLoadException e) {
                    LOG.warn("Apicurio auto-load failed: {}", e.getMessage());
                }
            }
        }
    }
}
