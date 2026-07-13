package ai.pipestream.proto.schema.apicurio;

import ai.pipestream.proto.descriptors.DescriptorLoader;
import io.apicurio.registry.client.RegistryClientFactory;
import io.apicurio.registry.client.common.RegistryClientOptions;
import io.apicurio.registry.rest.client.RegistryClient;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CDI producers for Apicurio-backed {@link DescriptorLoader}s.
 *
 * <p>Exactly one {@link RegistryClient} (with a dedicated, daemon-threaded Vert.x transport)
 * exists per application, held by the {@code @Singleton} {@link ApicurioRegistryClientHolder};
 * its transport is closed on shutdown via {@code @Disposes}. The nullable
 * {@code @Dependent}-scoped {@link RegistryClient} producer hands out that shared instance,
 * so injection points do not each get a fresh, never-closed client.</p>
 */
@ApplicationScoped
public class ApicurioDescriptorLoaderProducer {

    private static final Logger LOG = LoggerFactory.getLogger(ApicurioDescriptorLoaderProducer.class);

    /**
     * Application-wide owner of the registry client and the Vert.x instance backing its
     * HTTP transport. {@code client()} is {@code null} when the loader is disabled or no
     * registry URL is configured.
     */
    public static final class ApicurioRegistryClientHolder implements AutoCloseable {
        private final RegistryClient client;
        private final Vertx vertx;

        ApicurioRegistryClientHolder(RegistryClient client, Vertx vertx) {
            this.client = client;
            this.vertx = vertx;
        }

        public RegistryClient client() {
            return client;
        }

        /** Closes the owned Vert.x transport (idempotent, safe when nothing was created). */
        @Override
        public void close() {
            if (vertx != null) {
                vertx.close();
            }
        }
    }

    /**
     * {@code @Singleton} (never null): when the loader is disabled or unconfigured the holder
     * simply carries no client. Creating the client here (rather than per injection point)
     * gives it a single owned transport that the disposer below can close.
     */
    @Produces
    @Singleton
    public ApicurioRegistryClientHolder registryClientHolder(ProtoToolsApicurioConfig config) {
        if (!config.enabled()) {
            return new ApicurioRegistryClientHolder(null, null);
        }

        String url = config.registryUrl().orElseGet(this::resolveRegistryUrl);
        if (url == null || url.isBlank()) {
            LOG.warn("No Apicurio Registry URL found. Descriptor loading from Apicurio is disabled.");
            return new ApicurioRegistryClientHolder(null, null);
        }

        LOG.info("Creating Apicurio Registry client for URL: {}", url);
        // Own the Vert.x instance so the transport has a deterministic lifecycle (the SDK's
        // fallback is a shared static instance that is never closed). Daemon threads so an
        // unclosed instance can never block JVM shutdown.
        Vertx vertx = Vertx.vertx(new VertxOptions().setUseDaemonThread(true));
        try {
            RegistryClient client = RegistryClientFactory.create(RegistryClientOptions.create(url, vertx));
            return new ApicurioRegistryClientHolder(client, vertx);
        } catch (RuntimeException e) {
            vertx.close();
            throw e;
        }
    }

    /** Closes the client transport when the application shuts down. */
    void closeRegistryClientHolder(@Disposes ApicurioRegistryClientHolder holder) {
        LOG.debug("Closing Apicurio Registry client transport");
        holder.close();
    }

    /**
     * {@code @Dependent} so a {@code null} product is legal when the loader is disabled or
     * no registry URL is configured; consumers must treat the client as optional. All
     * injection points share the holder's single client instance.
     */
    @Produces
    @Dependent
    public RegistryClient produceRegistryClient(ApicurioRegistryClientHolder holder) {
        return holder.client();
    }

    /**
     * Never returns {@code null} (forbidden for non-{@code @Dependent} producers): when the
     * loader is disabled or unconfigured, produces an unavailable loader so the extension
     * degrades gracefully instead of failing injection.
     */
    @Produces
    @Singleton
    public DescriptorLoader produceApicurioDescriptorLoader(
            RegistryClient client, ProtoToolsApicurioConfig config) {
        String groupId = config.groupId();
        if (client == null || !config.enabled()) {
            LOG.warn("Apicurio descriptor loading is disabled or unconfigured; "
                    + "producing an unavailable loader (group={})", groupId);
            return new ApicurioDescriptorLoader((RegistryClient) null, groupId);
        }

        LOG.info("Producing ApicurioDescriptorLoader for group: {}", groupId);
        return new ApicurioDescriptorLoader(client, groupId);
    }

    private String resolveRegistryUrl() {
        return ConfigProvider.getConfig()
                .getOptionalValue("mp.messaging.connector.smallrye-kafka.apicurio.registry.url", String.class)
                .or(() -> ConfigProvider.getConfig().getOptionalValue("apicurio.registry.url", String.class))
                .orElse(null);
    }
}
