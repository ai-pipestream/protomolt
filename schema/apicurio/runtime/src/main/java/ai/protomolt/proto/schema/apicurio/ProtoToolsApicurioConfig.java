package ai.protomolt.proto.schema.apicurio;

import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

import java.util.Optional;

/**
 * Runtime configuration for the proto-tools Apicurio descriptor extension.
 *
 * <pre>
 * protomolt.apicurio.enabled=true
 * protomolt.apicurio.registry-url=http://localhost:8080/apis/registry/v3
 * protomolt.apicurio.group-id=default
 * protomolt.apicurio.auto-load-on-startup=false
 * </pre>
 */
@ConfigMapping(prefix = "protomolt.apicurio")
@ConfigRoot(phase = ConfigPhase.RUN_TIME)
public interface ProtoToolsApicurioConfig {

    /** Whether to enable the Apicurio descriptor loader. */
    @WithDefault("true")
    boolean enabled();

    /**
     * Apicurio Registry URL. When unset, falls back to
     * {@code apicurio.registry.url} / Kafka connector properties.
     */
    Optional<String> registryUrl();

    /** Group ID used when searching for PROTOBUF artifacts. */
    @WithDefault("default")
    String groupId();

    /**
     * When true, bulk-load all PROTOBUF artifacts from the group at startup.
     * When false, resolve on demand.
     */
    @WithDefault("false")
    boolean autoLoadOnStartup();
}
