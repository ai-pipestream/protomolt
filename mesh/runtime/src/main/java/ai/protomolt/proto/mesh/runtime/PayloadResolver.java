package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mesh.runtime.v1.TypedPayload;
import ai.protomolt.proto.mesh.v1.EntityEnvelope;
import com.google.protobuf.DynamicMessage;

/** Resolves an admitted inline payload or claim check to exact protobuf bytes. */
@FunctionalInterface
public interface PayloadResolver {

    DynamicMessage resolve(EntityEnvelope envelope);

    /** Resolver for deployments that accept inline protobuf payloads only. */
    static PayloadResolver inlineOnly(DescriptorRegistry registry) {
        return envelope -> {
            if (!envelope.hasPayload()) {
                throw new IllegalArgumentException("flow input "
                        + envelope.getHeader().getEntityId()
                        + " is a claim check but no payload resolver is configured");
            }
            return RuntimeSchemas.unpack(registry, TypedPayload.newBuilder()
                    .setSchema(envelope.getSchema())
                    .setPayload(envelope.getPayload())
                    .build());
        };
    }
}
