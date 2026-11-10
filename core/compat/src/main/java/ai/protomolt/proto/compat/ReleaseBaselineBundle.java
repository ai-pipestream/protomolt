package ai.protomolt.proto.compat;

import ai.protomolt.proto.descriptors.ApiStability;
import ai.protomolt.proto.descriptors.StabilityTier;

import java.util.Objects;

/**
 * Complete five-surface release baseline bundle combining the {@link ReleaseBaselineManifest}
 * with snapshots of Java APIs, protobuf contracts, stored formats, REST routes, and action
 * envelopes.
 *
 * @param manifest        baseline metadata and reviewed waivers
 * @param javaApi         Java API surface snapshot
 * @param protobuf        protobuf contract surface snapshot
 * @param storedFormats   retained stored format snapshot
 * @param restRoutes      HTTP/REST route contract snapshot
 * @param actionEnvelopes action catalog verb envelope snapshot
 */
@ApiStability(StabilityTier.STABLE)
public record ReleaseBaselineBundle(
        ReleaseBaselineManifest manifest,
        JavaApiSnapshot javaApi,
        ProtobufSurfaceSnapshot protobuf,
        StoredFormatSnapshot storedFormats,
        RestRouteSnapshot restRoutes,
        ActionEnvelopeSnapshot actionEnvelopes) {

    public ReleaseBaselineBundle {
        Objects.requireNonNull(manifest, "manifest");
        Objects.requireNonNull(javaApi, "javaApi");
        Objects.requireNonNull(protobuf, "protobuf");
        Objects.requireNonNull(storedFormats, "storedFormats");
        Objects.requireNonNull(restRoutes, "restRoutes");
        Objects.requireNonNull(actionEnvelopes, "actionEnvelopes");
    }
}
