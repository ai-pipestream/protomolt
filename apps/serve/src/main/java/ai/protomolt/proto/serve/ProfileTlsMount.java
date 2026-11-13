package ai.protomolt.proto.serve;

import ai.protomolt.proto.grpc.workspace.FileProfileTlsResolver;
import ai.protomolt.proto.grpc.workspace.ProfileCredentialResolver;
import ai.protomolt.proto.grpc.workspace.ProfileTlsConfigs;
import java.io.IOException;
import java.nio.file.Path;

/**
 * Resolves outbound service-profile credentials for {@link ProtoMoltServe}: composes the
 * optional {@link CorrectionStarter} bearer resolver with an optional host-configured
 * {@link FileProfileTlsResolver} loaded from {@code --profile-tls-config} /
 * {@code PROTOMOLT_PROFILE_TLS_CONFIG}. When no TLS config is mounted, profiles naming
 * {@code trust_ref} or {@code client_certificate_ref} remain refused with
 * {@code unsupported-transport}.
 */
final class ProfileTlsMount {

    private ProfileTlsMount() {
    }

    static ProfileCredentialResolver resolve(Path profileTlsConfig, CorrectionStarter starter) {
        ProfileCredentialResolver bearer = starter == null ? null : starter.credentials();
        if (profileTlsConfig == null) {
            return bearer;
        }
        FileProfileTlsResolver tls;
        try {
            tls = ProfileTlsConfigs.loadResolver(profileTlsConfig);
        } catch (IOException e) {
            throw new IllegalStateException("failed to read profile TLS config at "
                    + profileTlsConfig + ": " + e.getMessage(), e);
        }
        return ProfileCredentialResolver.withTls(bearer, tls);
    }
}
