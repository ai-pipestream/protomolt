package ai.protomolt.proto.repo.service;

import io.grpc.ClientInterceptor;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;

/** Explicit upstream credential. Diagnostic output never includes its value. */
public final class RemoteRepositoryCredential {
    private final String token;

    public RemoteRepositoryCredential(String token) {
        if (token == null || token.isBlank())
            throw new IllegalArgumentException(RepoServiceConfig.ENV_REPO_API_TOKEN + " must be nonblank");
        this.token = token;
    }

    ClientInterceptor interceptor() {
        var headers = new Metadata();
        headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), token);
        return MetadataUtils.newAttachHeadersInterceptor(headers);
    }

    @Override
    public String toString() {
        return "RemoteRepositoryCredential[redacted]";
    }
}
