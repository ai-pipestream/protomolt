package ai.protomolt.proto.authz.grpc;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.authz.AuthenticatedCaller;
import ai.protomolt.proto.authz.AuthenticatedCallerResolver;
import ai.protomolt.proto.authz.CallerResolver;
import ai.protomolt.proto.authz.CredentialBinding;
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/** Real gRPC calls inspect trusted authentication context, without repository/storage substitutes. */
class AuthenticatedCallerContextTest {
    @Test void twoKeysOfOnePrincipalRemainDistinctAndHeadersCannotSupplyIdentity() throws Exception {
        var principal = Caller.scoped("same-principal", Set.of());
        var keys = Map.of("synthetic-key-a", new CredentialBinding("test-authority", UUID.randomUUID(), 1),
                "synthetic-key-b", new CredentialBinding("test-authority", UUID.randomUUID(), 2));
        var lookups = new AtomicInteger();
        AuthenticatedCallerResolver identified = token -> {
            lookups.incrementAndGet();
            return Optional.ofNullable(keys.get(token)).map(key -> new AuthenticatedCaller(principal, Optional.of(key)));
        };
        CallerResolver legacy = token -> "synthetic-legacy".equals(token) ? Optional.of(principal) : Optional.empty();
        var resolver = CallerResolver.chain(List.of(identified, legacy));
        var observed = new ConcurrentHashMap<String, AuthenticatedCaller>();
        String name = InProcessServerBuilder.generateName();
        var server = InProcessServerBuilder.forName(name).addService(new HealthGrpc.HealthImplBase() {
            @Override public void check(HealthCheckRequest request, StreamObserver<HealthCheckResponse> response) {
                observed.put(request.getService(), CallerContexts.authentication().orElseThrow());
                response.onNext(HealthCheckResponse.newBuilder().setStatus(HealthCheckResponse.ServingStatus.SERVING).build());
                response.onCompleted();
            }
        }).intercept(new ApiTokenServerInterceptor("synthetic-operator", resolver)).build().start();
        var channel = InProcessChannelBuilder.forName(name).build();
        try {
            for (String token : List.of("synthetic-key-a", "synthetic-key-b", "synthetic-legacy", "synthetic-operator")) {
                var headers = new Metadata();
                headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), token);
                headers.put(Metadata.Key.of("credential-id", Metadata.ASCII_STRING_MARSHALLER), "forged-id");
                HealthGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS)
                        .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers))
                        .check(HealthCheckRequest.newBuilder().setService(token).build());
            }
            assertThat(lookups).hasValue(3); // Operator is resolved locally; each other call resolves once.
            for (var key : keys.entrySet()) {
                assertThat(observed.get(key.getKey()).caller()).isEqualTo(principal);
                assertThat(observed.get(key.getKey()).binding()).contains(key.getValue());
            }
            assertThat(observed.get("synthetic-legacy").binding()).isEmpty();
            assertThat(observed.get("synthetic-operator")).isEqualTo(AuthenticatedCaller.unbound(Caller.operator()));
            assertThat(CallerContexts.authentication()).isEmpty();
            assertThatThrownBy(() -> HealthGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS)
                    .check(HealthCheckRequest.newBuilder().setService("unauthenticated").build()))
                    .isInstanceOfSatisfying(StatusRuntimeException.class,
                            failure -> assertThat(failure.getStatus().getCode()).isEqualTo(io.grpc.Status.Code.UNAUTHENTICATED));
            assertThat(observed).doesNotContainKey("unauthenticated");
        } finally {
            channel.shutdownNow(); server.shutdownNow();
            assertThat(channel.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            assertThat(server.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test void mismatchedLegacyAndCompleteContextsCannotExposeABinding() {
        var caller = Caller.scoped("first", Set.of());
        var auth = new AuthenticatedCaller(caller, Optional.of(new CredentialBinding("issuer", UUID.randomUUID(), 1)));
        Context.current().withValues(CallerContexts.CALLER, Caller.scoped("second", Set.of()),
                CallerContexts.AUTHENTICATED_CALLER, auth).run(() ->
                assertThatThrownBy(CallerContexts::authentication).isInstanceOf(IllegalStateException.class));
        Context.current().withValue(CallerContexts.CALLER, caller).run(() ->
                assertThat(CallerContexts.authentication()).isEmpty());
    }
}
