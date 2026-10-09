package ai.protomolt.proto.authz;

import ai.protomolt.proto.actions.Caller;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class AuthenticatedCallerResolverTest {
    private static final Caller PRINCIPAL = Caller.scoped("same-principal", Set.of());
    private static final CredentialBinding KEY = new CredentialBinding("test-authority", UUID.randomUUID(), 3);
    private static final AuthenticatedCaller BOUND = new AuthenticatedCaller(PRINCIPAL, Optional.of(KEY));

    @Test void chainPreservesTheFirstCompleteResultAndResolvesOnce() {
        var lookups = new AtomicInteger();
        AuthenticatedCallerResolver resolver = token -> {
            lookups.incrementAndGet();
            return Optional.of(BOUND);
        };
        var chain = CallerResolver.chain(List.of(token -> Optional.empty(), resolver,
                token -> { throw new AssertionError("Must not continue after a match"); }));
        assertThat(chain.resolveAuthenticated("synthetic-token")).containsSame(BOUND);
        assertThat(lookups).hasValue(1);
        assertThat(chain.resolve("synthetic-token")).contains(PRINCIPAL);
        assertThat(lookups).hasValue(2);
    }

    @Test void unboundMatchCannotBeUpgradedByALaterResolver() {
        CallerResolver legacy = token -> Optional.of(PRINCIPAL);
        var chain = CallerResolver.chain(List.of(legacy,
                (AuthenticatedCallerResolver) token -> { throw new AssertionError("Must not upgrade a match"); }));
        assertThat(chain.resolveAuthenticated("synthetic-token")).contains(AuthenticatedCaller.unbound(PRINCIPAL));
    }

    @Test void storeFailureStopsResolutionWithoutFallingThrough() {
        var outage = new IllegalStateException("synthetic-store-outage");
        var chain = CallerResolver.chain(List.of(token -> { throw outage; },
                (AuthenticatedCallerResolver) token -> { throw new AssertionError("Must not resolve after outage"); }));
        assertThatThrownBy(() -> chain.resolveAuthenticated("synthetic-token")).isSameAs(outage);
        assertThatThrownBy(() -> chain.resolve("synthetic-token")).isSameAs(outage);
    }

    @Test void bindingsRequireValidOpaqueIdentityAndScopedAuthority() {
        assertThatThrownBy(() -> new CredentialBinding(" ", UUID.randomUUID(), 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CredentialBinding("bad\nissuer", UUID.randomUUID(), 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CredentialBinding("x".repeat(129), UUID.randomUUID(), 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CredentialBinding("issuer", null, 1)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new CredentialBinding("issuer", UUID.randomUUID(), 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AuthenticatedCaller(Caller.operator(), Optional.of(KEY))).isInstanceOf(IllegalArgumentException.class);
        assertThat(AuthenticatedCaller.unbound(Caller.operator()).binding()).isEmpty();
    }
}
