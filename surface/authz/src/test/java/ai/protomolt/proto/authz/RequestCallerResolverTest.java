package ai.protomolt.proto.authz;

import ai.protomolt.proto.actions.Caller;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class RequestCallerResolverTest {
    private static final String OPERATOR = "synthetic-operator-token";
    private static final String SCOPED = "synthetic-scoped-token";
    private static final AuthenticatedCaller BOUND = new AuthenticatedCaller(
            Caller.scoped("console-user", Set.of()),
            Optional.of(new CredentialBinding("test-authority", UUID.randomUUID(), 1)));
    private static final AuthenticatedCallerResolver POLICY =
            token -> SCOPED.equals(token) ? Optional.of(BOUND) : Optional.empty();

    private static RequestHeaders headers(Map<String, List<String>> values) {
        return RequestHeaders.of(values);
    }

    @Test void theOperatorTokenRunsWithProcessAuthorityFromEitherHeader() {
        var bearer = RequestCallerResolver.bearer(OPERATOR, POLICY);

        assertThat(bearer.resolve(headers(Map.of("api_token", List.of(OPERATOR)))))
                .contains(AuthenticatedCaller.unbound(Caller.operator()));
        assertThat(bearer.resolve(headers(Map.of("Authorization", List.of("bEaReR " + OPERATOR + " ")))))
                .contains(AuthenticatedCaller.unbound(Caller.operator()));
    }

    @Test void anyOtherCredentialGoesToTheResolverWithItsBindingKept() {
        var bearer = RequestCallerResolver.bearer(OPERATOR, POLICY);

        assertThat(bearer.resolve(headers(Map.of("API_TOKEN", List.of(SCOPED))))).containsSame(BOUND);
        assertThat(bearer.resolve(headers(Map.of("authorization", List.of("Bearer " + SCOPED)))))
                .containsSame(BOUND);
        assertThat(bearer.resolve(headers(Map.of("api_token", List.of("unknown-token"))))).isEmpty();
    }

    @Test void theApiTokenHeaderWinsOverAuthorizationAsOnTheGrpcSurface() {
        var bearer = RequestCallerResolver.bearer(OPERATOR, POLICY);

        assertThat(bearer.resolve(headers(Map.of(
                "api_token", List.of(SCOPED),
                "Authorization", List.of("Bearer " + OPERATOR))))).containsSame(BOUND);
    }

    @Test void missingBlankAmbiguousOrNonBearerCredentialsResolveEmpty() {
        var bearer = RequestCallerResolver.bearer(OPERATOR, POLICY);

        assertThat(bearer.resolve(headers(Map.of()))).isEmpty();
        assertThat(bearer.resolve(headers(Map.of("api_token", List.of("  "))))).isEmpty();
        assertThat(bearer.resolve(headers(Map.of("Authorization", List.of("Bearer  "))))).isEmpty();
        assertThat(bearer.resolve(headers(Map.of("Authorization", List.of("Basic " + OPERATOR))))).isEmpty();
        assertThat(bearer.resolve(headers(Map.of("api_token", List.of(OPERATOR, OPERATOR))))).isEmpty();
        assertThat(bearer.resolve(headers(Map.of("Api_Token", List.of(OPERATOR), "api_token", List.of(SCOPED)))))
                .isEmpty();
        assertThat(bearer.resolve(headers(Map.of("Authorization",
                List.of("Bearer " + OPERATOR, "Bearer " + OPERATOR))))).isEmpty();
    }

    @Test void theOperatorOnlyFormKnowsNoOtherCredential() {
        var operatorOnly = RequestCallerResolver.bearer(OPERATOR);

        assertThat(operatorOnly.resolve(headers(Map.of("api_token", List.of(OPERATOR)))))
                .contains(AuthenticatedCaller.unbound(Caller.operator()));
        assertThat(operatorOnly.resolve(headers(Map.of("api_token", List.of(SCOPED))))).isEmpty();
    }

    @Test void aStoreOutagePropagatesInsteadOfReadingAsUnauthenticated() {
        var outage = new IllegalStateException("synthetic-idp-outage");
        var bearer = RequestCallerResolver.bearer(OPERATOR, token -> { throw outage; });

        assertThatThrownBy(() -> bearer.resolve(headers(Map.of("api_token", List.of(SCOPED)))))
                .isSameAs(outage);
        assertThat(bearer.resolve(headers(Map.of("api_token", List.of(OPERATOR)))))
                .contains(AuthenticatedCaller.unbound(Caller.operator()));
    }

    @Test void bearerRefusesAMissingOrBlankOperatorTokenAndAMissingResolver() {
        assertThatThrownBy(() -> RequestCallerResolver.bearer(null, POLICY))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("operatorToken");
        assertThatThrownBy(() -> RequestCallerResolver.bearer(" ", POLICY))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("operatorToken");
        assertThatThrownBy(() -> RequestCallerResolver.bearer(OPERATOR, null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("resolver");
        assertThatThrownBy(() -> RequestCallerResolver.bearer(" "))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("operatorToken");
    }

    @Test void firstOfAnswersTheFirstPresentCallerInOrder() {
        var asked = new AtomicInteger();
        RequestCallerResolver session = request -> {
            asked.incrementAndGet();
            return request.values("cookie").isEmpty() ? Optional.empty() : Optional.of(BOUND);
        };
        var composed = RequestCallerResolver.firstOf(List.of(
                RequestCallerResolver.bearer(OPERATOR, POLICY), session,
                request -> { throw new AssertionError("Must not continue after a match"); }));

        assertThat(composed.resolve(headers(Map.of("Cookie", List.of("session=x"))))).containsSame(BOUND);
        assertThat(asked).hasValue(1);
        assertThat(composed.resolve(headers(Map.of("api_token", List.of(OPERATOR)))))
                .contains(AuthenticatedCaller.unbound(Caller.operator()));
        assertThat(asked).hasValue(1);
    }

    @Test void firstOfIsEmptyOnlyWhenEveryResolverIs() {
        var composed = RequestCallerResolver.firstOf(List.of(
                request -> Optional.empty(), request -> Optional.empty()));

        assertThat(composed.resolve(headers(Map.of("api_token", List.of(SCOPED))))).isEmpty();
    }

    @Test void firstOfStopsAtAStoreOutage() {
        var outage = new IllegalStateException("synthetic-session-store-outage");
        var composed = RequestCallerResolver.firstOf(List.of(
                request -> { throw outage; },
                request -> { throw new AssertionError("Must not fall through an outage"); }));

        assertThatThrownBy(() -> composed.resolve(headers(Map.of()))).isSameAs(outage);
    }

    @Test void aBearerOutageFailsClosedEvenWhenAValidSessionCookieFollows() {
        var outage = new IllegalStateException("synthetic-credential-store-outage");
        var sessionAsked = new AtomicInteger();
        RequestCallerResolver session = request -> {
            sessionAsked.incrementAndGet();
            return request.values("cookie").isEmpty() ? Optional.empty() : Optional.of(BOUND);
        };
        var composed = RequestCallerResolver.firstOf(List.of(
                RequestCallerResolver.bearer(OPERATOR, token -> { throw outage; }), session));

        // Both credentials present: a non-operator Bearer whose store is away refuses
        // the request as an outage instead of falling through, so a cookie cannot
        // route around a credential check that could not run.
        assertThatThrownBy(() -> composed.resolve(headers(Map.of(
                "Authorization", List.of("Bearer " + SCOPED),
                "Cookie", List.of("session=x")))))
                .isSameAs(outage);
        assertThat(sessionAsked).hasValue(0);

        // The same cookie without the Bearer resolves through the session store.
        assertThat(composed.resolve(headers(Map.of("Cookie", List.of("session=x"))))).containsSame(BOUND);
        assertThat(sessionAsked).hasValue(1);
    }

    @Test void firstOfRefusesAnEmptyOrNullList() {
        assertThatThrownBy(() -> RequestCallerResolver.firstOf(List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> RequestCallerResolver.firstOf(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test void headerNamesMatchCaseInsensitivelyAndAbsentHeadersReadEmpty() {
        var view = RequestHeaders.of(Map.of("X-Forwarded-For", List.of("a", "b")));

        assertThat(view.values("x-forwarded-for")).containsExactly("a", "b");
        assertThat(view.values("X-FORWARDED-FOR")).containsExactly("a", "b");
        assertThat(view.values("cookie")).isEmpty();
        assertThat(view.first("x-forwarded-for")).contains("a");
        assertThat(view.first("cookie")).isEmpty();
        assertThatThrownBy(() -> view.values("x-forwarded-for").add("c"))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
