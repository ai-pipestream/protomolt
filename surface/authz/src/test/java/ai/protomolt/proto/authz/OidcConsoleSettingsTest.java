package ai.protomolt.proto.authz;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The sign-in settings are all or none: none means no sign-in, a subset refuses naming
 * what is missing, and every value is checked by name.
 */
class OidcConsoleSettingsTest {

    private static Map<String, String> complete() {
        Map<String, String> env = new HashMap<>();
        env.put(OidcConsoleSettings.ENV_ISSUER, "https://idp.example.test/realms/protomolt");
        env.put(OidcConsoleSettings.ENV_CLIENT_ID, "protomolt-console");
        env.put(OidcConsoleSettings.ENV_CLIENT_SECRET, "console-secret");
        env.put(OidcConsoleSettings.ENV_REDIRECT_URI, "https://repo.example.test/console/session/callback");
        env.put(OidcConsoleSettings.ENV_SESSION_TTL, "PT8H");
        return env;
    }

    @Test
    void noVariableMeansNoSignIn() {
        assertThat(OidcConsoleSettings.fromEnvironment(Map.of())).isEmpty();
        assertThat(OidcConsoleSettings.fromEnvironment(Map.of(OidcConsoleSettings.ENV_ISSUER, " "))).isEmpty();
    }

    @Test
    void allFiveVariablesReadIntoTheSettings() {
        OidcConsoleSettings settings = OidcConsoleSettings.fromEnvironment(complete()).orElseThrow();
        assertThat(settings.issuer()).isEqualTo(URI.create("https://idp.example.test/realms/protomolt"));
        assertThat(settings.clientId()).isEqualTo("protomolt-console");
        assertThat(settings.clientSecret()).isEqualTo("console-secret");
        assertThat(settings.redirectUri())
                .isEqualTo(URI.create("https://repo.example.test/console/session/callback"));
        assertThat(settings.sessionTtl()).isEqualTo(Duration.ofHours(8));
    }

    @Test
    void aSubsetRefusesNamingWhatIsMissing() {
        Map<String, String> env = complete();
        env.remove(OidcConsoleSettings.ENV_CLIENT_SECRET);
        env.put(OidcConsoleSettings.ENV_SESSION_TTL, "");
        assertThatThrownBy(() -> OidcConsoleSettings.fromEnvironment(env))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(OidcConsoleSettings.ENV_CLIENT_SECRET)
                .hasMessageContaining(OidcConsoleSettings.ENV_SESSION_TTL)
                .hasMessageContaining("partly configured");
    }

    @Test
    void anUnusableValueRefusesByName() {
        Map<String, String> ttl = complete();
        ttl.put(OidcConsoleSettings.ENV_SESSION_TTL, "8 hours");
        assertThatThrownBy(() -> OidcConsoleSettings.fromEnvironment(ttl))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(OidcConsoleSettings.ENV_SESSION_TTL)
                .hasMessageContaining("ISO-8601");

        Map<String, String> tooLong = complete();
        tooLong.put(OidcConsoleSettings.ENV_SESSION_TTL, "P8D");
        assertThatThrownBy(() -> OidcConsoleSettings.fromEnvironment(tooLong))
                .hasMessageContaining("seven days");

        Map<String, String> issuer = complete();
        issuer.put(OidcConsoleSettings.ENV_ISSUER, "idp.example.test/realms/protomolt");
        assertThatThrownBy(() -> OidcConsoleSettings.fromEnvironment(issuer))
                .hasMessageContaining(OidcConsoleSettings.ENV_ISSUER)
                .hasMessageContaining("issuer must be an absolute http(s) URI");

        Map<String, String> redirect = complete();
        redirect.put(OidcConsoleSettings.ENV_REDIRECT_URI, "ftp://repo.example.test/callback");
        assertThatThrownBy(() -> OidcConsoleSettings.fromEnvironment(redirect))
                .hasMessageContaining("redirectUri must be an http(s) URI");
    }

    @Test
    void theIssuerCarriesNoQueryOrFragment() {
        assertThatThrownBy(() -> new OidcConsoleSettings(URI.create("https://idp.example.test/x?y=1"),
                "c", "s", URI.create("https://r.example.test/cb"), Duration.ofHours(1)))
                .hasMessageContaining("query or fragment");
    }
}
