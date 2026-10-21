package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.authz.OidcConsoleSessions;
import ai.protomolt.proto.authz.OidcConsoleSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The repository host's console sign-in, as the environment names it: the five
 * {@link OidcConsoleSettings} variables, all or none. With all five the provider is
 * discovered at boot, so a wrong issuer fails the boot; with none the sign-in routes answer
 * 503 naming the variables and only bearer credentials reach the HTTP port's guarded routes.
 *
 * @param sessions the session store, or empty when sign-in is not configured
 */
record ConsoleSignIn(Optional<OidcConsoleSessions> sessions) {

    private static final Logger LOG = LoggerFactory.getLogger(ConsoleSignIn.class);

    ConsoleSignIn {
        Objects.requireNonNull(sessions, "sessions");
    }

    /** A host without sign-in. */
    static ConsoleSignIn none() {
        return new ConsoleSignIn(Optional.empty());
    }

    /**
     * Resolves the sign-in from {@code environment}; a partial or unusable configuration and
     * an unreachable or mismatched issuer refuse at boot.
     */
    static ConsoleSignIn fromEnvironment(Map<String, String> environment) {
        Optional<OidcConsoleSettings> settings = OidcConsoleSettings.fromEnvironment(environment);
        if (settings.isEmpty()) {
            return none();
        }
        OidcConsoleSessions sessions = new OidcConsoleSessions(settings.get());
        LOG.info("console sign-in mounted against {} as client '{}', sessions last {}",
                settings.get().issuer(), settings.get().clientId(), settings.get().sessionTtl());
        return new ConsoleSignIn(Optional.of(sessions));
    }

    /** Whether sign-in is configured. */
    boolean configured() {
        return sessions.isPresent();
    }
}
