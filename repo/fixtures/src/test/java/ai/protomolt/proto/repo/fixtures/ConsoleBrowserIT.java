package ai.protomolt.proto.repo.fixtures;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.authz.AccountBinding;
import ai.protomolt.proto.authz.AccountBindingResolver;
import ai.protomolt.proto.authz.AccountBindings;
import ai.protomolt.proto.authz.AclIdentity;
import ai.protomolt.proto.authz.OidcConsoleSessions;
import ai.protomolt.proto.authz.OidcConsoleSettings;
import ai.protomolt.proto.repo.accession.RepositoryCorpus;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.service.ConsoleSignInHandler;
import ai.protomolt.proto.repo.service.RepoServiceConfig;
import ai.protomolt.proto.repo.service.RepoServices;
import ai.protomolt.proto.repo.service.RepositoryConsole;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The console's browser leg: Playwright drives the built console bundle in Chromium against a
 * real repository host. The host is the production composition on PostgreSQL and LocalStack S3
 * with the fixture corpus seeded through {@link CorpusSeeder}, the Connect gateway and the
 * upload route on its HTTP port, and console sign-in through Keycloak with an imported realm.
 * The browser signs in through Keycloak's own login page and holds nothing but the session
 * cookie. The specs live in {@code apps/console/e2e}; this suite boots the host, runs
 * {@code playwright test} against it and fails when any spec fails.
 *
 * <p>Opt-in: {@code PROTOMOLT_CONSOLE_BROWSER_IT=1}. The suite also needs the console built
 * ({@code npm ci && npm run build} in {@code apps/console}, which installs Playwright) and
 * Playwright's Chromium ({@code npx playwright install chromium}); a missing one fails the
 * suite by name. Gradle never runs npm.
 */
@Testcontainers
class ConsoleBrowserIT {

    /** The Keycloak user the specs sign in as. */
    static final String USER = "analyst";
    static final String PASSWORD = "analyst-password";

    private static final String OPERATOR = "console-browser-operator";
    private static final String REALM = "protomolt";
    private static final String CLIENT_ID = "protomolt-console";
    private static final String CLIENT_SECRET = "console-secret";
    private static final Duration PLAYWRIGHT_TIMEOUT = Duration.ofMinutes(10);

    /** The repository HTTP port, chosen before Keycloak starts: the realm pins the callback to it. */
    private static final int HTTP_PORT = freePort();

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");

    @Container
    static final GenericContainer<?> KEYCLOAK = new GenericContainer<>("quay.io/keycloak/keycloak:26.4")
            .withCommand("start-dev", "--import-realm")
            .withEnv("KC_BOOTSTRAP_ADMIN_USERNAME", "admin")
            .withEnv("KC_BOOTSTRAP_ADMIN_PASSWORD", "admin-password")
            .withCopyToContainer(Transferable.of(realm(callback())), "/opt/keycloak/data/import/" + REALM + "-realm.json")
            .withExposedPorts(8080)
            .waitingFor(Wait.forHttp("/realms/" + REALM + "/.well-known/openid-configuration")
                    .forPort(8080).withStartupTimeout(Duration.ofMinutes(4)));

    static RepoServices services;
    static Path consoleDir;

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /** The host's own address as the browser sees it: the loopback IP, the same one the realm's callback names. */
    private static String base() {
        return "http://127.0.0.1:" + HTTP_PORT;
    }

    private static String callback() {
        return base() + ConsoleSignInHandler.PATH + "/callback";
    }

    private static String issuer() {
        return "http://127.0.0.1:" + KEYCLOAK.getMappedPort(8080) + "/realms/" + REALM;
    }

    private static String realm(String callbackUri) {
        try (InputStream in = ConsoleBrowserIT.class.getResourceAsStream("console-browser-realm.json")) {
            return new String(Objects.requireNonNull(in, "console-browser-realm.json").readAllBytes(),
                    StandardCharsets.UTF_8).replace("${CALLBACK}", callbackUri);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /** The analyst reads every corpus account through its user principal name. */
    private static AccountBindings bindings() {
        return AccountBindings.newBuilder()
                .addBindings(AccountBinding.newBuilder().setPrincipal(USER)
                        .addAllAccountIds(RepositoryCorpus.ACCOUNTS)
                        .addAclIdentities(AclIdentity.newBuilder()
                                .setIdentityType("user-principal-name").setIdentity(RepositoryCorpus.ANALYST_UPN)))
                .build();
    }

    @BeforeAll
    static void boot() {
        String dir = System.getProperty("protomolt.console.dir");
        assertThat(dir).as("system property protomolt.console.dir (set by the integrationTest task)").isNotBlank();
        consoleDir = Path.of(dir);
        assertThat(consoleDir.resolve("dist/index.html"))
                .as("the built console bundle; run npm ci && npm run build in apps/console").exists();
        assertThat(playwright())
                .as("Playwright in apps/console/node_modules; run npm ci in apps/console").exists();

        var config = new RepoServiceConfig(0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                LOCALSTACK.getEndpoint().toString(), LOCALSTACK.getRegion(), LOCALSTACK.getAccessKey(),
                LOCALSTACK.getSecretKey(), "console-browser", HTTP_PORT, null, null, 0, 0L);
        services = RepoServices.build(config).withAccountBindings(AccountBindingResolver.of(bindings()));
        var report = CorpusSeeder.seed(services, RepositoryCorpus.build());
        assertThat(report.written()).isTrue();

        var sessions = new OidcConsoleSessions(new OidcConsoleSettings(URI.create(issuer()), CLIENT_ID, CLIENT_SECRET,
                URI.create(callback()), Duration.ofHours(1)));
        services.startHttp(HTTP_PORT, OPERATOR, RepositoryConsole.directory(consoleDir.resolve("dist")),
                services.consoleSignInRoutes(Optional.of(sessions)),
                RepoServices.requestCallers(OPERATOR, null, Optional.of(sessions)));
    }

    @AfterAll
    static void tearDown() {
        if (services != null) services.close();
    }

    private static Path playwright() {
        return consoleDir.resolve("node_modules/.bin/playwright");
    }

    @Test
    void theConsoleSpecsPassAgainstTheSeededHost() throws Exception {
        var command = new ProcessBuilder(List.of(playwright().toString(), "test"))
                .directory(consoleDir.toFile())
                .inheritIO();
        command.environment().put("CONSOLE_BASE_URL", base());
        command.environment().put("CONSOLE_USER", USER);
        command.environment().put("CONSOLE_PASSWORD", PASSWORD);
        command.environment().put("CONSOLE_OPERATOR_TOKEN", OPERATOR);
        command.environment().put("CI", "1");
        Process run = command.start();
        boolean finished = run.waitFor(PLAYWRIGHT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        if (!finished) {
            run.destroyForcibly();
        }
        assertThat(finished).as("playwright test finished within " + PLAYWRIGHT_TIMEOUT).isTrue();
        assertThat(run.exitValue()).as("playwright test exit code; the report is in apps/console/playwright-report")
                .isZero();
    }
}
