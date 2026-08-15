package ai.pipestream.proto.platform;

import ai.pipestream.proto.intake.service.IntakeServiceMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Standalone entry point: the whole document platform from environment
 * variables. The repository family is {@code DOCUMENT_PLATFORM_*} as
 * repo-service documents it; the platform's own variables are listed on
 * {@link DocumentPlatformConfig}.
 *
 * <p>Key-store selection is {@link IntakeServiceMain#selectResolver}: OIDC
 * introspection over the JDBC store over the env-seeded table, both-set
 * rejected loudly. The duplicate that used to live here collapsed onto the
 * intake door's public helper when the ServiceModule SPI landed.
 */
public final class DocumentPlatformMain {

    /** Env var seeding the in-memory key store; see {@link IntakeServiceMain#ENV_KEYS}. */
    public static final String ENV_KEYS = IntakeServiceMain.ENV_KEYS;

    /** Env var naming the IdP's introspection endpoint; see {@link IntakeServiceMain#ENV_OIDC_URL}. */
    public static final String ENV_OIDC_URL = IntakeServiceMain.ENV_OIDC_URL;

    private static final Logger LOG = LoggerFactory.getLogger(DocumentPlatformMain.class);

    private DocumentPlatformMain() {
    }

    /**
     * Boots the platform from the environment and blocks until SIGTERM.
     *
     * @param args unused
     * @throws Exception when boot fails
     */
    public static void main(String[] args) throws Exception {
        DocumentPlatformConfig config = DocumentPlatformConfig.fromEnvironment();
        DocumentPlatform platform = DocumentPlatform.start(
                config, IntakeServiceMain.selectResolver(System.getenv()));
        Runtime.getRuntime().addShutdownHook(new Thread(platform::close, "platform-shutdown"));
        LOG.info("document platform serving; shut down with SIGTERM");
        Thread.currentThread().join();
    }
}
