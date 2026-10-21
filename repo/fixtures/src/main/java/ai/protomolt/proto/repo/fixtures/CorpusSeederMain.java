package ai.protomolt.proto.repo.fixtures;

import ai.protomolt.proto.repo.accession.RepositoryCorpus;
import ai.protomolt.proto.repo.service.RepoServiceConfig;
import ai.protomolt.proto.repo.service.RepoServices;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The dev corpus seeder: a one-shot process that writes the fixture corpus into
 * the repository a compose dev profile runs. It reads the same
 * {@code DOCUMENT_PLATFORM_*} configuration as the repository host and builds
 * its own {@link RepoServices} composition over the same ledger and object
 * store, so the saves go through the real admission, validation and catalog
 * code under the corpus's stepped clock.
 *
 * <p>It runs only where {@link #ENV_PROFILE} is exactly {@code dev}: the
 * compose dev profile sets it, nothing else does, and without it the process
 * exits before touching the store. {@link #ENV_SEED} and {@link #ENV_LARGE_BLOB_BYTES}
 * pick the corpus parameters; unset means the defaults.
 */
public final class CorpusSeederMain {

    /** Must be exactly {@code dev}; the seeder refuses to run otherwise. */
    public static final String ENV_PROFILE = "PROTOMOLT_CORPUS_SEED";

    /** The corpus seed; unset means {@link RepositoryCorpus#DEFAULT_SEED}. */
    public static final String ENV_SEED = "PROTOMOLT_CORPUS_SEED_VALUE";

    /** The large blob's length in bytes; unset means {@link RepositoryCorpus#DEFAULT_LARGE_BLOB_BYTES}. */
    public static final String ENV_LARGE_BLOB_BYTES = "PROTOMOLT_CORPUS_LARGE_BLOB_BYTES";

    private static final Logger LOG = LoggerFactory.getLogger(CorpusSeederMain.class);

    private CorpusSeederMain() {}

    /**
     * Seeds and exits: 0 when the corpus was written or was already there, 2 when
     * the profile gate or the configuration refuses, 1 when the store refused.
     *
     * @param args ignored; configuration is environment driven
     */
    public static void main(String[] args) {
        System.exit(run(System.getenv()));
    }

    /** The main body, for tests: returns the exit code instead of exiting. */
    static int run(Map<String, String> environment) {
        if (!"dev".equals(environment.get(ENV_PROFILE))) {
            LOG.error("{} is not 'dev'; the corpus seeder runs only in the dev compose profile", ENV_PROFILE);
            return 2;
        }
        RepositoryCorpus.Parameters parameters;
        RepoServiceConfig config;
        try {
            parameters = parameters(environment);
            config = RepoServiceConfig.fromEnvironment(environment);
        } catch (IllegalArgumentException refused) {
            LOG.error("Corpus seeder configuration refused: {}", refused.getMessage());
            return 2;
        }
        var corpus = RepositoryCorpus.build(parameters);
        try (var services = RepoServices.build(config)) {
            var report = CorpusSeeder.seed(services, corpus);
            if (report.written()) {
                LOG.info("Corpus {} (seed {}) written: {} saves, {} deletes, {} archive entries across {}",
                        corpus.digest(), parameters.seed(), report.saves(), report.deletes(), report.archiveEntries(),
                        RepositoryCorpus.ACCOUNTS);
            } else {
                LOG.info("Corpus already present in {}; nothing written", report.decisions().keySet());
            }
            return 0;
        } catch (CorpusAccountGuard.ForeignDocuments refused) {
            LOG.error("Corpus seeder refused: {}", refused.getMessage());
            return 1;
        } catch (RuntimeException failure) {
            LOG.error("Corpus seeder failed", failure);
            return 1;
        }
    }

    static RepositoryCorpus.Parameters parameters(Map<String, String> environment) {
        long seed = RepositoryCorpus.DEFAULT_SEED;
        long largeBlobBytes = RepositoryCorpus.DEFAULT_LARGE_BLOB_BYTES;
        var seedText = environment.get(ENV_SEED);
        if (seedText != null && !seedText.isBlank()) seed = parse(ENV_SEED, seedText);
        var lengthText = environment.get(ENV_LARGE_BLOB_BYTES);
        if (lengthText != null && !lengthText.isBlank()) largeBlobBytes = parse(ENV_LARGE_BLOB_BYTES, lengthText);
        return new RepositoryCorpus.Parameters(seed, largeBlobBytes);
    }

    private static long parse(String name, String text) {
        try {
            return Long.parseLong(text.trim());
        } catch (NumberFormatException malformed) {
            throw new IllegalArgumentException(name + " must be an integer, not '" + text + "'");
        }
    }
}
