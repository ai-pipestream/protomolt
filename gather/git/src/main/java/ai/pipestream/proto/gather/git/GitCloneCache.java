package ai.pipestream.proto.gather.git;

import ai.pipestream.proto.gather.GatherException;

import org.eclipse.jgit.api.CloneCommand;
import org.eclipse.jgit.api.FetchCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.ResetCommand;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.RefSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Persistent clone cache: clone into the cache directory on first use, {@code fetch} and hard
 * reset to the requested ref on reuse. Concurrent access is serialized with a JVM lock plus a
 * sibling {@code <cache>.lock} file lock, so parallel builds sharing a cache do not corrupt it.
 *
 * <p>Offline behavior: with {@code offline=true} the cached checkout is used as-is and a cold
 * cache fails; with {@code offline=false} a failed fetch over a warm cache logs a warning and
 * resets to the requested ref from the cached refs, failing when it is not among them.</p>
 */
final class GitCloneCache {

    private static final Logger LOG = LoggerFactory.getLogger(GitCloneCache.class);
    private static final ConcurrentMap<Path, ReentrantLock> JVM_LOCKS = new ConcurrentHashMap<>();

    private GitCloneCache() {
    }

    /** Returns the cache directory holding a checkout of {@code ref}. */
    static Path ensureCheckout(Path cacheDir, String repoUrl, String ref,
                               CredentialsProvider credentials, boolean offline) throws GatherException {
        Path lockFile = cacheDir.toAbsolutePath().normalize()
                .resolveSibling(cacheDir.getFileName() + ".lock");
        ReentrantLock jvmLock = JVM_LOCKS.computeIfAbsent(lockFile, path -> new ReentrantLock());
        jvmLock.lock();
        try {
            Files.createDirectories(lockFile.getParent());
            try (FileChannel channel = FileChannel.open(lockFile,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                    FileLock ignored = channel.lock()) {
                return checkoutLocked(cacheDir, repoUrl, ref, credentials, offline);
            }
        } catch (IOException e) {
            throw new GatherException("Failed locking git cache " + cacheDir, e);
        } finally {
            jvmLock.unlock();
        }
    }

    private static Path checkoutLocked(Path cacheDir, String repoUrl, String ref,
                                       CredentialsProvider credentials, boolean offline)
            throws GatherException {
        if (offline) {
            if (Files.isDirectory(cacheDir)) {
                return cacheDir;
            }
            throw new GatherException("Offline and no cached checkout for " + repoUrl + "@" + ref
                    + " (cache: " + cacheDir + ")");
        }

        if (!Files.isDirectory(cacheDir)) {
            cloneRepo(repoUrl, credentials, cacheDir);
            resetTo(repoUrl, ref, cacheDir);
            return cacheDir;
        }

        try {
            fetch(repoUrl, credentials, cacheDir);
        } catch (Exception e) {
            // Fall through to the reset regardless: the cached refs may already hold the
            // requested ref, and if they do not, resetTo fails rather than serving another.
            LOG.warn("Failed fetching {} — falling back to the refs cached in {}",
                    repoUrl, cacheDir, e);
        }
        resetTo(repoUrl, ref, cacheDir);
        return cacheDir;
    }

    private static void cloneRepo(String repoUrl, CredentialsProvider credentials, Path cacheDir)
            throws GatherException {
        try {
            CloneCommand clone = Git.cloneRepository()
                    .setURI(repoUrl)
                    .setDirectory(cacheDir.toFile());
            if (credentials != null) {
                clone.setCredentialsProvider(credentials);
            }
            clone.call().close();
        } catch (Exception e) {
            throw new GatherException("Failed cloning git repo " + repoUrl, e);
        }
    }

    private static void fetch(String repoUrl, CredentialsProvider credentials, Path cacheDir)
            throws Exception {
        try (Git git = Git.open(cacheDir.toFile())) {
            // Always fetch heads and tags, even for commit-SHA refs: fetch-by-SHA requires
            // uploadpack.allowReachableSHA1InWant, which the big hosts leave off.
            FetchCommand fetch = git.fetch()
                    .setRemote("origin")
                    .setRefSpecs(
                            new RefSpec("+refs/heads/*:refs/remotes/origin/*"),
                            new RefSpec("+refs/tags/*:refs/tags/*"));
            if (credentials != null) {
                fetch.setCredentialsProvider(credentials);
            }
            fetch.call();
        }
    }

    /** Hard-resets the checkout to {@code ref} — a branch, a tag, or a commit SHA. */
    private static void resetTo(String repoUrl, String ref, Path cacheDir) throws GatherException {
        try (Git git = Git.open(cacheDir.toFile())) {
            ObjectId commit = resolveCommit(git, ref);
            if (commit == null) {
                throw new GatherException("Could not resolve ref " + ref + " on " + repoUrl);
            }
            git.reset()
                    .setMode(ResetCommand.ResetType.HARD)
                    .setRef(commit.getName())
                    .call();
        } catch (GatherException e) {
            throw e;
        } catch (Exception e) {
            throw new GatherException("Failed checking out " + repoUrl + "@" + ref, e);
        }
    }

    private static ObjectId resolveCommit(Git git, String ref) throws IOException {
        for (String candidate : new String[]{"refs/remotes/origin/" + ref, "refs/tags/" + ref, ref}) {
            ObjectId commit = git.getRepository().resolve(candidate + "^{commit}");
            if (commit != null) {
                return commit;
            }
        }
        return null;
    }
}
