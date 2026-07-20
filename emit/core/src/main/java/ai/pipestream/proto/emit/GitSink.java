package ai.pipestream.proto.emit;

import org.eclipse.jgit.api.CommitCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.revwalk.RevCommit;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Writes a bundle into a git repository's working tree and commits it — the delivery lane
 * for bundles that should carry history (an OKF knowledge bundle updated on every schema
 * change, generated docs beside the sources they describe). The repository is opened if it
 * exists and initialized if it does not, the same convention as the git-backed registry
 * store. Only the bundle's own paths are staged and committed, so unrelated working-tree or
 * index state is never swept into the commit; a bundle that changes none of its own paths
 * produces no commit, whatever else the repository is carrying.
 */
public final class GitSink implements BundleSink {

    private final Path repoDir;
    private final String prefix;
    private final String message;
    private final PersonIdent author;

    /** Commits at the repository root. */
    public GitSink(Path repoDir, String message) {
        this(repoDir, "", message, "protomolt-emit", "emit@protomolt.local");
    }

    /**
     * @param prefix directory within the repository the bundle lands under ("" for the root)
     */
    public GitSink(Path repoDir, String prefix, String message,
                   String authorName, String authorEmail) {
        this.repoDir = Objects.requireNonNull(repoDir, "repoDir").toAbsolutePath().normalize();
        this.prefix = normalizePrefix(prefix);
        this.message = Objects.requireNonNull(message, "message");
        this.author = new PersonIdent(authorName, authorEmail);
    }

    @Override
    public String write(Bundle bundle) throws IOException {
        try (Git git = openOrInit()) {
            Path workTree = git.getRepository().getWorkTree().toPath().toAbsolutePath().normalize();
            List<String> repoPaths = new ArrayList<>(bundle.size());
            for (String path : bundle.paths()) {
                String repoPath = prefix.isEmpty() ? path : prefix + "/" + path;
                Path target = workTree.resolve(repoPath).normalize();
                if (!target.startsWith(workTree)) {
                    throw new IOException("Bundle path escapes the repository: " + path);
                }
                Files.createDirectories(target.getParent());
                Files.write(target, bundle.file(path));
                git.add().addFilepattern(repoPath).call();
                repoPaths.add(repoPath);
            }
            // Both the decision and the commit are restricted to the bundle's own paths: the
            // repository may carry unrelated staged or modified files, and neither may trigger
            // a commit here nor be swept into one.
            if (Collections.disjoint(git.status().call().getUncommittedChanges(), repoPaths)) {
                if (git.getRepository().resolve("HEAD") == null) {
                    return "nothing to commit (empty repository)";
                }
                RevCommit head = git.log().setMaxCount(1).call().iterator().next();
                return head.getName() + " (unchanged)";
            }
            CommitCommand commit = git.commit()
                    .setMessage(message)
                    .setAuthor(author)
                    .setCommitter(author);
            repoPaths.forEach(commit::setOnly);
            return commit.call().getName();
        } catch (GitAPIException e) {
            throw new IOException("Git delivery failed: " + e.getMessage(), e);
        }
    }

    private Git openOrInit() throws IOException {
        try {
            if (Files.isDirectory(repoDir.resolve(".git"))) {
                return Git.open(repoDir.toFile());
            }
            Files.createDirectories(repoDir);
            return Git.init().setDirectory(repoDir.toFile()).call();
        } catch (GitAPIException e) {
            throw new IOException("Failed to open or initialize " + repoDir, e);
        }
    }

    private static String normalizePrefix(String prefix) {
        String cleaned = Objects.requireNonNull(prefix, "prefix").replace('\\', '/');
        while (cleaned.startsWith("/")) {
            cleaned = cleaned.substring(1);
        }
        while (cleaned.endsWith("/")) {
            cleaned = cleaned.substring(0, cleaned.length() - 1);
        }
        for (String segment : cleaned.isEmpty() ? new String[0] : cleaned.split("/")) {
            if (segment.isBlank() || segment.equals(".") || segment.equals("..")) {
                throw new IllegalArgumentException("Invalid prefix segment: " + prefix);
            }
        }
        return cleaned;
    }
}
