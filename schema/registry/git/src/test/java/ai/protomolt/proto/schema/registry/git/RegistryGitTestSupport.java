package ai.protomolt.proto.schema.registry.git;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.ResetCommand;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.FileMode;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.TreeWalk;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Shared JGit probes for the registry git tests: assertions run against the committed
 * HEAD tree (the authoritative state) and external commits are made with plain porcelain,
 * standing in for an outside actor that still uses a checked-out working tree.
 */
final class RegistryGitTestSupport {

    private RegistryGitTestSupport() {
    }

    /** Commits every working-tree change under the repository with one porcelain commit. */
    static void commitWorkingTree(Path repoDir, String message) throws Exception {
        try (Git git = Git.open(repoDir.toFile())) {
            git.add().addFilepattern(".").call();
            git.commit().setMessage(message).call();
        }
    }

    /**
     * Materializes HEAD into the index and working tree, like a fresh checkout by an
     * external actor: porcelain commits stage the index, and the registry store itself
     * never writes the working tree, so an outside commit must start from a checkout or
     * its tree would be missing everything committed so far.
     */
    static void checkoutHead(Path repoDir) throws Exception {
        try (Git git = Git.open(repoDir.toFile())) {
            git.reset().setMode(ResetCommand.ResetType.HARD).call();
        }
    }

    /** An empty commit on top of HEAD, like an external tool that bypasses the lock file. */
    static String commitEmptyExternally(Path repoDir, String message) throws Exception {
        checkoutHead(repoDir);
        try (Git git = Git.open(repoDir.toFile())) {
            return git.commit().setAllowEmpty(true).setMessage(message)
                    .setAuthor("External", "external@example.test")
                    .call().getName();
        }
    }

    /** The commit id HEAD names, or {@code null} while the branch is unborn. */
    static String head(Path repoDir) throws Exception {
        try (Git git = Git.open(repoDir.toFile())) {
            var head = git.getRepository().resolve(Constants.HEAD);
            return head == null ? null : head.getName();
        }
    }

    /** The bytes of one blob in HEAD's tree, or {@code null} when the path is absent. */
    static byte[] headBlob(Path repoDir, String path) throws Exception {
        try (Git git = Git.open(repoDir.toFile())) {
            ObjectId head = git.getRepository().resolve(Constants.HEAD);
            if (head == null) {
                return null;
            }
            try (RevWalk walk = new RevWalk(git.getRepository());
                    TreeWalk found = TreeWalk.forPath(git.getRepository(), path,
                            walk.parseCommit(head).getTree())) {
                return found == null ? null
                        : git.getRepository().open(found.getObjectId(0)).getBytes();
            }
        }
    }

    /** The mode of one blob entry in HEAD's tree, or {@code null} when the path is absent. */
    static FileMode headBlobMode(Path repoDir, String path) throws Exception {
        try (Git git = Git.open(repoDir.toFile())) {
            var head = git.getRepository().resolve(Constants.HEAD);
            if (head == null) {
                return null;
            }
            try (RevWalk walk = new RevWalk(git.getRepository());
                    TreeWalk found = TreeWalk.forPath(git.getRepository(), path,
                            walk.parseCommit(head).getTree())) {
                return found == null ? null : found.getFileMode(0);
            }
        }
    }

    /** Every repo-relative blob path of HEAD's tree, sorted. */
    static List<String> headBlobPaths(Path repoDir) throws Exception {
        try (Git git = Git.open(repoDir.toFile())) {
            var head = git.getRepository().resolve(Constants.HEAD);
            if (head == null) {
                return List.of();
            }
            List<String> paths = new ArrayList<>();
            try (RevWalk revWalk = new RevWalk(git.getRepository());
                    TreeWalk walk = new TreeWalk(git.getRepository())) {
                walk.addTree(revWalk.parseCommit(head).getTree());
                walk.setRecursive(true);
                while (walk.next()) {
                    paths.add(walk.getPathString());
                }
            }
            return paths.stream().sorted().toList();
        }
    }

    /** Every commit message from HEAD backward, newest first. */
    static List<String> commitMessages(Path repoDir) throws Exception {
        try (Git git = Git.open(repoDir.toFile())) {
            List<String> messages = new ArrayList<>();
            for (RevCommit commit : git.log().call()) {
                messages.add(commit.getFullMessage().strip());
            }
            return messages;
        }
    }
}
