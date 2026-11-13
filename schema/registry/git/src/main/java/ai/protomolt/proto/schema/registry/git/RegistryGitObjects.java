package ai.protomolt.proto.schema.registry.git;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

import org.eclipse.jgit.dircache.DirCache;
import org.eclipse.jgit.dircache.DirCacheBuilder;
import org.eclipse.jgit.dircache.DirCacheEntry;
import org.eclipse.jgit.lib.CommitBuilder;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.FileMode;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectInserter;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.ObjectReader;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.RefUpdate;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.TreeFormatter;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.TreeWalk;

/**
 * Package-private object-database access for {@link GitSchemaRegistryStore}.
 *
 * <p>Every read descends from a committed tree ({@code HEAD} or a caller-pinned commit), and
 * every write inserts blobs, trees and one commit with {@link ObjectInserter}, then advances
 * {@code HEAD} only through a {@link RefUpdate} compare-and-swap against the exact commit
 * {@code HEAD} named when the write started. On non-bare repositories, {@code .git/index}
 * ({@link DirCache}) is aligned with the newly committed root tree alongside the ref update so
 * subsequent porcelain commits preserve all registered files; bare repositories have no index
 * and skip {@link DirCache}. A checked-out working tree itself is never read or rewritten, so
 * bare and non-bare repositories behave identically on registry reads and writes and the
 * working tree of a non-bare repository stays untouched (see the store's class documentation
 * for the recovery semantics).
 *
 * <p>Unreachable objects left behind by a failed write are harmless and are reclaimed by
 * ordinary Git GC.
 */
final class RegistryGitObjects {

    private RegistryGitObjects() {
    }

    /** One entry of a git tree: name, mode and object id. */
    record TreeChild(String name, FileMode mode, ObjectId id) {

        boolean isTree() {
            return mode.getObjectType() == Constants.OBJ_TREE;
        }

        boolean isBlob() {
            return mode.getObjectType() == Constants.OBJ_BLOB;
        }
    }

    /** Stages of one atomic write, in execution order; hooks exist for fault-injection tests. */
    enum WriteStage {
        /** All new blob objects are inserted; trees, the commit and the ref update remain. */
        AFTER_BLOBS,
        /** The rewritten root tree is inserted; the commit and the ref update remain. */
        AFTER_TREES,
        /** The commit object is inserted and objects are flushed; the ref update remains. */
        AFTER_COMMIT,
        /** HEAD was advanced; nothing remains. A fault here makes the write outcome ambiguous. */
        AFTER_REF_UPDATE
    }

    /** A fault-injection hook between the stages of {@link #commit}. */
    @FunctionalInterface
    interface WriteHook {

        WriteHook NONE = stage -> {
        };

        void at(WriteStage stage) throws IOException;
    }

    /** A committed tree disagrees with the registry layout (a file where a directory belongs). */
    static final class CorruptTreeException extends IOException {

        CorruptTreeException(String path, String detail) {
            super("corrupt registry tree at " + path + ": " + detail);
        }
    }

    /**
     * HEAD moved after the write read it: the compare-and-swap refused to overwrite the
     * external commit, so this write was not applied.
     */
    static final class HeadChangedException extends IOException {

        HeadChangedException(ObjectId expected, RefUpdate.Result result) {
            super("registry HEAD changed concurrently after the write started"
                    + " (expected " + (expected == null ? "no commit" : expected.getName())
                    + ", ref update result " + result + "); refresh and retry instead");
        }
    }

    /** A committed blob exceeds the reader's declared size bound. */
    static final class ObjectTooLargeException extends IOException {
        ObjectTooLargeException(String path, long maxBytes) {
            super("Git object exceeds the " + maxBytes + "-byte limit at " + path);
        }
    }

    // ---------------------------------------------------------------- reads

    /** The commit HEAD names, or {@code null} while the branch is unborn. */
    static ObjectId headCommit(Repository repo) throws IOException {
        return repo.resolve(Constants.HEAD);
    }

    /**
     * The bytes of the blob at one repo-relative path of HEAD's tree, or {@code null} when
     * HEAD is unborn or the path is absent.
     *
     * @throws CorruptTreeException when a path component is not a tree or the final component
     *         names a tree where the blob belongs
     * @throws IOException when the repository cannot be read at all
     */
    static byte[] headBlob(Repository repo, String path) throws IOException {
        return headBlob(repo, path, Long.MAX_VALUE);
    }

    /** Reads a committed blob from HEAD only after checking its size against the caller's limit. */
    static byte[] headBlob(Repository repo, String path, long maxBytes) throws IOException {
        return blob(repo, headCommit(repo), path, maxBytes);
    }

    /**
     * The bytes of the blob at one repo-relative path of {@code commit}'s tree, or {@code null}
     * when {@code commit} is {@code null} or the path is absent.
     */
    static byte[] blob(Repository repo, ObjectId commit, String path) throws IOException {
        return blob(repo, commit, path, Long.MAX_VALUE);
    }

    /**
     * Reads a blob from a pinned {@code commit}'s tree only after checking its size against the
     * caller's limit, or returns {@code null} when {@code commit} is {@code null} or absent.
     */
    static byte[] blob(Repository repo, ObjectId commit, String path, long maxBytes)
            throws IOException {
        if (commit == null) {
            return null;
        }
        try (RevWalk walk = new RevWalk(repo)) {
            ObjectId id = descend(repo, walk.parseCommit(commit).getTree(), path);
            if (id == null) {
                return null;
            }
            ObjectLoader loader = repo.open(id);
            if (loader.getType() != Constants.OBJ_BLOB) {
                throw new CorruptTreeException(path, "tree where a blob belongs");
            }
            if (loader.getSize() > maxBytes) {
                throw new ObjectTooLargeException(path, maxBytes);
            }
            return loader.getBytes();
        }
    }

    /**
     * The entries of the tree at one repo-relative directory of HEAD's tree, in git tree
     * order, or an empty list when HEAD is unborn or the directory is absent.
     *
     * @throws CorruptTreeException when the directory path names a blob
     * @throws IOException when the repository cannot be read at all
     */
    static List<TreeChild> headChildren(Repository repo, String dir) throws IOException {
        return children(repo, headCommit(repo), dir);
    }

    /**
     * The entries of the tree at one repo-relative directory of {@code commit}'s tree, in git
     * tree order, or an empty list when {@code commit} is {@code null} or the directory is absent.
     */
    static List<TreeChild> children(Repository repo, ObjectId commit, String dir)
            throws IOException {
        if (commit == null) {
            return List.of();
        }
        try (RevWalk walk = new RevWalk(repo)) {
            ObjectId tree = descend(repo, walk.parseCommit(commit).getTree(), dir);
            if (tree == null) {
                return List.of();
            }
            List<TreeChild> children = new ArrayList<>();
            try (TreeWalk listing = new TreeWalk(repo)) {
                listing.addTree(tree);
                while (listing.next()) {
                    children.add(new TreeChild(listing.getNameString(),
                            listing.getFileMode(0), listing.getObjectId(0)));
                }
            }
            return children;
        }
    }

    /**
     * Every blob under one repo-relative directory of HEAD's tree, keyed by repo-relative
     * path and sorted; empty when HEAD is unborn or nothing lives under the directory.
     *
     * @throws IOException when the repository cannot be read at all
     */
    static Map<String, byte[]> headBlobsUnder(Repository repo, String dir) throws IOException {
        return blobsUnder(repo, headCommit(repo), dir);
    }

    /**
     * Every blob under one repo-relative directory of {@code commit}'s tree, keyed by
     * repo-relative path and sorted; empty when {@code commit} is {@code null} or nothing lives
     * under the directory.
     */
    static Map<String, byte[]> blobsUnder(Repository repo, ObjectId commit, String dir)
            throws IOException {
        if (commit == null) {
            return Map.of();
        }
        Map<String, byte[]> blobs = new TreeMap<>();
        try (RevWalk revWalk = new RevWalk(repo);
                TreeWalk walk = new TreeWalk(repo)) {
            walk.addTree(revWalk.parseCommit(commit).getTree());
            walk.setRecursive(true);
            String prefix = dir + "/";
            while (walk.next()) {
                String path = walk.getPathString();
                if (!path.startsWith(prefix)) {
                    continue;
                }
                blobs.put(path, repo.open(walk.getObjectId(0)).getBytes());
            }
        }
        return blobs;
    }

    /** Resolves one repo-relative path against a tree; {@code null} when a component is absent. */
    private static ObjectId descend(Repository repo, ObjectId tree, String path) throws IOException {
        ObjectId current = tree;
        int start = 0;
        while (true) {
            int slash = path.indexOf('/', start);
            boolean last = slash < 0;
            String name = last ? path.substring(start) : path.substring(start, slash);
            TreeChild child = child(repo, current, name);
            if (child == null) {
                return null;
            }
            if (last) {
                return child.id();
            }
            if (!child.isTree()) {
                throw new CorruptTreeException(path, "a non-tree entry on the path");
            }
            current = child.id();
            start = slash + 1;
        }
    }

    private static TreeChild child(Repository repo, ObjectId tree, String name) throws IOException {
        try (TreeWalk walk = new TreeWalk(repo)) {
            walk.addTree(tree);
            while (walk.next()) {
                if (walk.getNameString().equals(name)) {
                    return new TreeChild(name, walk.getFileMode(0), walk.getObjectId(0));
                }
            }
            return null;
        }
    }

    // ---------------------------------------------------------------- writes

    /**
     * Applies repo-relative byte changes to HEAD's current tree as one new commit and advances
     * HEAD by compare-and-swap. Delegates to {@link #commit(Repository, ObjectId, PersonIdent,
     * String, Map, WriteHook)} after resolving {@link #headCommit(Repository)}.
     */
    static String commit(Repository repo, PersonIdent author, String message,
            Map<String, Optional<byte[]>> changes, WriteHook hook) throws IOException {
        validateChanges(changes);
        return commit(repo, headCommit(repo), author, message, changes, hook);
    }

    /**
     * Applies repo-relative byte changes to {@code expectedHead}'s tree as one new commit and
     * advances HEAD by compare-and-swap against {@code expectedHead}. {@code changes} maps a
     * repo-relative path to its new content, or to {@link Optional#empty()} to delete the path.
     *
     * <p>Stages run in {@link WriteStage} order; objects are flushed before the ref update,
     * so a failure before the compare-and-swap leaves HEAD untouched and only leaks
     * unreachable objects. On non-bare repositories, {@code .git/index} ({@link DirCache}) is
     * updated alongside HEAD to match the newly committed root tree. A failure after the ref
     * update means the commit may or may not have landed; the caller must re-read HEAD before
     * retrying.
     *
     * @return the new commit id
     * @throws HeadChangedException when HEAD advanced after {@code expectedHead} was read:
     *         nothing was overwritten
     * @throws CorruptTreeException when a change descends below a committed non-tree entry
     * @throws IOException when the repository cannot be read or written
     */
    static String commit(Repository repo, ObjectId expectedHead, PersonIdent author, String message,
            Map<String, Optional<byte[]>> changes, WriteHook hook) throws IOException {
        validateChanges(changes);
        TreeMap<String, Optional<ObjectId>> objectChanges = new TreeMap<>();
        try (ObjectInserter inserter = repo.newObjectInserter()) {
            RevTree baseTree = null;
            if (expectedHead != null) {
                try (RevWalk walk = new RevWalk(repo)) {
                    baseTree = walk.parseCommit(expectedHead).getTree();
                }
            }
            for (Map.Entry<String, Optional<byte[]>> change : changes.entrySet()) {
                if (change.getValue().isPresent()) {
                    objectChanges.put(change.getKey(),
                            Optional.of(inserter.insert(Constants.OBJ_BLOB, change.getValue().get())));
                }
            }
            hook.at(WriteStage.AFTER_BLOBS);
            ObjectId newTree = writeTree(repo, inserter, baseTree, objectChanges, "");
            hook.at(WriteStage.AFTER_TREES);
            CommitBuilder builder = new CommitBuilder();
            builder.setTreeId(newTree);
            if (expectedHead != null) {
                builder.setParentIds(expectedHead);
            }
            builder.setAuthor(author);
            builder.setCommitter(author);
            builder.setMessage(message);
            ObjectId newCommit = inserter.insert(builder);
            inserter.flush();
            hook.at(WriteStage.AFTER_COMMIT);
            DirCache dirCache = repo.isBare() ? null : repo.lockDirCache();
            try {
                if (dirCache != null) {
                    DirCacheBuilder cacheBuilder = dirCache.builder();
                    try (ObjectReader reader = repo.newObjectReader()) {
                        cacheBuilder.addTree(new byte[0], DirCacheEntry.STAGE_0, reader, newTree);
                    }
                    cacheBuilder.finish();
                    dirCache.write();
                }
                advanceHead(repo, author, message, expectedHead, newCommit);
                if (dirCache != null && !dirCache.commit()) {
                    throw new IOException("failed to commit git index for " + newCommit.getName());
                }
            } finally {
                if (dirCache != null) {
                    dirCache.unlock();
                }
            }
            hook.at(WriteStage.AFTER_REF_UPDATE);
            return newCommit.getName();
        }
    }

    private static void validateChanges(Map<String, Optional<byte[]>> changes) {
        if (changes.isEmpty()) {
            throw new IllegalArgumentException("a registry commit needs at least one change");
        }
        for (String path : changes.keySet()) {
            requireSafeChangePath(path);
            for (String other : changes.keySet()) {
                if (path.length() < other.length() && other.startsWith(path + "/")) {
                    throw new IllegalArgumentException(
                            "registry changes cannot contain both a file and its descendant: " + path);
                }
            }
        }
    }

    private static void advanceHead(Repository repo, PersonIdent author, String message,
            ObjectId expectedOld, ObjectId newCommit) throws IOException {
        RefUpdate update = repo.updateRef(headRefName(repo));
        update.setExpectedOldObjectId(expectedOld == null ? ObjectId.zeroId() : expectedOld);
        update.setNewObjectId(newCommit);
        update.setRefLogIdent(author);
        update.setRefLogMessage(message, false);
        RefUpdate.Result result = update.update();
        switch (result) {
            case NEW, FAST_FORWARD, NO_CHANGE -> {
                // applied (or already there)
            }
            default -> throw new HeadChangedException(expectedOld, result);
        }
    }

    /** The ref HEAD points at: the symbolic target, or HEAD itself when detached. */
    private static String headRefName(Repository repo) throws IOException {
        Ref head = repo.getRefDatabase().exactRef(Constants.HEAD);
        if (head == null) {
            throw new IOException("repository has no HEAD");
        }
        return head.isSymbolic() ? head.getTarget().getName() : Constants.HEAD;
    }

    /**
     * Rewrites one tree level: applies the changes for this level, recurses into subtrees
     * the changes touch, and carries every untouched entry over with its original mode, so
     * executable files and gitlinks survive reconstruction.
     *
     * @param changes keys are repo-relative paths, all lying at or below {@code prefix}
     * @return the new tree id (the empty tree when nothing remains)
     */
    private static ObjectId writeTree(Repository repo, ObjectInserter inserter, ObjectId baseTree,
            NavigableMap<String, Optional<ObjectId>> changes, String prefix) throws IOException {
        List<TreeChild> entries = new ArrayList<>();
        List<String> consumed = new ArrayList<>();

        if (baseTree != null) {
            try (TreeWalk walk = new TreeWalk(repo)) {
                walk.addTree(baseTree);
                while (walk.next()) {
                    String name = walk.getNameString();
                    String full = prefix.isEmpty() ? name : prefix + "/" + name;
                    TreeChild base = new TreeChild(name, walk.getFileMode(0), walk.getObjectId(0));
                    Optional<ObjectId> exact = changes.get(full);
                    if (exact != null) {
                        consumed.add(full);
                        exact.ifPresent(id -> entries.add(
                                new TreeChild(name, FileMode.REGULAR_FILE, id)));
                        continue;
                    }
                    NavigableMap<String, Optional<ObjectId>> nested =
                            nestedChanges(changes, full, consumed);
                    if (nested == null) {
                        entries.add(base); // untouched
                    } else if (!base.isTree()) {
                        throw new CorruptTreeException(full,
                                "changes descend below a non-tree entry");
                    } else {
                        entries.add(new TreeChild(name, FileMode.TREE,
                                writeTree(repo, inserter, base.id(), nested, full)));
                    }
                }
            }
        }

        // Changes the base tree did not cover: new blobs at this level, or new subtrees.
        for (Map.Entry<String, Optional<ObjectId>> change : changes.entrySet()) {
            String key = change.getKey();
            if (consumed.contains(key)) {
                continue;
            }
            String local = prefix.isEmpty() ? key : key.substring(prefix.length() + 1);
            int slash = local.indexOf('/');
            if (slash < 0) {
                consumed.add(key);
                change.getValue().ifPresent(id ->
                        entries.add(new TreeChild(local, FileMode.REGULAR_FILE, id)));
            } else {
                String name = local.substring(0, slash);
                String full = prefix.isEmpty() ? name : prefix + "/" + name;
                NavigableMap<String, Optional<ObjectId>> nested =
                        nestedChanges(changes, full, consumed);
                entries.add(new TreeChild(name, FileMode.TREE,
                        writeTree(repo, inserter, null, nested, full)));
            }
        }

        entries.sort(RegistryGitObjects::compareEntries);
        TreeFormatter formatter = new TreeFormatter();
        for (TreeChild entry : entries) {
            formatter.append(entry.name(), entry.mode(), entry.id());
        }
        return formatter.insertTo(inserter);
    }

    /** Git's canonical tree order: byte-wise on names, trees compared with a trailing '/'. */
    private static int compareEntries(TreeChild a, TreeChild b) {
        byte[] aName = a.name().getBytes(StandardCharsets.UTF_8);
        byte[] bName = b.name().getBytes(StandardCharsets.UTF_8);
        int aLength = aName.length + (a.isTree() ? 1 : 0);
        int bLength = bName.length + (b.isTree() ? 1 : 0);
        int limit = Math.min(aLength, bLength);
        for (int i = 0; i < limit; i++) {
            byte aByte = i < aName.length ? aName[i] : (byte) '/';
            byte bByte = i < bName.length ? bName[i] : (byte) '/';
            int compared = Byte.compareUnsigned(aByte, bByte);
            if (compared != 0) {
                return compared;
            }
        }
        return Integer.compare(aLength, bLength);
    }

    /**
     * The changes lying strictly below {@code full} (keys prefixed {@code full/}), marking
     * them consumed, or {@code null} when no change descends below it.
     */
    private static NavigableMap<String, Optional<ObjectId>> nestedChanges(
            NavigableMap<String, Optional<ObjectId>> changes, String full, List<String> consumed) {
        String prefix = full + "/";
        NavigableMap<String, Optional<ObjectId>> nested = new TreeMap<>();
        for (Map.Entry<String, Optional<ObjectId>> change : changes.tailMap(prefix).entrySet()) {
            if (!change.getKey().startsWith(prefix)) {
                break;
            }
            nested.put(change.getKey(), change.getValue());
            consumed.add(change.getKey());
        }
        return nested.isEmpty() ? null : nested;
    }

    /**
     * Change paths are repo-relative, never blank, and never carry {@code .} or {@code ..}
     * segments: a registry write must not be able to address outside the repository.
     */
    private static void requireSafeChangePath(String path) {
        int start = 0;
        while (true) {
            int slash = path.indexOf('/', start);
            String segment = slash < 0 ? path.substring(start) : path.substring(start, slash);
            if (segment.isEmpty() || segment.equals(".") || segment.equals("..")) {
                throw new IllegalArgumentException(
                        "registry paths are relative and stay inside the repository: " + path);
            }
            if (slash < 0) {
                return;
            }
            start = slash + 1;
        }
    }
}
