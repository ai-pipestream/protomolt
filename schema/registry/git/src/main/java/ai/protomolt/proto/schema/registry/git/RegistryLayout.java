package ai.protomolt.proto.schema.registry.git;

import ai.protomolt.proto.registry.DescriptorSetArtifacts;
import ai.protomolt.proto.registry.RegistrationSupport;
import java.io.IOException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Package-private working-tree layout for {@link GitSchemaRegistryStore}:
 * directory and file names, subject/workflow/descriptor path derivation
 * with traversal checks, stored-name URL encoding, version-file name
 * parsing, and document-name validation.
 *
 * <p>Pure path and name transformations only: this class never writes,
 * locks, or commits. Transaction and lock ownership stay in
 * {@link GitSchemaRegistryStore}; the layout answers where a name lives
 * and whether a repository directory is present to be read from.
 */
final class RegistryLayout {

    static final String SUBJECTS_DIR = "subjects";
    static final String WORKFLOWS_DIR = "workflows";
    static final String CONFIGS_DIR = "configs";
    static final String WORKFLOW_VERSIONS_DIR = "workflow-versions";
    static final String DESCRIPTORS_DIR = "descriptors/sha256";
    static final String REGISTRY_FILE = "registry.json";
    static final String LOCK_FILE = "registry.lock";
    static final String CONFIG_FILE = "config.json";

    private final Path repoDir;

    RegistryLayout(Path repoDir) {
        this.repoDir = repoDir;
    }

    static String encode(String subject) {
        return URLEncoder.encode(subject, StandardCharsets.UTF_8);
    }

    static String decode(String encoded) {
        return URLDecoder.decode(encoded, StandardCharsets.UTF_8);
    }

    /**
     * The repo-relative {@code subjects/<encoded>} path for a subject, checked to stay inside
     * {@link #SUBJECTS_DIR}.
     *
     * <p>{@link RegistrationSupport#requireSubject} already refuses the names that can escape,
     * so this is a second line rather than the only one: it keeps a future change to
     * {@link #encode} from silently turning a subject name into a path traversal.
     */
    String subjectDir(String subject) {
        String relative = SUBJECTS_DIR + "/" + encode(subject);
        Path resolved = repoDir.resolve(relative).normalize();
        if (!resolved.startsWith(repoDir.resolve(SUBJECTS_DIR).normalize())
                || resolved.equals(repoDir.resolve(SUBJECTS_DIR).normalize())) {
            throw new IllegalArgumentException(
                    "subject '" + subject + "' does not name a directory under " + SUBJECTS_DIR);
        }
        return relative;
    }

    /**
     * The repo-relative {@code workflows/<encoded>/<encoded>.pb} path, checked to stay inside
     * {@link #WORKFLOW_VERSIONS_DIR} — the same second line of defense as {@link #subjectDir}.
     */
    String workflowPath(String name, String version) {
        String relative = WORKFLOW_VERSIONS_DIR + "/" + encode(name) + "/" + encode(version) + ".pb";
        Path resolved = repoDir.resolve(relative).normalize();
        if (!resolved.startsWith(repoDir.resolve(WORKFLOW_VERSIONS_DIR).normalize())) {
            throw new IllegalArgumentException(
                    "workflow identity does not name a file under " + WORKFLOW_VERSIONS_DIR);
        }
        return relative;
    }

    /** The content-addressed descriptor artifact path, checked to stay inside its directory. */
    Path descriptorPath(String fingerprint) {
        DescriptorSetArtifacts.requireFingerprint(fingerprint);
        Path directory = repoDir.resolve(DESCRIPTORS_DIR).normalize();
        Path resolved = directory.resolve(fingerprint + ".pb").normalize();
        if (!resolved.startsWith(directory)) {
            throw new IllegalArgumentException("descriptor fingerprint escapes registry storage");
        }
        return resolved;
    }

    /**
     * The version number named by a {@code v<N>.proto} file name, or {@code -1} when the
     * name is not exactly of that form (equivalent to a full match of {@code v(\d+)\.proto}).
     */
    static int versionNumber(String fileName) {
        String suffix = ".proto";
        if (fileName.length() <= 1 + suffix.length()
                || fileName.charAt(0) != 'v'
                || !fileName.endsWith(suffix)) {
            return -1;
        }
        String digits = fileName.substring(1, fileName.length() - suffix.length());
        for (int i = 0; i < digits.length(); i++) {
            char c = digits.charAt(i);
            if (c < '0' || c > '9') {
                return -1;
            }
        }
        return Integer.parseInt(digits);
    }

    static void requireDocumentName(String name, String kind) {
        if (name == null || name.isBlank() || !isDocumentName(name)) {
            throw new IllegalArgumentException(
                    kind + " names use [A-Za-z0-9._-]; got '" + name + "'");
        }
    }

    static void requireWorkflowName(String name) {
        if (name == null || name.isBlank() || !isDocumentName(name)) {
            throw new IllegalArgumentException(
                    "Workflow names use [A-Za-z0-9._-]; got '" + name + "'");
        }
    }

    /** Full match of {@code [A-Za-z0-9._-]+}. */
    private static boolean isDocumentName(String name) {
        if (name.isEmpty()) {
            return false;
        }
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            boolean ok = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
                    || (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-';
            if (!ok) {
                return false;
            }
        }
        return true;
    }

    /** Fails when the repository directory is not a Git working directory. */
    void requireDescriptorRepository() throws IOException {
        if (!Files.readAttributes(repoDir, BasicFileAttributes.class).isDirectory()
                || !Files.readAttributes(repoDir.resolve(".git"), BasicFileAttributes.class,
                        LinkOption.NOFOLLOW_LINKS).isDirectory())
            throw new IOException("Descriptor repository is not a Git working directory");
    }

    static boolean descriptorDirectoryExists(Path path) throws IOException {
        try {
            if (!Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS).isDirectory())
                throw new IOException("Descriptor parent is not a directory: " + path);
            return true;
        } catch (NoSuchFileException missing) {
            return false;
        }
    }
}
