package ai.protomolt.proto.schema.registry.git;

import ai.protomolt.proto.registry.DescriptorSetArtifacts;
import ai.protomolt.proto.registry.RegistrationSupport;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Package-private repository layout for {@link GitSchemaRegistryStore}:
 * directory and file names, subject/workflow/descriptor path derivation
 * with traversal checks, stored-name URL encoding, version-file name
 * parsing, document-name validation, and detection of an existing
 * repository directory.
 *
 * <p>Pure path and name transformations only: this class never writes,
 * locks, or commits. The layout names where a document lives in the
 * repository tree; transaction and lock ownership stay in
 * {@link GitSchemaRegistryStore} and object access in
 * {@link RegistryGitObjects}.
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

    /**
     * The repo-relative {@code descriptors/sha256/<fingerprint>.pb} path, checked to stay
     * inside {@link #DESCRIPTORS_DIR} — the same second line of defense as
     * {@link #subjectDir}.
     */
    String descriptorRelPath(String fingerprint) {
        DescriptorSetArtifacts.requireFingerprint(fingerprint);
        String relative = DESCRIPTORS_DIR + "/" + fingerprint + ".pb";
        Path resolved = repoDir.resolve(relative).normalize();
        if (!resolved.startsWith(repoDir.resolve(DESCRIPTORS_DIR).normalize())) {
            throw new IllegalArgumentException("descriptor fingerprint escapes registry storage");
        }
        return relative;
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

    /**
     * True when the directory already has the shape of a git repository directory (a
     * {@code HEAD} file with {@code objects/} and {@code refs/}): either a bare repository
     * or a {@code .git} directory of a non-bare one.
     */
    static boolean isGitDirectory(Path dir) {
        return Files.isRegularFile(dir.resolve("HEAD"))
                && Files.isDirectory(dir.resolve("objects"))
                && Files.isDirectory(dir.resolve("refs"));
    }
}
