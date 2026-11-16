package ai.protomolt.proto.codegen;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies {@code core/codegen/provenance/pin-generator-inputs.patch} against the pinned
 * {@code protobuf4j} pre-image blobs and enforces that the patched WASM build container
 * verifies {@code https://snapshot.ubuntu.com} with a pinned CA bundle while keeping
 * {@code Verify-Peer}, {@code Verify-Host}, and {@code Check-Valid-Until} enabled.
 */
class WasmProvenancePatchTest {

    private static final String PROVENANCE_DIR_PROPERTY = "protomolt.codegen.provenance.dir";
    private static final String PATCH_FILENAME = "pin-generator-inputs.patch";
    private static final String REBUILD_SCRIPT_FILENAME = "rebuild-protoc-wrapper-v4.sh";
    private static final String BASE_RESOURCE_PREFIX =
            "/ai/protomolt/proto/codegen/provenance/base/";
    private static final String DOCKERFILE_REL_PATH = "buildtools-v4/Dockerfile";
    private static final String EXECUTABLE_MODE = "100755";

    private static final String DIGEST_PINNED_FROM =
            "FROM --platform=linux/amd64 ubuntu:24.04@sha256:";
    private static final String ADD_CHECKSUM_PREFIX = "ADD --checksum=sha256:";
    private static final String CA_CERTIFICATES_DEB_MARKER = "ca-certificates_";
    private static final String OPENSSL_DEB_MARKER = "openssl_";
    private static final String CA_BUNDLE_CHECK = "test -s /etc/ssl/certs/ca-certificates.crt";
    private static final String HTTPS_SNAPSHOT_PREFIX = "https://snapshot.ubuntu.com/ubuntu/";
    private static final String HTTP_SNAPSHOT_PREFIX = "http://snapshot.ubuntu.com/";
    private static final String APT_UPDATE_STRICT = "apt-get --error-on=any update";
    private static final String SOURCES_REWRITE_SED = "sed -i";
    private static final String ARCHIVE_HOST_REWRITE =
            "s|http://archive.ubuntu.com/ubuntu/|https://snapshot.ubuntu.com/ubuntu/";
    private static final String SECURITY_HOST_REWRITE =
            "s|http://security.ubuntu.com/ubuntu/|https://snapshot.ubuntu.com/ubuntu/";
    private static final String UBUNTU_SOURCES_SED_LINE = "&& sed -i \\";
    private static final String UBUNTU_SOURCES_FILE = "/etc/apt/sources.list.d/ubuntu.sources";
    private static final String LEFTOVER_HOST_GREP =
            "! grep -q -e 'archive.ubuntu.com' -e 'security.ubuntu.com' -e 'ports.ubuntu.com' -e '^URIs:.*http://'";

    /**
     * One file entry parsed from {@code diff --git} and {@code index} lines in the provenance patch.
     */
    private record PatchFileEntry(
            String relativePath,
            String oldBlobPrefix,
            String newBlobPrefix,
            String fileMode) {
    }

    @Test
    void patchAppliesCleanlyAgainstPinnedProtobuf4jPreImage(@TempDir Path tempDir)
            throws IOException, InterruptedException {
        Path provenanceDir = resolveProvenanceDir();
        Path patchFile = provenanceDir.resolve(PATCH_FILENAME);
        Path rebuildScript = provenanceDir.resolve(REBUILD_SCRIPT_FILENAME);
        assertThat(patchFile).isRegularFile();
        assertThat(rebuildScript).isRegularFile();
        assertThat(Files.readString(rebuildScript, StandardCharsets.UTF_8))
                .contains("apply --check");

        String patchText = Files.readString(patchFile, StandardCharsets.UTF_8);
        List<PatchFileEntry> entries = parsePatchEntries(patchText);
        assertThat(entries)
                .extracting(PatchFileEntry::relativePath)
                .contains(DOCKERFILE_REL_PATH);

        materializeBaseTree(tempDir, entries);
        for (PatchFileEntry entry : entries) {
            String actualOldHash = gitHashObject(tempDir, tempDir.resolve(entry.relativePath()));
            assertThat(actualOldHash)
                    .as("pre-image blob hash for %s", entry.relativePath())
                    .startsWith(entry.oldBlobPrefix());
        }

        runGit(tempDir, "apply", "--check", patchFile.toAbsolutePath().toString());
        runGit(tempDir, "apply", patchFile.toAbsolutePath().toString());

        for (PatchFileEntry entry : entries) {
            String actualNewHash = gitHashObject(tempDir, tempDir.resolve(entry.relativePath()));
            assertThat(actualNewHash)
                    .as("post-image blob hash for %s", entry.relativePath())
                    .startsWith(entry.newBlobPrefix());
        }

        String patchedDockerfile = Files.readString(
                tempDir.resolve(DOCKERFILE_REL_PATH), StandardCharsets.UTF_8);
        assertThatCode(() -> validateAptTlsAndSnapshotConfig(patchText, patchedDockerfile))
                .doesNotThrowAnyException();
    }

    @Test
    void gitApplyCheckRefusesCorruptedPatchHunk(@TempDir Path tempDir)
            throws IOException, InterruptedException {
        Path patchFile = resolveProvenanceDir().resolve(PATCH_FILENAME);
        String patchText = Files.readString(patchFile, StandardCharsets.UTF_8);
        List<PatchFileEntry> entries = parsePatchEntries(patchText);
        materializeBaseTree(tempDir, entries);

        Path corruptedPatch = tempDir.resolve("corrupted.patch");
        Files.writeString(
                corruptedPatch,
                patchText.replace("WORKDIR /workspace", "WORKDIR /tampered-context\n+EXTRA"),
                StandardCharsets.UTF_8);

        ProcessResult result = runGitAllowingFailure(
                tempDir, "apply", "--check", corruptedPatch.toAbsolutePath().toString());
        assertThat(result.exitCode()).isNotZero();
    }

    @Test
    void validatorRefusesDisabledVerifyPeer(@TempDir Path tempDir)
            throws IOException, InterruptedException {
        String validDockerfile = applyAndReadDockerfile(tempDir);

        String viaAptConf = validDockerfile
                + "\nRUN printf 'Acquire::https::snapshot.ubuntu.com::Verify-Peer \"false\";\\n'"
                + " > /etc/apt/apt.conf.d/99snapshot-tls\n";
        assertThatThrownBy(() -> validateAptTlsAndSnapshotConfig("", viaAptConf))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Verify-Peer");

        String viaCliOption = validDockerfile.replace(
                APT_UPDATE_STRICT,
                "apt-get -o Acquire::https::Verify-Peer=false --error-on=any update");
        assertThatThrownBy(() -> validateAptTlsAndSnapshotConfig("", viaCliOption))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Verify-Peer");
    }

    @Test
    void validatorRefusesDisabledVerifyHost(@TempDir Path tempDir)
            throws IOException, InterruptedException {
        String validDockerfile = applyAndReadDockerfile(tempDir);

        String viaAptConf = validDockerfile
                + "\nRUN printf 'Acquire::https::snapshot.ubuntu.com::Verify-Host \"false\";\\n'"
                + " > /etc/apt/apt.conf.d/99snapshot-tls\n";
        assertThatThrownBy(() -> validateAptTlsAndSnapshotConfig("", viaAptConf))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Verify-Host");

        String viaNumericZero = validDockerfile.replace(
                APT_UPDATE_STRICT,
                "apt-get -o Acquire::https::Verify-Host=0 --error-on=any update");
        assertThatThrownBy(() -> validateAptTlsAndSnapshotConfig("", viaNumericZero))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Verify-Host");
    }

    @Test
    void validatorRefusesDisabledCheckValidUntil(@TempDir Path tempDir)
            throws IOException, InterruptedException {
        String validDockerfile = applyAndReadDockerfile(tempDir);

        String viaCliOption = validDockerfile.replace(
                APT_UPDATE_STRICT,
                "apt-get -o Acquire::Check-Valid-Until=false --error-on=any update");
        assertThatThrownBy(() -> validateAptTlsAndSnapshotConfig("", viaCliOption))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Check-Valid-Until");

        String viaAptConf = validDockerfile
                + "\nRUN printf 'Acquire::Check-Valid-Until \"false\";\\n'"
                + " > /etc/apt/apt.conf.d/99valid\n";
        assertThatThrownBy(() -> validateAptTlsAndSnapshotConfig("", viaAptConf))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Check-Valid-Until");
    }

    @Test
    void validatorRefusesPlainHttpSnapshotOrMissingCaBootstrap(@TempDir Path tempDir)
            throws IOException, InterruptedException {
        String validDockerfile = applyAndReadDockerfile(tempDir);

        String plainHttp = validDockerfile.replace(HTTPS_SNAPSHOT_PREFIX, HTTP_SNAPSHOT_PREFIX);
        assertThatThrownBy(() -> validateAptTlsAndSnapshotConfig("", plainHttp))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(HTTP_SNAPSHOT_PREFIX);

        String missingCaBundleCheck = validDockerfile.replace(CA_BUNDLE_CHECK, "true");
        assertThatThrownBy(() -> validateAptTlsAndSnapshotConfig("", missingCaBundleCheck))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(CA_BUNDLE_CHECK);
    }

    @Test
    void validatorRefusesDockerfileMissingSourcesRewriteOrLeftoverHostGrep(@TempDir Path tempDir)
            throws IOException, InterruptedException {
        String validDockerfile = applyAndReadDockerfile(tempDir);
        assertThatCode(() -> validateAptTlsAndSnapshotConfig("", validDockerfile))
                .doesNotThrowAnyException();

        String missingSed = removeLineContaining(validDockerfile, SOURCES_REWRITE_SED);
        assertThat(missingSed).doesNotContain(SOURCES_REWRITE_SED);
        assertThatThrownBy(() -> validateAptTlsAndSnapshotConfig("", missingSed))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(SOURCES_REWRITE_SED);

        String unrelatedSedOnly = "RUN sed -i 's|a|b|' /etc/unrelated.conf\n"
                + removeLineEqualTo(validDockerfile, UBUNTU_SOURCES_SED_LINE);
        assertThat(unrelatedSedOnly).contains(SOURCES_REWRITE_SED).contains(ARCHIVE_HOST_REWRITE);
        assertThatThrownBy(() -> validateAptTlsAndSnapshotConfig("", unrelatedSedOnly))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(SOURCES_REWRITE_SED);

        String missingArchiveRewrite = validDockerfile.replace(ARCHIVE_HOST_REWRITE, "s|http://archive.ubuntu.com/ubuntu/|http://archive.ubuntu.com/ubuntu/|");
        assertThatThrownBy(() -> validateAptTlsAndSnapshotConfig("", missingArchiveRewrite))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(ARCHIVE_HOST_REWRITE);

        String missingSecurityRewrite = validDockerfile.replace(SECURITY_HOST_REWRITE, "s|http://security.ubuntu.com/ubuntu/|http://security.ubuntu.com/ubuntu/|");
        assertThatThrownBy(() -> validateAptTlsAndSnapshotConfig("", missingSecurityRewrite))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(SECURITY_HOST_REWRITE);

        String missingGrep = removeLineContaining(validDockerfile, LEFTOVER_HOST_GREP);
        assertThat(missingGrep).doesNotContain(LEFTOVER_HOST_GREP);
        assertThatThrownBy(() -> validateAptTlsAndSnapshotConfig("", missingGrep))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(LEFTOVER_HOST_GREP);
    }

    private static String removeLineEqualTo(String text, String trimmedLine) {
        String removed = text.lines()
                .filter(line -> !line.strip().equals(trimmedLine))
                .reduce((left, right) -> left + "\n" + right)
                .orElse("");
        assertThat(removed).isNotEqualTo(text);
        return removed;
    }

    private static String removeLineContaining(String text, String needle) {
        return text.lines()
                .filter(line -> !line.contains(needle))
                .reduce((left, right) -> left + "\n" + right)
                .orElse("");
    }

    private static String applyAndReadDockerfile(Path tempDir)
            throws IOException, InterruptedException {
        Path patchFile = resolveProvenanceDir().resolve(PATCH_FILENAME);
        String patchText = Files.readString(patchFile, StandardCharsets.UTF_8);
        List<PatchFileEntry> entries = parsePatchEntries(patchText);
        materializeBaseTree(tempDir, entries);
        runGit(tempDir, "apply", patchFile.toAbsolutePath().toString());
        return Files.readString(tempDir.resolve(DOCKERFILE_REL_PATH), StandardCharsets.UTF_8);
    }

    /**
     * Validates that neither the provenance patch nor the resulting {@code Dockerfile} disables
     * {@code Verify-Peer}, {@code Verify-Host}, or {@code Check-Valid-Until}, and that the
     * {@code Dockerfile} installs a checksum-pinned CA bundle, rewrites
     * {@code /etc/apt/sources.list.d/ubuntu.sources} onto the HTTPS snapshot, and rejects
     * leftover archive hosts and plain-HTTP {@code URIs:} entries before
     * {@code apt-get update}.
     */
    static void validateAptTlsAndSnapshotConfig(String patchText, String dockerfileText) {
        refuseDisabledOption(patchText, "Verify-Peer");
        refuseDisabledOption(dockerfileText, "Verify-Peer");
        refuseDisabledOption(patchText, "Verify-Host");
        refuseDisabledOption(dockerfileText, "Verify-Host");
        refuseDisabledOption(patchText, "Check-Valid-Until");
        refuseDisabledOption(dockerfileText, "Check-Valid-Until");

        String lowerCombined = (patchText + "\n" + dockerfileText).toLowerCase(Locale.ROOT);
        for (String forbidden : List.of(
                "--allow-insecure-repositories",
                "--allow-unauthenticated",
                "trusted=yes")) {
            if (lowerCombined.contains(forbidden)) {
                throw new IllegalArgumentException(
                        "Provenance recipe weakens apt signature verification with " + forbidden);
            }
        }

        if (patchText.contains(HTTP_SNAPSHOT_PREFIX) || dockerfileText.contains(HTTP_SNAPSHOT_PREFIX)) {
            throw new IllegalArgumentException(
                    "Provenance recipe must use HTTPS, found " + HTTP_SNAPSHOT_PREFIX);
        }

        if (!dockerfileText.contains(DIGEST_PINNED_FROM)) {
            throw new IllegalArgumentException(
                    "Dockerfile must pin the linux/amd64 ubuntu:24.04 base image by digest");
        }
        int addChecksumPos = dockerfileText.indexOf(ADD_CHECKSUM_PREFIX);
        int opensslPos = dockerfileText.indexOf(OPENSSL_DEB_MARKER);
        int caCertDebPos = dockerfileText.indexOf(CA_CERTIFICATES_DEB_MARKER);
        int caBundleCheckPos = dockerfileText.indexOf(CA_BUNDLE_CHECK);
        int httpsSnapshotPos = dockerfileText.indexOf(HTTPS_SNAPSHOT_PREFIX);
        int aptUpdatePos = dockerfileText.indexOf(APT_UPDATE_STRICT);

        if (addChecksumPos < 0 || opensslPos < 0 || caCertDebPos < 0) {
            throw new IllegalArgumentException(
                    "Dockerfile must bootstrap openssl and ca-certificates via " + ADD_CHECKSUM_PREFIX);
        }
        if (caBundleCheckPos < 0) {
            throw new IllegalArgumentException(
                    "Dockerfile must verify populated CA bundle with " + CA_BUNDLE_CHECK);
        }
        if (httpsSnapshotPos < 0) {
            throw new IllegalArgumentException(
                    "Dockerfile must configure " + HTTPS_SNAPSHOT_PREFIX);
        }
        if (aptUpdatePos < 0) {
            throw new IllegalArgumentException(
                    "Dockerfile must run " + APT_UPDATE_STRICT);
        }
        if (!(addChecksumPos < caBundleCheckPos && caBundleCheckPos < aptUpdatePos)) {
            throw new IllegalArgumentException(
                    "CA bundle must be checksum-fetched and verified before apt-get update runs");
        }
        int archiveRewritePos = requireBeforeUpdate(dockerfileText, ARCHIVE_HOST_REWRITE, aptUpdatePos);
        int sedPos = rewritingSed(dockerfileText, archiveRewritePos);
        int securityRewritePos = requireBeforeUpdate(dockerfileText, SECURITY_HOST_REWRITE, aptUpdatePos);
        int sourcesFilePos = requireBeforeUpdate(dockerfileText, UBUNTU_SOURCES_FILE, aptUpdatePos);
        int leftoverGrepPos = requireBeforeUpdate(dockerfileText, LEFTOVER_HOST_GREP, aptUpdatePos);
        if (!(sedPos < archiveRewritePos
                && archiveRewritePos < securityRewritePos
                && securityRewritePos < sourcesFilePos
                && sourcesFilePos < leftoverGrepPos)) {
            throw new IllegalArgumentException(
                    "Dockerfile must rewrite ubuntu.sources onto the HTTPS snapshot and then reject leftover archive hosts");
        }
    }

    /**
     * The position of the {@code sed -i} whose arguments start with the archive rewrite: only
     * whitespace, line continuations and {@code -e '} may sit between the two. An unrelated
     * {@code sed -i} elsewhere in the file does not satisfy the requirement.
     */
    private static int rewritingSed(String text, int archiveRewritePos) {
        int pos = text.lastIndexOf(SOURCES_REWRITE_SED, archiveRewritePos);
        if (pos < 0) {
            throw new IllegalArgumentException(
                    "Dockerfile must rewrite ubuntu.sources before apt-get update, missing " + SOURCES_REWRITE_SED);
        }
        for (int i = pos + SOURCES_REWRITE_SED.length(); i < archiveRewritePos; i++) {
            if (" \t\r\n\\-e'".indexOf(text.charAt(i)) < 0) {
                throw new IllegalArgumentException("Dockerfile must apply the archive rewrite with "
                        + SOURCES_REWRITE_SED + " on ubuntu.sources, found other text in between");
            }
        }
        return pos;
    }

    private static int requireBeforeUpdate(String text, String snippet, int aptUpdatePos) {
        int pos = text.indexOf(snippet);
        if (pos < 0 || pos > aptUpdatePos) {
            throw new IllegalArgumentException(
                    "Dockerfile must rewrite ubuntu.sources before apt-get update, missing " + snippet);
        }
        return pos;
    }

    private static void refuseDisabledOption(String text, String optionName) {
        String lowerText = text.toLowerCase(Locale.ROOT);
        String lowerOption = optionName.toLowerCase(Locale.ROOT);
        int from = 0;
        while (true) {
            int idx = lowerText.indexOf(lowerOption, from);
            if (idx < 0) {
                return;
            }
            int pos = idx + lowerOption.length();
            while (pos < lowerText.length()
                    && (Character.isWhitespace(lowerText.charAt(pos))
                    || lowerText.charAt(pos) == '='
                    || lowerText.charAt(pos) == '"'
                    || lowerText.charAt(pos) == '\'')) {
                pos++;
            }
            if (lowerText.startsWith("false", pos) || isStandaloneZero(lowerText, pos)) {
                throw new IllegalArgumentException(
                        "Provenance recipe disables apt option " + optionName);
            }
            from = idx + lowerOption.length();
        }
    }

    private static boolean isStandaloneZero(String text, int pos) {
        if (pos >= text.length() || text.charAt(pos) != '0') {
            return false;
        }
        int next = pos + 1;
        return next == text.length() || !Character.isLetterOrDigit(text.charAt(next));
    }

    private static Path resolveProvenanceDir() {
        String configured = System.getProperty(PROVENANCE_DIR_PROPERTY);
        if (configured != null && !configured.isBlank()) {
            return Path.of(configured);
        }
        Path moduleRelative = Path.of("provenance");
        if (Files.isDirectory(moduleRelative)) {
            return moduleRelative;
        }
        return Path.of("core/codegen/provenance");
    }

    private static List<PatchFileEntry> parsePatchEntries(String patchText) {
        List<PatchFileEntry> entries = new ArrayList<>();
        List<String> lines = patchText.lines().toList();
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            if (!line.startsWith("diff --git a/")) {
                continue;
            }
            int middle = line.indexOf(" b/");
            if (middle < 0) {
                throw new IllegalStateException("Malformed diff header: " + line);
            }
            String relPath = line.substring("diff --git a/".length(), middle);
            if (i + 1 >= lines.size() || !lines.get(i + 1).startsWith("index ")) {
                throw new IllegalStateException("Missing index header after: " + line);
            }
            String indexLine = lines.get(i + 1).substring("index ".length()).trim();
            int space = indexLine.indexOf(' ');
            String range = space >= 0 ? indexLine.substring(0, space) : indexLine;
            String mode = space >= 0 ? indexLine.substring(space + 1).trim() : "100644";
            int dots = range.indexOf("..");
            if (dots < 0) {
                throw new IllegalStateException("Malformed index range: " + lines.get(i + 1));
            }
            entries.add(new PatchFileEntry(
                    relPath,
                    range.substring(0, dots),
                    range.substring(dots + 2),
                    mode));
        }
        return entries;
    }

    private static void materializeBaseTree(Path targetRoot, List<PatchFileEntry> entries)
            throws IOException, InterruptedException {
        runGit(targetRoot, "init", "--quiet");
        for (PatchFileEntry entry : entries) {
            String resourcePath = BASE_RESOURCE_PREFIX + entry.relativePath();
            byte[] content;
            try (InputStream in = WasmProvenancePatchTest.class.getResourceAsStream(resourcePath)) {
                if (in == null) {
                    throw new IllegalStateException("Missing base test resource: " + resourcePath);
                }
                content = in.readAllBytes();
            }
            Path targetFile = targetRoot.resolve(entry.relativePath());
            Files.createDirectories(targetFile.getParent());
            Files.write(targetFile, content);
            if (EXECUTABLE_MODE.equals(entry.fileMode())) {
                Set<PosixFilePermission> perms = EnumSet.copyOf(Files.getPosixFilePermissions(targetFile));
                perms.add(PosixFilePermission.OWNER_EXECUTE);
                perms.add(PosixFilePermission.GROUP_EXECUTE);
                perms.add(PosixFilePermission.OTHERS_EXECUTE);
                Files.setPosixFilePermissions(targetFile, perms);
            }
        }
        runGit(targetRoot, "add", ".");
    }

    private static String gitHashObject(Path workingDir, Path file)
            throws IOException, InterruptedException {
        return runGit(workingDir, "hash-object", file.toAbsolutePath().toString()).strip();
    }

    private record ProcessResult(int exitCode, String output) {
    }

    private static String runGit(Path workingDir, String... args)
            throws IOException, InterruptedException {
        ProcessResult result = runGitAllowingFailure(workingDir, args);
        if (result.exitCode() != 0) {
            throw new IllegalStateException(
                    "git " + String.join(" ", args) + " failed (" + result.exitCode() + "): "
                            + result.output());
        }
        return result.output();
    }

    private static ProcessResult runGitAllowingFailure(Path workingDir, String... args)
            throws IOException, InterruptedException {
        List<String> command = new ArrayList<>(args.length + 1);
        command.add("git");
        command.addAll(List.of(args));
        Process process = new ProcessBuilder(command)
                .directory(workingDir.toFile())
                .redirectErrorStream(true)
                .start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        int exitCode = process.waitFor();
        return new ProcessResult(exitCode, output);
    }
}
