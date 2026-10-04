package ai.protomolt.proto.repo.container.ledger;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.*;

class DocumentRuntimeArtifactTest {
    @TempDir Path directory;
    private static final DocumentRuntimeArtifact.Limits LIMITS = new DocumentRuntimeArtifact.Limits(16_000_000, 4096, 16);

    @Test void hashesAttributedJarOriginOfARealLoadedClass() throws Exception {
        var classes = compile();
        var jar = directory.resolve("fixture.jar");
        try (var output = new java.util.jar.JarOutputStream(Files.newOutputStream(jar))) {
            output.putNextEntry(new java.util.jar.JarEntry("fixture/Anchor.class"));
            output.write(Files.readAllBytes(classes.resolve("fixture/Anchor.class"))); output.closeEntry();
        }
        try (var loader = new java.net.URLClassLoader(new java.net.URL[]{jar.toUri().toURL()}, null)) {
            var anchor = Class.forName("fixture.Anchor", false, loader);
            var observed = DocumentRuntimeArtifact.observe("fixture-artifact", anchor, LIMITS, () -> {});
            String digest = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(jar)));
            assertThat(observed.getArtifactSha256()).isEqualTo(digest);
            assertThat(observed.getVersion()).isEqualTo("file-sha256:" + digest);
            assertThat(observed.getName()).isEqualTo("fixture-artifact");
        }
    }
    @Test void observesExplodedClassOriginAndHashesResourcesIndependentlyOfDirectoryLocation() throws Exception {
        var classes = compile();
        Files.writeString(classes.resolve("resource.txt"), "resource");
        var copy = Files.createDirectories(directory.resolve("copy/fixture")).getParent();
        Files.writeString(copy.resolve("resource.txt"), "resource");
        Files.copy(classes.resolve("fixture/Anchor.class"), copy.resolve("fixture/Anchor.class"));
        try (var loader = new java.net.URLClassLoader(new java.net.URL[]{classes.toUri().toURL()}, null)) {
            var anchor = Class.forName("fixture.Anchor", false, loader);
            var first = DocumentRuntimeArtifact.observe("fixture-artifact", anchor, LIMITS, () -> {});
            var second = DocumentRuntimeArtifact.observe("fixture-artifact", copy, LIMITS, () -> {});
            assertThat(first).isEqualTo(second);
            assertThat(first.getVersion()).startsWith("tree-v1-sha256:");
            Files.writeString(copy.resolve("resource.txt"), "different");
            assertThat(DocumentRuntimeArtifact.observe("fixture-artifact", copy, LIMITS, () -> {}).getArtifactSha256())
                    .isNotEqualTo(first.getArtifactSha256());
        }
    }
    @Test void treeNamesAreBoundToContentAndLinksAreRefused() throws Exception {
        var root = Files.createDirectory(directory.resolve("tree"));
        Files.writeString(root.resolve("a"), "data");
        var before = DocumentRuntimeArtifact.observe("fixture", root, LIMITS, () -> {});
        Files.createDirectory(root.resolve("empty"));
        assertThat(DocumentRuntimeArtifact.observe("fixture", root, LIMITS, () -> {})).isEqualTo(before);
        Files.move(root.resolve("a"), root.resolve("b"));
        assertThat(DocumentRuntimeArtifact.observe("fixture", root, LIMITS, () -> {}).getArtifactSha256()).isNotEqualTo(before.getArtifactSha256());
        Files.createSymbolicLink(root.resolve("link"), root.resolve("b"));
        assertThatThrownBy(() -> DocumentRuntimeArtifact.observe("fixture", root, LIMITS, () -> {})).hasMessageContaining("link");
        assertThatThrownBy(() -> DocumentRuntimeArtifact.observe("fixture", root.resolve("link"), LIMITS, () -> {})).hasMessageContaining("links");
    }
    @Test void boundsIncludeEmptyDirectoriesAndDoNotSilentlyTruncateDepth() throws Exception {
        var root = Files.createDirectory(directory.resolve("tree"));
        Files.writeString(root.resolve("data"), "1234");
        assertThatThrownBy(() -> DocumentRuntimeArtifact.observe("fixture", root, new DocumentRuntimeArtifact.Limits(3, 10, 3), () -> {}))
                .hasMessageContaining("byte bound");
        Files.createDirectories(root.resolve("a/b/c"));
        assertThatThrownBy(() -> DocumentRuntimeArtifact.observe("fixture", root, new DocumentRuntimeArtifact.Limits(100, 2, 8), () -> {}))
                .hasMessageContaining("entry bound");
        assertThatThrownBy(() -> DocumentRuntimeArtifact.observe("fixture", root, new DocumentRuntimeArtifact.Limits(100, 10, 2), () -> {}))
                .hasMessageContaining("depth bound");
    }
    @Test void refusesChangedFileAndCancellationWithoutInventingAnIdentity() throws Exception {
        var file = directory.resolve("artifact"); Files.writeString(file, "original");
        var calls = new AtomicInteger();
        assertThatThrownBy(() -> DocumentRuntimeArtifact.observe("fixture", file, LIMITS, () -> {
            // Change after initial stat, at the pre-read control boundary.
            if (calls.incrementAndGet() == 2) {
                try { Files.writeString(file, "changed content"); }
                catch (java.io.IOException failed) { throw new java.io.UncheckedIOException(failed); }
            }
        })).hasMessageContaining("changed before read");
        var cancelled = new java.util.concurrent.CancellationException("stop");
        assertThatThrownBy(() -> DocumentRuntimeArtifact.observe("fixture", file, LIMITS, () -> { throw cancelled; })).isSameAs(cancelled);
        assertThat(DocumentRuntimeArtifact.observe("fixture", file, LIMITS, () -> {}).getArtifactSha256()).hasSize(64);
    }
    @Test void rejectsMissingUnsupportedAndEmptyOrigins() throws Exception {
        assertThatThrownBy(() -> DocumentRuntimeArtifact.observe("fixture", String.class, LIMITS, () -> {})).hasMessageContaining("no local code origin");
        assertThatThrownBy(() -> DocumentRuntimeArtifact.observe("fixture", directory.resolve("missing"), LIMITS, () -> {}))
                .isInstanceOf(java.nio.file.NoSuchFileException.class);
        assertThatThrownBy(() -> DocumentRuntimeArtifact.observe("fixture", Files.createDirectory(directory.resolve("empty")), LIMITS, () -> {}))
                .hasMessageContaining("Empty runtime artifact");
        assertThatThrownBy(() -> DocumentRuntimeArtifact.observe(" ", directory, LIMITS, () -> {})).hasMessageContaining("name");
    }
    private Path compile() throws Exception {
        var source = directory.resolve("Anchor.java");
        Files.writeString(source, "package fixture; public final class Anchor {}", StandardCharsets.UTF_8);
        var classes = Files.createDirectory(directory.resolve("classes"));
        var compiler = javax.tools.ToolProvider.getSystemJavaCompiler();
        assertThat(compiler).isNotNull();
        assertThat(compiler.run(null, null, null, "-d", classes.toString(), source.toString())).isZero();
        return classes;
    }
}
