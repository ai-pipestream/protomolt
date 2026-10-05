package ai.protomolt.proto.repo.container.ledger;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.*;

class DocumentRuntimeClasspathTest {
    @TempDir Path root;
    private record Fixture(DocumentRuntimeInventory inventory, Path jar) {}

    @Test void requiresEveryArtifactAndRejectsDuplicateProviders() throws Exception {
        var fixture = fixture();
        assertThat(DocumentRuntimeClasspath.verify(fixture.inventory(), List.of(fixture.jar()), () -> {})).hasSize(1);
        var extra = jar("extra.jar", Map.of("other/Class.class", new byte[]{2}));
        assertThat(DocumentRuntimeClasspath.verify(fixture.inventory(), List.of(extra, fixture.jar()), () -> {})).hasSize(1);
        assertThatThrownBy(() -> DocumentRuntimeClasspath.verify(fixture.inventory(), List.of(extra), () -> {})).hasMessageContaining("missing inventory");
        assertThatThrownBy(() -> DocumentRuntimeClasspath.verify(fixture.inventory(), List.of(fixture.jar(), fixture.jar()), () -> {}))
                .hasMessageContaining("Duplicate runtime classpath path");
        var copy = Files.copy(fixture.jar(), root.resolve("copy.jar"));
        assertThatThrownBy(() -> DocumentRuntimeClasspath.verify(fixture.inventory(), List.of(fixture.jar(), copy), () -> {}))
                .hasMessageContaining("Duplicate runtime provider");
    }
    @Test void detectsShadowingInEitherOrderIncludingMultiReleaseEntries() throws Exception {
        var fixture = fixture();
        for (var name : List.of("sample/Class.class", "META-INF/versions/25/sample/Class.class")) {
            var extra = jar(name.startsWith("META") ? "multi.jar" : "shadow.jar", Map.of(name, new byte[]{2}));
            for (var paths : List.of(List.of(extra, fixture.jar()), List.of(fixture.jar(), extra))) {
                assertThatThrownBy(() -> DocumentRuntimeClasspath.verify(fixture.inventory(), paths, () -> {}))
                        .hasMessageContaining("shadows inventory class");
            }
        }
    }
    @Test void refusesManifestClasspathAndBoundsDecompressedManifest() throws Exception {
        var fixture = fixture();
        var extension = jar("extension.jar", Map.of("META-INF/MANIFEST.MF", "Manifest-Version: 1.0\r\nClass-Path: elsewhere.jar\r\n\r\n".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        assertThatThrownBy(() -> DocumentRuntimeClasspath.verify(fixture.inventory(), List.of(fixture.jar(), extension), () -> {}))
                .hasMessageContaining("Manifest Class-Path");
        var huge = jar("huge.jar", Map.of("META-INF/MANIFEST.MF", new byte[262145]));
        assertThatThrownBy(() -> DocumentRuntimeClasspath.verify(fixture.inventory(), List.of(fixture.jar(), huge), () -> {}))
                .hasMessageContaining("manifest exceeds byte bound");
    }
    @Test void acceptsLargeHostMetadataWithoutSkippingClasspathChecks() throws Exception {
        var fixture = fixture();
        var manifest = new java.util.jar.Manifest();
        manifest.getMainAttributes().putValue("Manifest-Version", "1.0");
        manifest.getMainAttributes().putValue("Implementation-Notes", "x".repeat(171463));
        var encoded = new java.io.ByteArrayOutputStream();
        manifest.write(encoded);
        assertThat(encoded.size()).isGreaterThan(65536).isLessThan(262144);
        var metadata = jar("metadata.jar", Map.of("META-INF/MANIFEST.MF", encoded.toByteArray()));
        assertThat(DocumentRuntimeClasspath.verify(fixture.inventory(), List.of(fixture.jar(), metadata), () -> {})).hasSize(1);
        String original = encoded.toString(java.nio.charset.StandardCharsets.UTF_8);
        assertThat(original).endsWith("\r\n\r\n");
        String extended = original.substring(0, original.length() - 2) + "Class-Path: hidden.jar\r\n\r\n";
        assertThat(extended.indexOf("Class-Path:")).isGreaterThan(65536);
        var extension = jar("large-extension.jar", Map.of("META-INF/MANIFEST.MF", extended.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        assertThatThrownBy(() -> DocumentRuntimeClasspath.verify(fixture.inventory(), List.of(fixture.jar(), extension), () -> {}))
                .hasMessageContaining("Manifest Class-Path");
    }
    @Test void refusesLinksDirectoriesEmptyListsAndCancellation() throws Exception {
        var fixture = fixture();
        var link = Files.createSymbolicLink(root.resolve("link.jar"), fixture.jar());
        assertThatThrownBy(() -> DocumentRuntimeClasspath.verify(fixture.inventory(), List.of(link), () -> {})).hasMessageContaining("without links");
        assertThatThrownBy(() -> DocumentRuntimeClasspath.verify(fixture.inventory(), List.of(root), () -> {})).hasMessageContaining("regular JAR");
        assertThatThrownBy(() -> DocumentRuntimeClasspath.verify(fixture.inventory(), List.of(), () -> {})).hasMessageContaining("count");
        var stop = new java.util.concurrent.CancellationException("stop");
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        assertThatThrownBy(() -> DocumentRuntimeClasspath.verify(fixture.inventory(), List.of(fixture.jar()), () -> {
            if (calls.incrementAndGet() == 5) throw stop;
        })).isSameAs(stop);
        assertThat(DocumentRuntimeClasspath.verify(fixture.inventory(), List.of(fixture.jar()), () -> {})).hasSize(1);
    }
    @Test void refusesArtifactChangedBetweenHashAndArchiveScan() throws Exception {
        var fixture = fixture();
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        assertThatThrownBy(() -> DocumentRuntimeClasspath.verify(fixture.inventory(), List.of(fixture.jar()), () -> {
            // This small JAR uses one hash buffer; the next control point starts ZIP observation.
            if (calls.incrementAndGet() == 8) {
                try { Files.write(fixture.jar(), new byte[]{1}); }
                catch (java.io.IOException failure) { throw new java.io.UncheckedIOException(failure); }
            }
        })).hasMessageContaining("changed during observation");
        assertThat(calls.get()).isEqualTo(8);
    }
    private Fixture fixture() throws Exception {
        // The collision scanner inspects archive names, not executable bytecode. Real loading is
        // separately exercised by DocumentAdmissionRuntimeTest with the production bundle.
        var jar = jar("fixture.jar", Map.of("sample/Class.class", new byte[]{1}));
        String hash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(jar)));
        var bundle = Files.createDirectory(root.resolve("bundle")); Files.createDirectory(bundle.resolve("artifacts"));
        Files.copy(jar, bundle.resolve("artifacts/" + hash + ".jar"));
        Files.writeString(bundle.resolve("inventory.tsv"), "protomolt-admission-runtime-inventory/v1\na\t" + hash + "\t" + Files.size(jar) + "\tartifacts/" + hash + ".jar\n");
        return new Fixture(DocumentRuntimeInventory.read(bundle, () -> {}), jar);
    }
    private Path jar(String name, Map<String, byte[]> entries) throws Exception {
        var path = root.resolve(name);
        try (var output = new JarOutputStream(Files.newOutputStream(path))) {
            for (var entry : entries.entrySet()) {
                output.putNextEntry(new JarEntry(entry.getKey())); output.write(entry.getValue()); output.closeEntry();
            }
        }
        return path;
    }
}
