package ai.protomolt.proto.repo.container.ledger;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.net.URLClassLoader;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.*;

class DocumentRuntimeInventoryTest {
    @TempDir Path directory;
    private static final String HEADER = "protomolt-admission-runtime-inventory/v1\n";
    private record Fixture(Path root, Path jar, String hash, long size) {
        String row(String name) { return name + "\t" + hash + "\t" + size + "\tartifacts/" + hash + ".jar\n"; }
        Path blob() { return root.resolve("artifacts/" + hash + ".jar"); }
        Path manifest() { return root.resolve("inventory.tsv"); }
    }

    @Test void verifiesRealLoadedOriginAndAllowsDistinctIdentitiesForSameBlob() throws Exception {
        var f = fixture();
        Files.writeString(f.manifest(), HEADER + f.row("a") + f.row("b"));
        var inventory = DocumentRuntimeInventory.read(f.root(), () -> {});
        assertThat(inventory.identities()).extracting(it -> it.getName()).containsExactly("a", "b");
        try (var loader = loader(f.jar())) {
            var anchor = Class.forName("fixture.Anchor", false, loader);
            inventory.verifyAnchors(List.of(new DocumentRuntimeInventory.Anchor("a", anchor)), () -> {});
            assertThatThrownBy(() -> inventory.verifyAnchors(List.of(new DocumentRuntimeInventory.Anchor("missing", anchor)), () -> {}))
                    .hasMessageContaining("absent");
            assertThatThrownBy(() -> inventory.verifyAnchors(List.of(new DocumentRuntimeInventory.Anchor("a", anchor),
                    new DocumentRuntimeInventory.Anchor("b", anchor)), () -> {})).hasMessageContaining("Duplicate");
        }
        assertThatThrownBy(() -> inventory.verifyAnchors(List.of(), () -> {})).hasMessageContaining("count");
        assertThatThrownBy(() -> inventory.verifyAnchors(List.of(new DocumentRuntimeInventory.Anchor("a", String.class)), () -> {}))
                .hasMessageContaining("no local code origin");
    }

    @Test void rejectsDifferentActualOriginEvenWhenBundleIsValid() throws Exception {
        var f = fixture();
        var inventory = DocumentRuntimeInventory.read(f.root(), () -> {});
        var changed = directory.resolve("changed.jar");
        jar(changed, directory.resolve("classes"), "changed");
        try (var loader = loader(changed)) {
            var anchor = Class.forName("fixture.Anchor", false, loader);
            assertThatThrownBy(() -> inventory.verifyAnchors(List.of(new DocumentRuntimeInventory.Anchor("a", anchor)), () -> {}))
                    .hasMessageContaining("does not match");
        }
    }

    @Test void rejectsNoncanonicalGrammarBeforeOpeningArtifacts() throws Exception {
        var f = fixture();
        Files.delete(f.blob());
        var row = f.row("a");
        var malformed = List.of(
                HEADER + row + row, HEADER + f.row("b") + row,
                HEADER + row.stripTrailing(), HEADER + row + "\n", "\ufeff" + HEADER + row,
                HEADER + row.replace("\t" + f.size() + "\t", "\t0" + f.size() + "\t"),
                HEADER + row.replace("\t" + f.size() + "\t", "\t0\t"),
                HEADER + row.replace("artifacts/", "../artifacts/"),
                HEADER + f.row("bad\u0000name"), HEADER + row.replace("\n", "\r\n"),
                HEADER + row.repeat(65), HEADER + row.replace("\t" + f.size() + "\t", "\t1073741825\t"));
        for (String text : malformed) {
            Files.writeString(f.manifest(), text);
            assertThatThrownBy(() -> DocumentRuntimeInventory.read(f.root(), () -> {}))
                    .isInstanceOf(java.io.IOException.class).isNotInstanceOf(java.nio.file.NoSuchFileException.class);
        }
        Files.write(f.manifest(), new byte[]{(byte) 0xc3, 0x28});
        assertThatThrownBy(() -> DocumentRuntimeInventory.read(f.root(), () -> {}))
                .isInstanceOf(java.nio.charset.CharacterCodingException.class);
        Files.writeString(f.manifest(), "x".repeat(128 * 1024 + 1));
        assertThatThrownBy(() -> DocumentRuntimeInventory.read(f.root(), () -> {})).hasMessageContaining("manifest byte bound");
    }

    @Test void refusesCorruptionMissingFilesAndLinks() throws Exception {
        var f = fixture();
        byte[] original = Files.readAllBytes(f.blob());
        byte[] altered = original.clone(); altered[altered.length / 2] ^= 1;
        Files.write(f.blob(), altered);
        assertThatThrownBy(() -> DocumentRuntimeInventory.read(f.root(), () -> {})).hasMessageContaining("hash");
        Files.write(f.blob(), new byte[]{1});
        assertThatThrownBy(() -> DocumentRuntimeInventory.read(f.root(), () -> {})).hasMessageContaining("size");
        Files.delete(f.blob());
        assertThatThrownBy(() -> DocumentRuntimeInventory.read(f.root(), () -> {})).isInstanceOf(java.nio.file.NoSuchFileException.class);
        Files.createSymbolicLink(f.blob(), f.jar());
        assertThatThrownBy(() -> DocumentRuntimeInventory.read(f.root(), () -> {})).hasMessageContaining("without links");
        Files.delete(f.blob()); Files.write(f.blob(), original);
        Files.createSymbolicLink(directory.resolve("alias"), f.root());
        assertThatThrownBy(() -> DocumentRuntimeInventory.read(directory.resolve("alias"), () -> {})).hasMessageContaining("without links");
        Files.delete(f.manifest()); Files.createSymbolicLink(f.manifest(), f.jar());
        assertThatThrownBy(() -> DocumentRuntimeInventory.read(f.root(), () -> {})).hasMessageContaining("without links");
    }

    @Test void refusesSelfConsistentNonArchiveAndPreservesCancellation() throws Exception {
        var f = fixture();
        byte[] content = "not an archive".getBytes(StandardCharsets.UTF_8);
        String hash = hash(content);
        Files.write(f.root().resolve("artifacts/" + hash + ".jar"), content);
        Files.writeString(f.manifest(), HEADER + "a\t" + hash + "\t" + content.length + "\tartifacts/" + hash + ".jar\n");
        assertThatThrownBy(() -> DocumentRuntimeInventory.read(f.root(), () -> {})).isInstanceOf(java.util.zip.ZipException.class);
        Files.writeString(f.manifest(), HEADER + f.row("a"));
        var cancelled = new java.util.concurrent.CancellationException("stop");
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        assertThatThrownBy(() -> DocumentRuntimeInventory.read(f.root(), () -> {
            if (calls.incrementAndGet() == 5) throw cancelled;
        })).isSameAs(cancelled);
        assertThat(DocumentRuntimeInventory.read(f.root(), () -> {}).identities()).hasSize(1);
    }

    private Fixture fixture() throws Exception {
        var source = directory.resolve("Anchor.java");
        Files.writeString(source, "package fixture; public final class Anchor {}", StandardCharsets.UTF_8);
        var classes = Files.createDirectory(directory.resolve("classes"));
        var compiler = javax.tools.ToolProvider.getSystemJavaCompiler();
        assertThat(compiler).isNotNull();
        assertThat(compiler.run(null, null, null, "-d", classes.toString(), source.toString())).isZero();
        var jar = directory.resolve("original.jar"); jar(jar, classes, "original");
        byte[] content = Files.readAllBytes(jar);
        var root = Files.createDirectory(directory.resolve("bundle")); Files.createDirectory(root.resolve("artifacts"));
        var f = new Fixture(root, jar, hash(content), content.length);
        Files.write(f.blob(), content); Files.writeString(f.manifest(), HEADER + f.row("a"));
        return f;
    }
    private static URLClassLoader loader(Path jar) throws Exception {
        return new URLClassLoader(new java.net.URL[]{jar.toUri().toURL()}, null);
    }
    private static void jar(Path path, Path classes, String resource) throws Exception {
        try (var output = new JarOutputStream(Files.newOutputStream(path))) {
            output.putNextEntry(new JarEntry("fixture/Anchor.class"));
            output.write(Files.readAllBytes(classes.resolve("fixture/Anchor.class"))); output.closeEntry();
            output.putNextEntry(new JarEntry("resource.txt"));
            output.write(resource.getBytes(StandardCharsets.UTF_8)); output.closeEntry();
        }
    }
    private static String hash(byte[] content) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(content));
    }
}
