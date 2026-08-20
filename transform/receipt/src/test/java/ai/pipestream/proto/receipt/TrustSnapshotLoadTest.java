package ai.pipestream.proto.receipt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.util.JsonFormat;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The pinned-file custody model: extension-decided formats, verified on load. */
class TrustSnapshotLoadTest {

    private static TrustSnapshot snapshot() {
        return ConformanceCorpus.trust();
    }

    @Test
    void aJsonSnapshotLoadsAndVerifies(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("trust.json");
        Files.writeString(file, JsonFormat.printer().print(snapshot()));
        assertThat(TrustSnapshots.load(file)).isEqualTo(snapshot());
    }

    @Test
    void aBinarySnapshotLoadsAndVerifies(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("trust.binpb");
        Files.write(file, snapshot().toByteArray());
        assertThat(TrustSnapshots.load(file)).isEqualTo(snapshot());
    }

    @Test
    void anUnknownJsonFieldRefusesInsteadOfDroppingIt(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("trust.json");
        Files.writeString(file, "{\"issuers\": [], \"trustsEverything\": true}");
        assertThatThrownBy(() -> TrustSnapshots.load(file))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not parse");
    }

    @Test
    void anUnrecognizedExtensionRefusesNamingTheAcceptedForms(@TempDir Path dir)
            throws Exception {
        Path file = dir.resolve("trust.yaml");
        Files.writeString(file, "issuers: []");
        assertThatThrownBy(() -> TrustSnapshots.load(file))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(".json, .binpb, or .pb");
    }

    @Test
    void aLoadedSnapshotStillFailsWellFormedness(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("trust.binpb");
        Files.write(file, TrustSnapshot.getDefaultInstance().toByteArray());
        assertThatThrownBy(() -> TrustSnapshots.load(file))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid");
    }
}
