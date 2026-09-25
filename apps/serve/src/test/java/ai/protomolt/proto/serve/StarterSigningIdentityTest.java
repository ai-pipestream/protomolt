package ai.protomolt.proto.serve;

import ai.protomolt.proto.receipt.TrustSnapshots;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StarterSigningIdentityTest {
    @TempDir Path directory;

    @Test
    void persistsOneMatchingIdentityAcrossRestarts() throws Exception {
        StarterSigningIdentity.ensure(directory);
        byte[] originalSeed = Files.readAllBytes(directory.resolve("seed.bin"));
        byte[] originalTrust = Files.readAllBytes(directory.resolve("trust.binpb"));
        StarterSigningIdentity.ensure(directory);
        assertThat(Files.readAllBytes(directory.resolve("seed.bin"))).isEqualTo(originalSeed);
        assertThat(Files.readAllBytes(directory.resolve("trust.binpb"))).isEqualTo(originalTrust);
        assertThat(TrustSnapshots.load(directory.resolve("trust.binpb"))
                .getIssuers(0).getKeys(0).getPublicKey().toByteArray())
                .isEqualTo(Files.readAllBytes(directory.resolve("public.raw")));
    }

    @Test
    void refusesIncompleteOrMismatchedPersistedIdentity() throws Exception {
        StarterSigningIdentity.ensure(directory);
        byte[] originalSeed = Files.readAllBytes(directory.resolve("seed.bin"));
        byte[] wrongSeed = originalSeed.clone();
        wrongSeed[0] ^= 1;
        Files.write(directory.resolve("seed.bin"), wrongSeed);
        assertThatThrownBy(() -> StarterSigningIdentity.ensure(directory))
                .hasMessageContaining("does not match its public trust key");
        Files.write(directory.resolve("seed.bin"), originalSeed);
        Files.delete(directory.resolve("trust.binpb"));
        assertThatThrownBy(() -> StarterSigningIdentity.ensure(directory))
                .hasMessageContaining("incomplete");
    }
}
