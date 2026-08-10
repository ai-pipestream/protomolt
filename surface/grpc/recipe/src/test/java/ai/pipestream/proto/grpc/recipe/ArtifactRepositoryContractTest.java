package ai.pipestream.proto.grpc.recipe;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ArtifactRepositoryContractTest {

    @Test
    void storedArtifactDefensivelyCopiesBytes() {
        byte[] source = "abc".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        ArtifactRepository.StoredArtifact stored = new ArtifactRepository.StoredArtifact(
                TestRecipes.artifact("abc", true), source);
        source[0] = 'z';
        byte[] returned = stored.content();
        returned[1] = 'z';

        assertThat(stored.content()).containsExactly('a', 'b', 'c');
    }
}
