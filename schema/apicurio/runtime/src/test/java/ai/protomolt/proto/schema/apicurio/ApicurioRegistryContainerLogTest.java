package ai.protomolt.proto.schema.apicurio;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.output.OutputFrame;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class ApicurioRegistryContainerLogTest {

    @TempDir
    Path tempDir;

    @Test
    void writesOnlyTheMostRecentBytes() throws Exception {
        ApicurioRegistryContainer.BoundedLogTail logTail = new ApicurioRegistryContainer.BoundedLogTail(8);
        logTail.accept(frame("abcde"));
        logTail.accept(OutputFrame.END);
        logTail.accept(frame("fghijkl"));

        Path destination = tempDir.resolve("diagnostics/registry.log");
        logTail.writeTo(destination);

        assertThat(Files.readString(destination, StandardCharsets.UTF_8)).isEqualTo("efghijkl");
        assertThat(Files.size(destination)).isEqualTo(8);
    }

    private static OutputFrame frame(String text) {
        return new OutputFrame(OutputFrame.OutputType.STDOUT, text.getBytes(StandardCharsets.UTF_8));
    }
}
