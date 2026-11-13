package ai.protomolt.proto.repo.accession;

import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.stack.Stack;
import ai.protomolt.proto.repo.v2.StackRef;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** Only explicitly known stacks can be classified as durable. */
class DrivesTest {
    private static final Stack DURABLE = new MemoryStack("stack-durable");
    private static final Stack TEMPORARY = new MemoryStack("stack-temporary");

    @Test
    void staticTablesClassifyOnlyRegisteredStacksAsDurable() {
        var drives = new DocumentSaves.Drives("durable", Map.of("durable", DURABLE));
        assertThat(drives.temporary(DURABLE)).isFalse();
        assertUnknown(drives, TEMPORARY);
        assertUnknown(drives, new MemoryStack("stack-unknown"));
    }

    @Test
    void dynamicDurableResolverRequiresAKnownStack() {
        var drives = DocumentSaves.Drives.dynamic("durable", (account, drive) -> Optional.of(DURABLE),
                stackId -> stackId.equals(DURABLE.stackId()) ? Optional.of(DURABLE) : Optional.empty());
        assertThat(drives.temporary(DURABLE)).isFalse();
        assertUnknown(drives, TEMPORARY);
    }

    @Test
    void dynamicResolverRequiresExplicitClassification() {
        var drives = DocumentSaves.Drives.dynamic("durable",
                (account, drive) -> Optional.of(DURABLE), stackId -> Optional.of(DURABLE),
                stackId -> stackId.equals(TEMPORARY.stackId()) ? Optional.of(true)
                        : stackId.equals(DURABLE.stackId()) ? Optional.of(false) : Optional.empty());
        assertThat(drives.temporary(TEMPORARY)).isTrue();
        assertThat(drives.temporary(DURABLE)).isFalse();
        assertUnknown(drives, new MemoryStack("stack-unknown"));
    }

    private static void assertUnknown(DocumentSaves.Drives drives, Stack stack) {
        assertThatThrownBy(() -> drives.temporary(stack))
                .isInstanceOfSatisfying(AccessionException.class,
                        failure -> assertThat(failure.code()).isEqualTo(io.grpc.Status.Code.UNAVAILABLE));
    }

    private static final class MemoryStack implements Stack {
        private final String stackId;

        MemoryStack(String stackId) { this.stackId = stackId; }

        @Override public String stackId() { return stackId; }
        @Override public StackRef put(InputStream body, long expectedLength) { throw new UnsupportedOperationException(); }
        @Override public StackRef put(byte[] body) { throw new UnsupportedOperationException(); }
        @Override public InputStream open(StackRef ref) { return new ByteArrayInputStream(new byte[0]); }
        @Override public InputStream openRange(StackRef ref, long offset, long length) { return new ByteArrayInputStream(new byte[0]); }
        @Override public long head(StackRef ref) { return 0; }
        @Override public void delete(StackRef ref) throws IOException {}
    }
}
