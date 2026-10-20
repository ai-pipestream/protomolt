package ai.protomolt.proto.repo.stack;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.repo.v2.StackRecord;
import ai.protomolt.proto.repo.v2.StackRef;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Provider selection for a catalog stack record: providers register under a strict
 * ID, the record names its provider with no fallback, and a provider that opens a
 * stack other than the one the record names is closed and refused.
 */
class StackProvidersTest {
    private static final StackRecord RECORD = StackRecord.newBuilder().setStackId("stack-1").setProvider("fake")
            .setNamespace("bucket").build();

    @Test
    void discoverFindsTheBundledProvider() {
        assertThat(StackProviders.discover().ids()).containsExactly("s3");
    }

    @Test
    void registersEachProviderUnderItsId() {
        var providers = StackProviders.of(List.of(new FakeProvider("fake"), new FakeProvider("rust-fs2")));
        assertThat(providers.ids()).containsExactlyInAnyOrder("fake", "rust-fs2");
    }

    @Test
    void noProvidersRegistersNothing() {
        assertThat(StackProviders.of(List.of()).ids()).isEmpty();
    }

    @Test
    void duplicateIdIsRefused() {
        assertThatThrownBy(() -> StackProviders.of(List.of(new FakeProvider("s3"), new FakeProvider("s3"))))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Duplicate stack provider: s3");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "S3", "3s", "-s3", "s_3", "s3.", "s 3", "étag"})
    void invalidIdIsRefused(String id) {
        assertThatThrownBy(() -> StackProviders.of(List.of(new FakeProvider(id))))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid stack provider ID");
    }

    @Test
    void nullIdIsRefused() {
        assertThatThrownBy(() -> StackProviders.of(List.of(new FakeProvider(null))))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid stack provider ID");
    }

    @ParameterizedTest
    @ValueSource(strings = {"s", "s3", "rust-fs"})
    void lowercaseLetterFirstIdsAreAccepted(String id) {
        assertThat(StackProviders.of(List.of(new FakeProvider(id))).ids()).containsExactly(id);
    }

    @Test
    void nullProviderListAndNullProviderAreRefused() {
        assertThatThrownBy(() -> StackProviders.of(null))
                .isExactlyInstanceOf(NullPointerException.class).hasMessage("providers");
        assertThatThrownBy(() -> StackProviders.of(Arrays.asList(new FakeProvider("fake"), null)))
                .isExactlyInstanceOf(NullPointerException.class).hasMessage("provider");
    }

    @Test
    void openUsesTheNamedProviderWithACopyOfTheOptions() throws IOException {
        var stack = new FakeStack("stack-1");
        var provider = new FakeProvider("fake", stack);
        var options = new HashMap<String, String>();
        options.put("region", "us-east-1");

        var opened = StackProviders.of(List.of(new FakeProvider("other"), provider)).open(RECORD, options);

        assertThat(opened).isSameAs(stack);
        assertThat(provider.records).containsExactly(RECORD);
        var received = provider.options.getFirst();
        assertThat(received).isEqualTo(Map.of("region", "us-east-1")).isNotSameAs(options);
        assertThatThrownBy(() -> received.put("region", "eu-west-1"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(stack.closes).isZero();
    }

    @Test
    void uninstalledProviderIsRefusedWithoutFallback() {
        var installed = new FakeProvider("s3", new FakeStack("stack-1"));
        assertThatThrownBy(() -> StackProviders.of(List.of(installed)).open(RECORD, Map.of()))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Stack provider is not installed: fake");
        assertThat(installed.records).isEmpty();
    }

    @Test
    void providerReturningNoStackIsRefused() {
        var provider = new FakeProvider("fake");
        assertThatThrownBy(() -> StackProviders.of(List.of(provider)).open(RECORD, Map.of()))
                .isExactlyInstanceOf(NullPointerException.class)
                .hasMessage("Stack provider returned no stack");
    }

    @Test
    void providerOpenFailurePropagates() {
        var failure = new IOException("endpoint refused");
        var provider = new FakeProvider("fake") {
            @Override public Stack open(StackRecord record, Map<String, String> options) throws IOException {
                throw failure;
            }
        };
        assertThatThrownBy(() -> StackProviders.of(List.of(provider)).open(RECORD, Map.of())).isSameAs(failure);
    }

    @Test
    void anotherStackIdClosesTheStackAndIsRefused() {
        var stack = new FakeStack("stack-2");
        assertThatThrownBy(() -> StackProviders.of(List.of(new FakeProvider("fake", stack))).open(RECORD, Map.of()))
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessage("Stack provider opened another stack ID")
                .satisfies(failure -> assertThat(failure.getSuppressed()).isEmpty());
        assertThat(stack.closes).isEqualTo(1);
    }

    @Test
    void anotherStackIdKeepsTheCloseFailureAsSuppressed() {
        var cleanup = new IOException("release failed");
        var stack = new FakeStack("stack-2", cleanup);
        assertThatThrownBy(() -> StackProviders.of(List.of(new FakeProvider("fake", stack))).open(RECORD, Map.of()))
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessage("Stack provider opened another stack ID")
                .satisfies(failure -> assertThat(failure.getSuppressed()).containsExactly(cleanup));
        assertThat(stack.closes).isEqualTo(1);
    }

    /** Returns a fixed stack, or none, and records what it was asked to open. */
    private static class FakeProvider implements StackProvider {
        private final String id;
        private final Stack stack;
        final List<StackRecord> records = new ArrayList<>();
        final List<Map<String, String>> options = new ArrayList<>();

        FakeProvider(String id) { this(id, null); }

        FakeProvider(String id, Stack stack) {
            this.id = id;
            this.stack = stack;
        }

        @Override public String id() { return id; }

        @Override public Stack open(StackRecord record, Map<String, String> options) throws IOException {
            records.add(record);
            this.options.add(options);
            return stack;
        }
    }

    /** A stack that only reports its ID and counts closes; its storage operations are not part of selection. */
    private static final class FakeStack implements Stack {
        private final String stackId;
        private final IOException closeFailure;
        int closes;

        FakeStack(String stackId) { this(stackId, null); }

        FakeStack(String stackId, IOException closeFailure) {
            this.stackId = stackId;
            this.closeFailure = closeFailure;
        }

        @Override public String stackId() { return stackId; }

        @Override public void close() throws IOException {
            closes++;
            if (closeFailure != null) throw closeFailure;
        }

        @Override public StackRef put(InputStream body, long expectedLength) { throw unused(); }
        @Override public StackRef put(byte[] body) { throw unused(); }
        @Override public InputStream open(StackRef ref) { throw unused(); }
        @Override public InputStream openRange(StackRef ref, long offset, long length) { throw unused(); }
        @Override public long head(StackRef ref) { throw unused(); }
        @Override public void delete(StackRef ref) { throw unused(); }

        private static AssertionError unused() {
            return new AssertionError("Provider selection must not use stack storage");
        }
    }
}
