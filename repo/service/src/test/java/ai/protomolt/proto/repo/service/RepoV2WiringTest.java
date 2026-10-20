package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.container.ledger.DriveRecord;
import ai.protomolt.proto.repo.stack.Stack;
import ai.protomolt.proto.repo.v2.StackRef;
import java.io.InputStream;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for the non-regex leading-slash strip that replaced
 * {@code replaceFirst("^/+", "")} in the drive-prefix normalization.
 */
class RepoV2WiringTest {

    @Test
    void stripsLeadingSlashesExactlyLikeTheRegexItReplaced() {
        assertThat(RepoV2Wiring.stripLeadingSlashes("")).isEqualTo("");
        assertThat(RepoV2Wiring.stripLeadingSlashes("/")).isEqualTo("");
        assertThat(RepoV2Wiring.stripLeadingSlashes("///")).isEqualTo("");
        assertThat(RepoV2Wiring.stripLeadingSlashes("/a")).isEqualTo("a");
        assertThat(RepoV2Wiring.stripLeadingSlashes("//a/b/")).isEqualTo("a/b/");
        assertThat(RepoV2Wiring.stripLeadingSlashes("a/b")).isEqualTo("a/b");
        assertThat(RepoV2Wiring.stripLeadingSlashes("a//")).isEqualTo("a//");
        assertThat(RepoV2Wiring.stripLeadingSlashes(" /a")).isEqualTo(" /a");
    }

    @Test
    void registerStackRunsOnceAcrossRepeatedResolvesOfOneDrive() {
        var registrations = new AtomicInteger();
        var opened = new AtomicInteger();
        var stacksById = new ConcurrentHashMap<String, Stack>();
        var resolver = RepoV2Wiring.driveStacks(
                record -> { registrations.incrementAndGet(); return record.getStackId(); },
                (id, record, prefix) -> { opened.incrementAndGet(); return stack(id); },
                "s3", null, stacksById);
        var record = drive(UUID.randomUUID(), "bucket-a", "/docs/");

        var first = resolver.apply(record);
        assertThat(resolver.apply(record)).isSameAs(first);
        assertThat(resolver.apply(record)).isSameAs(first);
        assertThat(registrations.get()).as("one catalog registration for three resolves").isEqualTo(1);
        assertThat(opened.get()).isEqualTo(1);
        assertThat(stacksById).containsOnlyKeys("stack-" + record.driveId);
    }

    @Test
    void twoDrivesNamingOneNamespaceShareTheCatalogsCanonicalStack() {
        // registerStack answers with the namespace's recorded id, which can
        // belong to another drive; the resolver must honor that canonical id.
        var registrations = new AtomicInteger();
        var opened = new AtomicInteger();
        var stacksById = new ConcurrentHashMap<String, Stack>();
        var resolver = RepoV2Wiring.driveStacks(
                record -> { registrations.incrementAndGet(); return "stack-canonical"; },
                (id, record, prefix) -> { opened.incrementAndGet(); return stack(id); },
                "s3", null, stacksById);

        var first = resolver.apply(drive(UUID.randomUUID(), "shared", ""));
        var second = resolver.apply(drive(UUID.randomUUID(), "shared", ""));
        assertThat(second).isSameAs(first);
        assertThat(first.stackId()).isEqualTo("stack-canonical");
        assertThat(registrations.get()).as("each drive registers once").isEqualTo(2);
        assertThat(opened.get()).as("one stack serves the shared namespace").isEqualTo(1);
    }

    @Test
    void aRegistrationFailureCachesNothingAndTheNextResolveRetries() {
        var registrations = new AtomicInteger();
        var stacksById = new ConcurrentHashMap<String, Stack>();
        var resolver = RepoV2Wiring.driveStacks(
                record -> {
                    if (registrations.incrementAndGet() == 1) throw new IllegalStateException("catalog down");
                    return record.getStackId();
                },
                (id, record, prefix) -> stack(id),
                "s3", null, stacksById);
        var record = drive(UUID.randomUUID(), "bucket-b", "");

        assertThatThrownBy(() -> resolver.apply(record))
                .isInstanceOf(IllegalStateException.class).hasMessage("catalog down");
        assertThat(stacksById).as("the failed resolve cached nothing").isEmpty();
        assertThat(resolver.apply(record).stackId()).isEqualTo("stack-" + record.driveId);
        assertThat(registrations.get()).isEqualTo(2);
    }

    private static DriveRecord drive(UUID driveId, String bucket, String prefix) {
        var record = new DriveRecord();
        record.driveId = driveId;
        record.bucket = bucket;
        record.prefix = prefix;
        return record;
    }

    /** A stack stub: the resolver tests exercise caching, never storage. */
    private static Stack stack(String stackId) {
        return new Stack() {
            @Override public String stackId() { return stackId; }
            @Override public StackRef put(InputStream body, long expectedLength) { throw new UnsupportedOperationException(); }
            @Override public StackRef put(byte[] body) { throw new UnsupportedOperationException(); }
            @Override public InputStream open(StackRef ref) { throw new UnsupportedOperationException(); }
            @Override public InputStream openRange(StackRef ref, long offset, long length) { throw new UnsupportedOperationException(); }
            @Override public long head(StackRef ref) { throw new UnsupportedOperationException(); }
            @Override public void delete(StackRef ref) { throw new UnsupportedOperationException(); }
        };
    }

    @Test
    void matchesTheReplacedRegexOnRepresentativeInputs() {
        // The oracle uses the regex the production code may no longer use.
        for (String input : new String[] {"", "/", "///", "/a", "//a/b/", "a/b", "a//", " /a", "/ /"}) {
            assertThat(RepoV2Wiring.stripLeadingSlashes(input))
                    .as("input '%s'", input)
                    .isEqualTo(input.replaceFirst("^/+", ""));
        }
    }
}
