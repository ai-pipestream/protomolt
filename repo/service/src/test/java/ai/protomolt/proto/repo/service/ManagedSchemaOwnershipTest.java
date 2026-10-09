package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.admission.*;
import ai.protomolt.proto.repo.schema.registry.RegistrySchemaResolver;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.DocumentPublicationMember;
import ai.protomolt.proto.schema.registry.git.GitSchemaRegistryStore;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.*;

class ManagedSchemaOwnershipTest {
    @TempDir Path directory;

    @Test void failedCompositionLeavesActualResolverUsableByItsCaller() throws Exception {
        try (var git = GitSchemaRegistryStore.builder().repositoryDir(directory).build();
             var resolver = new RegistrySchemaResolver(git, new DocumentSchemaArtifactCache.Limits(8_000_000, 16, 4_000_000), 4, 16)) {
            var closes = new java.util.concurrent.atomic.AtomicInteger();
            var access = access(resolver, closes);
            var ownership = new ManagedSchemaOwnership(access);
            ownership.closeAdmission();
            ownership.closeAdmission();
            assertThat(closes.get()).isZero();
            assertThat(ownership.awaitIdle(Duration.ZERO)).isTrue();
            try (var scope = access.open(null, null, RepositoryReadControl.NONE)) {
                assertThat(scope).isNotNull();
            }
            assertThatThrownBy(ownership::transfer).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test void successfulTransferClosesActualResolverAndCannotTransferTwice() throws Exception {
        try (var git = GitSchemaRegistryStore.builder().repositoryDir(directory).build();
             var resolver = new RegistrySchemaResolver(git, new DocumentSchemaArtifactCache.Limits(8_000_000, 16, 4_000_000), 4, 16)) {
            var closes = new java.util.concurrent.atomic.AtomicInteger();
            var access = access(resolver, closes);
            var ownership = new ManagedSchemaOwnership(access);
            ownership.transfer();
            assertThatThrownBy(ownership::transfer).isInstanceOf(IllegalStateException.class);
            ownership.closeAdmission();
            ownership.closeAdmission();
            assertThat(closes.get()).isEqualTo(1);
            assertThat(ownership.awaitIdle(Duration.ofSeconds(5))).isTrue();
            assertThatThrownBy(() -> access.open(null, null, RepositoryReadControl.NONE))
                    .isInstanceOf(IllegalStateException.class).hasMessage("Registry resolver closed");
        }
    }

    private static ManagedSchemaAccess access(RegistrySchemaResolver resolver, java.util.concurrent.atomic.AtomicInteger closes) {
        var definition = BuiltinDocumentSchema.definition();
        return new ManagedSchemaAccess() {
            public DocumentSchemaAdmission.Resolution open(RepositoryCaller caller, DocumentPublicationMember member, RepositoryReadControl control) {
                return resolver.open(occurrence -> new RegistrySchemaResolver.Selected(definition.metadata(), definition.source()), control::check);
            }
            public void close() { closes.incrementAndGet(); resolver.close(); }
            public boolean awaitIdle(Duration timeout) throws InterruptedException { return resolver.awaitLoads(timeout); }
        };
    }

    @Test void closeFailureIsReportedAndRetriedBeforeRecordingCompletion() throws Exception {
        try (var git = GitSchemaRegistryStore.builder().repositoryDir(directory).build();
             var resolver = new RegistrySchemaResolver(git, new DocumentSchemaArtifactCache.Limits(8_000_000, 16, 4_000_000), 4, 16)) {
            var closes = new java.util.concurrent.atomic.AtomicInteger();
            var delegate = access(resolver, closes);
            var ownership = new ManagedSchemaOwnership(new ManagedSchemaAccess() {
                public DocumentSchemaAdmission.Resolution open(RepositoryCaller caller, DocumentPublicationMember member, RepositoryReadControl control) {
                    return delegate.open(caller, member, control);
                }
                public void close() {
                    delegate.close();
                    if (closes.get() == 1) throw new IllegalStateException("injected schema close acknowledgement loss");
                }
                public boolean awaitIdle(Duration timeout) throws InterruptedException { return delegate.awaitIdle(timeout); }
            });
            ownership.transfer();
            assertThatThrownBy(ownership::closeAdmission).isInstanceOf(IllegalStateException.class)
                    .hasMessage("injected schema close acknowledgement loss");
            ownership.closeAdmission();
            ownership.closeAdmission();
            assertThat(closes.get()).isEqualTo(2);
            assertThat(ownership.awaitIdle(Duration.ofSeconds(5))).isTrue();
        }
    }
}
