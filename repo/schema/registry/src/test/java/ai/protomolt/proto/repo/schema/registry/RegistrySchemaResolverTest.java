package ai.protomolt.proto.repo.schema.registry;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.registry.RegistryStoreException;
import ai.protomolt.proto.repo.admission.*;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.codec.PartLayouts;
import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.schema.registry.git.GitSchemaRegistryStore;
import com.google.protobuf.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.*;

class RegistrySchemaResolverTest {
    @TempDir Path temp;
    private static final Runnable ACTIVE = () -> {};
    private static final DocumentSchemaArtifactCache.Limits CACHE =
            new DocumentSchemaArtifactCache.Limits(32_000_000, 64, 16_000_000);
    private static final DocumentSchemaAdmission.Limits ADMISSION =
            new DocumentSchemaAdmission.Limits(100, 8_000_000, 10, 8_000_000, 64, 32_000_000, 8_000_000);
    private GitSchemaRegistryStore store() { return GitSchemaRegistryStore.builder().repositoryDir(temp.resolve("registry")).build(); }
    private static RegistrySchemaResolver.Selected selected(DocumentSchemaAdmission.Definition d) {
        return new RegistrySchemaResolver.Selected(d.metadata(), d.source());
    }
    private static DocumentSchemaAdmission.Selection occurrence(DocumentSchemaAdmission.Definition d) {
        return new DocumentSchemaAdmission.Selection(0, DocumentSchemaRootLocator.getDefaultInstance(),
                d.metadata().getTypeUrl(), List.of(), "a".repeat(64), 1);
    }

    @Test void warmCacheStillAuthorizesEverySelectionAndOwnsPinsAcrossHostClose() throws Exception {
        var d = definition(StringValue.getDescriptor());
        try (var store = store(); var host = new RegistrySchemaResolver(store, CACHE, 1, 64)) {
            store.putDescriptorSet(d.metadata().getArtifactSha256(), d.descriptors());
            var calls = new AtomicInteger();
            var denied = new SecurityException("revoked");
            try (var attempt = host.open(o -> {
                if (calls.incrementAndGet() == 3) throw denied;
                return selected(d);
            }, ACTIVE)) {
                var first = attempt.select(occurrence(d));
                // Exact immutable bytes remain usable without another artifact read.
                Files.delete(temp.resolve("registry/descriptors/sha256/" + d.metadata().getArtifactSha256() + ".pb"));
                assertThat(attempt.select(occurrence(d)).descriptors()).isSameAs(first.descriptors());
                assertThatThrownBy(() -> attempt.select(occurrence(d))).isSameAs(denied);
                host.close();
                assertThat(host.cachedBytes()).isEqualTo(d.descriptors().size());
                assertThat(first.descriptors()).isEqualTo(d.descriptors());
                assertThatThrownBy(() -> attempt.select(occurrence(d))).isInstanceOf(IllegalStateException.class);
            }
            assertThat(host.cachedBytes()).isZero();
        }
    }

    @Test void missingOutageAndRecoveryRemainDifferentAndAreNotNegativeCached() throws Exception {
        var d = definition(StringValue.getDescriptor());
        Path root = temp.resolve("registry");
        Path moved = temp.resolve("moved");
        try (var store = store(); var host = new RegistrySchemaResolver(store, CACHE, 1, 64)) {
            try (var attempt = host.open(o -> selected(d), ACTIVE)) {
                assertThatThrownBy(() -> attempt.select(occurrence(d))).isInstanceOf(RegistrySchemaResolver.MissingDescriptor.class);
                Files.move(root, moved);
                assertThatThrownBy(() -> attempt.select(occurrence(d))).isInstanceOf(RegistryStoreException.class);
                Files.move(moved, root);
                store.putDescriptorSet(d.metadata().getArtifactSha256(), d.descriptors());
                assertThat(attempt.select(occurrence(d)).descriptors()).isEqualTo(d.descriptors());
            }
        }
    }

    @Test void equalTypeUrlsKeepDifferentArtifactsAndSeparateHostsDoNotShareThem() throws Exception {
        var d = definition(StringValue.getDescriptor());
        var fds = DescriptorProtos.FileDescriptorSet.parseFrom(d.descriptors());
        var changed = fds.toBuilder().setFile(0, fds.getFile(0).toBuilder().setName("alternate.proto")).build();
        var other = definition(StringValue.getDescriptor().getFullName(), changed);
        try (var store = store(); var host = new RegistrySchemaResolver(store, CACHE, 2, 64)) {
            store.putDescriptorSet(d.metadata().getArtifactSha256(), d.descriptors());
            store.putDescriptorSet(other.metadata().getArtifactSha256(), other.descriptors());
            var calls = new AtomicInteger();
            try (var attempt = host.open(o -> selected(calls.incrementAndGet() == 1 ? d : other), ACTIVE)) {
                assertThat(attempt.select(occurrence(d)).descriptors()).isEqualTo(d.descriptors());
                assertThat(attempt.select(occurrence(d)).descriptors()).isEqualTo(other.descriptors());
            }
            try (var isolatedStore = GitSchemaRegistryStore.builder().repositoryDir(temp.resolve("isolated")).build();
                 var isolated = new RegistrySchemaResolver(isolatedStore, CACHE, 1, 64);
                 var attempt = isolated.open(o -> selected(d), ACTIVE)) {
                assertThatThrownBy(() -> attempt.select(occurrence(d))).isInstanceOf(RegistrySchemaResolver.MissingDescriptor.class);
            }
        }
    }

    @Test void cancellationAcrossColdSelectionCheckpointsReleasesAttemptCapacity() throws Exception {
        var d = definition(StringValue.getDescriptor());
        try (var store = store()) {
            store.putDescriptorSet(d.metadata().getArtifactSha256(), d.descriptors());
            // A cold selection has at least thirteen caller checks. Waiting can add
            // checks, so a count measured in another run is not a stable bound.
            for (int point = 1; point <= 13; point++) {
                int failAt = point;
                var calls = new AtomicInteger();
                var failure = new CancellationException();
                try (var host = new RegistrySchemaResolver(store, CACHE, 1, 64)) {
                    assertThatThrownBy(() -> {
                        try (var attempt = host.open(o -> selected(d), () -> {
                            if (calls.incrementAndGet() == failAt) throw failure;
                        })) { attempt.select(occurrence(d)); }
                    }).isSameAs(failure);
                    try (var retry = host.open(o -> selected(d), ACTIVE)) {
                        assertThat(retry.select(occurrence(d)).descriptors()).isEqualTo(d.descriptors());
                        assertThatThrownBy(() -> host.open(o -> selected(d), ACTIVE)).isInstanceOf(IllegalStateException.class);
                    }
                    host.close();
                    assertThat(host.awaitLoads(java.time.Duration.ofSeconds(5))).isTrue();
                    assertThat(host.cachedBytes()).isZero();
                }
            }
        }
    }

    @Test void realAdmissionCopiesRegistryDefinitionsAndRechecksChangedCandidates() throws Exception {
        var payload = definition(StringValue.getDescriptor());
        var container = definition(Document.getDescriptor());
        var ownership = OwnershipContext.newBuilder().setAccountId("account").setDatasourceId("source")
                .setSecurity(DocumentSecurity.getDefaultInstance()).build();
        var document = Document.newBuilder().setDocId("doc").setOwnership(ownership)
                .setStructuredData(Any.pack(StringValue.of("hello"), "type.test")).build();
        var preparation = preparation(document, container);
        try (var store = store(); var host = new RegistrySchemaResolver(store, CACHE, 1, 64)) {
            store.putDescriptorSet(payload.metadata().getArtifactSha256(), payload.descriptors());
            var reservations = new Reservations();
            DocumentSchemaAdmission.PreparedProof proof;
            try (var attempt = host.open(o -> selected(payload), ACTIVE)) {
                proof = DocumentSchemaAdmission.prepareAndCheck(preparation, attempt, ADMISSION, reservations, ACTIVE);
            }
            try (proof) {
                assertThat(proof.proof().document()).isEqualTo(document);
                var malformed = document.toBuilder().setStructuredData(document.getStructuredData().toBuilder()
                        .setValue(ByteString.copyFrom(new byte[] {(byte) 0x80}))).build();
                var invalid = preparation(malformed, container);
                var selections = new AtomicInteger();
                try (var attempt = host.open(o -> { selections.incrementAndGet(); return selected(payload); }, ACTIVE)) {
                    assertThatThrownBy(() -> DocumentSchemaAdmission.prepareAndCheck(invalid, attempt, ADMISSION,
                            reservations, ACTIVE)).isInstanceOf(InvalidProtocolBufferException.class);
                }
                assertThat(selections.get()).isPositive();
                host.close(); assertThat(host.cachedBytes()).isZero();
                assertThat(proof.proof().document()).isEqualTo(document);
            }
            assertThat(reservations.bytes).isZero();
        }
    }

    private static DocumentSchemaAdmission.Preparation preparation(Document document,
            DocumentSchemaAdmission.Definition container) throws Exception {
        var member = DocumentPublicationMember.newBuilder().setMemberId("member")
                .setDriveId(UUID.randomUUID().toString()).setOwnership(document.getOwnership())
                .setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE)
                .setDestination(DocumentRevisionCondition.newBuilder().setIfAbsent(true)
                        .setAddress(NodeAddress.newBuilder().setAccountId("account").setDocId("doc")
                                .setGraphId("graph").setGraphAddressId("node")));
        var fragments = new HashMap<Integer, ByteString>();
        var parts = DocumentPartCodec.split(document, PartLayouts.document());
        for (int i = 0; i < parts.size(); i++) {
            var p = parts.get(i); var bytes = ByteString.copyFrom(p.bytes()); fragments.put(i, bytes);
            member.addParts(DocumentPublicationPart.newBuilder()
                    .setSlot(DocumentPublicationSlot.newBuilder().setPart(p.part()).setSubKey(p.subKey()))
                    .setUpload(PublicationUpload.newBuilder().setSizeBytes(bytes.size()).setSha256(sha(bytes))
                            .setContentType("application/protobuf")));
        }
        return new DocumentSchemaAdmission.Preparation(ByteString.copyFrom(new byte[32]), "a".repeat(64),
                true, member.build(), fragments, container);
    }

    private static final class Reservations implements DocumentAdmissionReservations {
        long bytes;
        public Lease reserve(long size) {
            if (size > 64_000_000 - bytes) throw new IllegalStateException("test budget exhausted");
            bytes += size;
            return new Lease() { boolean closed; public void close() { if (!closed) { closed = true; bytes -= size; } } };
        }
    }
    private static DocumentSchemaAdmission.Definition definition(Descriptors.Descriptor type) throws Exception {
        return definition(type.getFullName(), DescriptorFingerprints.closure(type));
    }
    private static DocumentSchemaAdmission.Definition definition(String name, DescriptorProtos.FileDescriptorSet fds) throws Exception {
        var bytes = fds.toByteString();
        var metadata = RepositorySchemaAsset.newBuilder().setTypeUrl("type.test/" + name).setArtifactSha256(sha(bytes))
                .setSchema(PublicationSchemaCondition.newBuilder().setTypeName(name)
                        .setDescriptorFingerprint(DescriptorFingerprints.fingerprint(fds)))
                .setCompilation(SchemaCompilationProvenance.newBuilder()
                        .setOrigin(SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_IMPORTED_DESCRIPTOR)
                        .setEvidence(SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_UNKNOWN)
                        .setUnknownCompilerReason("fixture compiler unknown")
                        .setAdmissionRuntime(SchemaToolIdentity.newBuilder().setName("test-runtime").setVersion("1"))).build();
        return new DocumentSchemaAdmission.Definition(metadata, bytes, Optional.empty());
    }
    private static String sha(ByteString bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()));
    }
}
