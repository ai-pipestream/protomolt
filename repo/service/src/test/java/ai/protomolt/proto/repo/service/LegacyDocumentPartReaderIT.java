package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.engine.DocumentPartReader;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.*;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class LegacyDocumentPartReaderIT {
    @Container static final LocalStackContainer S3 = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");
    static OpenedBlobStore opened;
    static final String NAMESPACE = "legacy-source";
    @BeforeAll static void open() {
        opened = BlobStores.discover().open("s3", Map.of("endpoint", S3.getEndpoint().toString(), "region", S3.getRegion(),
                "access-key", S3.getAccessKey(), "secret-key", S3.getSecretKey(), "path-style", "true", "conditional-writes", "false"));
        opened.ensureNamespace(NAMESPACE);
        try (var admin = software.amazon.awssdk.services.s3.S3Client.builder().endpointOverride(S3.getEndpoint())
                .region(software.amazon.awssdk.regions.Region.of(S3.getRegion())).forcePathStyle(true)
                .credentialsProvider(software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                        software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(S3.getAccessKey(), S3.getSecretKey()))).build()) {
            admin.putBucketVersioning(b -> b.bucket(NAMESPACE).versioningConfiguration(v ->
                    v.status(software.amazon.awssdk.services.s3.model.BucketVersioningStatus.ENABLED)));
        }
    }
    @AfterAll static void close() throws Exception { if (opened != null) opened.close(); }
    record Source(PartManifestEntry entry, byte[] bytes, BlobStore.PutResult put) {}
    static Source source(DocumentPart part, String subKey) throws Exception {
        // A real protobuf with duplicate singular fields: decoding and reserializing changes its bytes.
        var output = new java.io.ByteArrayOutputStream();
        output.write(Document.newBuilder().setDocId("earlier").build().toByteArray());
        output.write(Document.newBuilder().setDocId("later").build().toByteArray());
        byte[] bytes = output.toByteArray();
        String key = "documents/legacy/" + UUID.randomUUID();
        var put = opened.store().put(new BlobStore.PutSpec(NAMESPACE, key, "application/protobuf", Map.of(), DocumentPartCodec.sha256Hex(bytes)), bytes);
        var entry = PartManifestEntry.newBuilder().setPart(part).setSubKey(subKey).setState(PartState.PART_STATE_PRESENT)
                .setObjectKey(key).setSizeBytes(bytes.length).setSha256(DocumentPartCodec.sha256Hex(bytes)).build();
        return new Source(entry, bytes, put);
    }
    static DocumentPartReader reader() { return reader(256L * 1024 * 1024); }
    static DocumentPartReader reader(long bound) {
        return new DocumentPartReader((generation, profile) -> { throw new AssertionError("Legacy source has no original profile"); }, 2, bound);
    }
    static BlobStore counted(AtomicInteger gets) {
        return (BlobStore) java.lang.reflect.Proxy.newProxyInstance(BlobStore.class.getClassLoader(), new Class<?>[] {BlobStore.class}, (proxy, method, args) -> {
            if (method.getName().equals("get")) gets.incrementAndGet();
            try { return method.invoke(opened.store(), args); }
            catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
        });
    }
    static void code(Throwable failure, RepositoryException.Code code) {
        assertThat(failure).isInstanceOfSatisfying(RepositoryException.class, e -> assertThat(e.code()).isEqualTo(code));
    }

    @Test void exactOrderedBytesUseRecordedCoreVersion() throws Exception {
        var core = source(DocumentPart.DOCUMENT_PART_CORE, "");
        var chunk = source(DocumentPart.DOCUMENT_PART_CHUNKS, "set-a");
        opened.store().put(new BlobStore.PutSpec(NAMESPACE, core.entry.getObjectKey(), "application/protobuf", Map.of(), null), new byte[] {1, 2});
        var result = reader().readLegacyFragments(opened.store(), NAMESPACE, List.of(chunk.entry, core.entry),
                core.put.versionId(), core.put.eTag(), RepositoryReadControl.NONE);
        assertThat(result).extracting(p -> p.part()).containsExactly(DocumentPart.DOCUMENT_PART_CHUNKS, DocumentPart.DOCUMENT_PART_CORE);
        assertThat(result.get(0).bytes()).containsExactly(chunk.bytes);
        assertThat(result.get(1).bytes()).containsExactly(core.bytes);
        assertThat(Document.parseFrom(core.bytes).toByteArray()).isNotEqualTo(core.bytes);
    }

    @Test void noRecordedNonCoreVersionCannotSilentlyChooseHistoricalBytes() throws Exception {
        var chunk = source(DocumentPart.DOCUMENT_PART_CHUNKS, "set-a");
        opened.store().put(new BlobStore.PutSpec(NAMESPACE, chunk.entry.getObjectKey(), "application/protobuf", Map.of(), null), new byte[] {1, 2});
        code(catchThrowable(() -> reader().readLegacyFragments(opened.store(), NAMESPACE, List.of(chunk.entry), null, null,
                RepositoryReadControl.NONE)), RepositoryException.Code.DATA_LOSS);
    }

    @Test void missingSourceRetainsRetryAsFullSaveMeaning() throws Exception {
        var core = source(DocumentPart.DOCUMENT_PART_CORE, "");
        assertThat(opened.reclaimer().reclaim(NAMESPACE, core.entry.getObjectKey())).isTrue();
        code(catchThrowable(() -> reader().readLegacyFragments(opened.store(), NAMESPACE, List.of(core.entry), core.put.versionId(),
                core.put.eTag(), RepositoryReadControl.NONE)), RepositoryException.Code.FAILED_PRECONDITION);
    }

    @ParameterizedTest @ValueSource(strings = {"digest", "size", "state", "slot", "key", "duplicate-slot", "duplicate-key"})
    void invalidSelectionCannotReachProvider(String defect) throws Exception {
        var core = source(DocumentPart.DOCUMENT_PART_CORE, "");
        var entry = core.entry.toBuilder();
        switch (defect) {
            case "digest" -> entry.clearSha256();
            case "size" -> entry.setSizeBytes(-1);
            case "state" -> entry.setState(PartState.PART_STATE_DELETED);
            case "slot" -> entry.setSubKey("unexpected");
            case "key" -> entry.clearObjectKey();
        }
        List<PartManifestEntry> selected = List.of(entry.build());
        if (defect.equals("duplicate-slot")) selected = List.of(entry.build(), entry.setObjectKey("different-key").build());
        if (defect.equals("duplicate-key")) selected = List.of(entry.build(), entry.setPart(DocumentPart.DOCUMENT_PART_CHUNKS).setSubKey("set-a").build());
        var stable = selected; var gets = new AtomicInteger();
        code(catchThrowable(() -> reader().readLegacyFragments(counted(gets), NAMESPACE, stable, null, null,
                RepositoryReadControl.NONE)), RepositoryException.Code.FAILED_PRECONDITION);
        assertThat(gets).hasValue(0);
    }

    @Test void aggregateByteLimitIsCheckedBeforeFetchingAnyPart() throws Exception {
        var core = source(DocumentPart.DOCUMENT_PART_CORE, ""); var chunk = source(DocumentPart.DOCUMENT_PART_CHUNKS, "set-a");
        var gets = new AtomicInteger();
        code(catchThrowable(() -> reader(core.bytes.length).readLegacyFragments(counted(gets), NAMESPACE,
                List.of(core.entry, chunk.entry), null, null, RepositoryReadControl.NONE)), RepositoryException.Code.RESOURCE_EXHAUSTED);
        assertThat(gets).hasValue(0);
    }

    @Test void suppliedCoreIdentityMustMatch() throws Exception {
        var core = source(DocumentPart.DOCUMENT_PART_CORE, "");
        code(catchThrowable(() -> reader().readLegacyFragments(opened.store(), NAMESPACE, List.of(core.entry), core.put.versionId(),
                "wrong-etag", RepositoryReadControl.NONE)), RepositoryException.Code.DATA_LOSS);
    }
}
