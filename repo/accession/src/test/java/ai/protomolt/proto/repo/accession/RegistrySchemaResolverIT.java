package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.INT64_URL;
import static ai.protomolt.proto.repo.accession.AccessionFixture.KEY_URL;
import static ai.protomolt.proto.repo.accession.AccessionFixture.POSTGRES;
import static ai.protomolt.proto.repo.accession.AccessionFixture.S3;
import static ai.protomolt.proto.repo.accession.AccessionFixture.address;
import static ai.protomolt.proto.repo.accession.AccessionFixture.claimChecked;
import static ai.protomolt.proto.repo.accession.AccessionFixture.create;
import static ai.protomolt.proto.repo.accession.AccessionFixture.document;
import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.repo.admission.BuiltinDocumentSchema;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.schema.registry.RegistrySchemaResolver;
import ai.protomolt.proto.repo.semantic.v1.EmbeddingSetKey;
import ai.protomolt.proto.repo.v1.PublicationSchemaCondition;
import ai.protomolt.proto.repo.v1.RepositorySchemaAsset;
import ai.protomolt.proto.repo.v1.SchemaCompilationOrigin;
import ai.protomolt.proto.repo.v1.SchemaCompilationProvenance;
import ai.protomolt.proto.repo.v1.SchemaCompilerEvidence;
import ai.protomolt.proto.repo.v1.SchemaToolIdentity;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.GetDocumentRequest;
import ai.protomolt.proto.repo.v2.RejectionReason;
import ai.protomolt.proto.repo.v2.SaveRejection;
import ai.protomolt.proto.repo.v2.ValidationStatus;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Int64Value;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.StringValue;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.protobuf.StatusProto;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests proving {@link DocumentSaves} and {@link AccessionGrpcService} admit
 * and refuse typed documents through a real {@link RegistrySchemaResolver} wired via
 * {@link V2RepositoryFixture}.
 */
class RegistrySchemaResolverIT {

    private static final String UNREGISTERED_STRING_URL =
            "type.googleapis.com/" + StringValue.getDescriptor().getFullName();
    private static final DocumentSchemaAdmission.Definition KEY_DEFINITION =
            BuiltinDocumentSchema.definition(EmbeddingSetKey.getDescriptor());
    private static final DocumentSchemaAdmission.Definition INT64_DEFINITION = int64Definition();

    private DocumentRegistry registry;
    private RegistrySchemaResolver resolver;
    private final AtomicInteger selectorInvocations = new AtomicInteger();
    private V2RepositoryFixture fixture;
    private ManagedChannel channel;
    private DocumentServiceGrpc.DocumentServiceBlockingStub stub;
    private String account;

    @BeforeEach
    void setUp() throws Exception {
        AccessionFixture.start();
        registry = new DocumentRegistry();
        registry.registerDocument(DocumentRegistry.documentSource());
        registry.store.putDescriptorSet(
                KEY_DEFINITION.metadata().getArtifactSha256(), KEY_DEFINITION.descriptors());

        resolver =
                new RegistrySchemaResolver(
                        registry.store, V2RepositoryFixture.DEFAULT_CACHE_LIMITS, 4, 64);
        Map<String, DocumentSchemaAdmission.Definition> catalogedTypes =
                Map.of(KEY_URL, KEY_DEFINITION, INT64_URL, INT64_DEFINITION);

        String serverName = "registry-resolver-it-" + UUID.randomUUID();
        fixture =
                V2RepositoryFixture.builder(POSTGRES, S3, serverName)
                        .isolatedDatabase()
                        .schemas(registry.schemas())
                        .registryResolver(
                                resolver,
                                occurrence -> {
                                    selectorInvocations.incrementAndGet();
                                    var def = catalogedTypes.get(occurrence.typeUrl());
                                    return def == null
                                            ? null
                                            : new RegistrySchemaResolver.Selected(
                                                    def.metadata(), def.source());
                                })
                        .build();
        channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        stub = DocumentServiceGrpc.newBlockingStub(channel);
        account = "acct-" + UUID.randomUUID();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (channel != null) {
            channel.shutdownNow();
        }
        if (fixture != null) {
            fixture.close();
        }
        if (resolver != null) {
            resolver.close();
            assertThat(resolver.awaitLoads(Duration.ofSeconds(5))).isTrue();
        }
    }

    @Test
    void typedDocumentResolvedThroughRegistrySchemaResolverCommitsAndRetainsSchemaAsset() {
        var validKey =
                EmbeddingSetKey.newBuilder()
                        .setSegmentationId("seg-primary")
                        .setEmbeddingSetId("emb-v1")
                        .build();
        var doc1 =
                document(account, "doc-typed-1", "First typed doc").toBuilder()
                        .setStructuredData(Any.pack(validKey))
                        .build();
        var addr1 = address(account, "doc-typed-1");

        var saved1 = stub.saveDocument(create("op-typed-1", addr1, doc1).build());
        assertThat(saved1.getAccession().getRevision()).isEqualTo(1L);
        assertThat(saved1.getAccession().getValidation().getStatus())
                .isEqualTo(ValidationStatus.VALIDATION_STATUS_TYPED_VALID);
        assertThat(resolver.stats().registryReads()).isEqualTo(1L);
        assertThat(resolver.stats().cacheMisses()).isEqualTo(1L);
        assertThat(resolver.stats().cacheHits()).isZero();
        assertThat(selectorInvocations.get()).isEqualTo(1);

        var storedManifest = fixture.catalog().manifest(addr1, 1L).orElseThrow();
        assertThat(storedManifest.getValidation().getResolvedOccurrencesList())
                .singleElement()
                .satisfies(
                        occurrence -> {
                            var ref = occurrence.getSchema();
                            assertThat(ref.getTypeUrl()).isEqualTo(KEY_URL);
                            assertThat(ref.getDescriptorSha256())
                                    .isEqualTo(KEY_DEFINITION.metadata().getArtifactSha256());
                            assertThat(ref.getMetadataSha256()).isNotBlank();
                            assertThat(fixture.catalog().artifact(ref.getDescriptorSha256()))
                                    .contains(KEY_DEFINITION.descriptors());
                        });

        // A second save of the same type URL hits the descriptor cache without another store read,
        // while still invoking the occurrence selector.
        var doc2 =
                document(account, "doc-typed-2", "Second typed doc").toBuilder()
                        .setStructuredData(Any.pack(validKey))
                        .build();
        var addr2 = address(account, "doc-typed-2");
        var saved2 = stub.saveDocument(create("op-typed-2", addr2, doc2).build());
        assertThat(saved2.getAccession().getRevision()).isEqualTo(1L);
        assertThat(resolver.stats().registryReads()).isEqualTo(1L);
        assertThat(resolver.stats().cacheHits()).isEqualTo(1L);
        assertThat(selectorInvocations.get()).isEqualTo(2);

        // Reads use the retained catalog schema artifacts and never consult the live registry.
        registry.stopped = true;
        int lookupsBeforeRead = registry.lookups.get();
        var fetched =
                stub.getDocument(GetDocumentRequest.newBuilder().setAddress(addr1).build());
        assertThat(fetched.getDocument()).isEqualTo(claimChecked(doc1, storedManifest));
        assertThat(registry.lookups.get()).isEqualTo(lookupsBeforeRead);
    }

    @Test
    void unregisteredTypeAndMissingDescriptorArtifactAreRefusedAsSchemaMissing() {
        // 1. Type URL not authorized/selected by the registry selector -> SCHEMA_MISSING.
        var addrUnregistered = address(account, "doc-unregistered");
        var docUnregistered =
                document(account, "doc-unregistered", "Unregistered payload").toBuilder()
                        .setStructuredData(Any.pack(StringValue.of("unregistered")))
                        .build();

        var unregisteredRejection =
                expectRejection(
                        () ->
                                stub.saveDocument(
                                        create("op-unregistered", addrUnregistered, docUnregistered)
                                                .build()),
                        Status.Code.FAILED_PRECONDITION);
        assertThat(unregisteredRejection.getReason())
                .isEqualTo(RejectionReason.REJECTION_REASON_SCHEMA_MISSING);
        assertThat(unregisteredRejection.getMessage())
                .isEqualTo("No registered schema for " + UNREGISTERED_STRING_URL);
        assertThat(
                        fixture.catalog()
                                .outcome(account, "op-unregistered")
                                .orElseThrow()
                                .outcome()
                                .getRejection()
                                .getReason())
                .isEqualTo(RejectionReason.REJECTION_REASON_SCHEMA_MISSING);
        assertThat(fixture.catalog().current(addrUnregistered)).isEmpty();

        // 2. Type URL selected by metadata, but descriptor set bytes absent from store ->
        // MissingDescriptor mapped to SCHEMA_MISSING without negative caching.
        var addrMissingBytes = address(account, "doc-missing-artifact");
        var docInt64 = document(account, "doc-missing-artifact", "Int64 payload");

        var missingArtifactRejection =
                expectRejection(
                        () ->
                                stub.saveDocument(
                                        create("op-missing-artifact", addrMissingBytes, docInt64)
                                                .build()),
                        Status.Code.FAILED_PRECONDITION);
        assertThat(missingArtifactRejection.getReason())
                .isEqualTo(RejectionReason.REJECTION_REASON_SCHEMA_MISSING);
        assertThat(missingArtifactRejection.getMessage())
                .isEqualTo("No registered schema for " + INT64_URL);
        assertThat(fixture.catalog().current(addrMissingBytes)).isEmpty();

        // Once the descriptor artifact is populated in the registry store, a new save succeeds.
        registry.store.putDescriptorSet(
                INT64_DEFINITION.metadata().getArtifactSha256(), INT64_DEFINITION.descriptors());
        var recovered =
                stub.saveDocument(
                        create("op-present-artifact", addrMissingBytes, docInt64).build());
        assertThat(recovered.getAccession().getRevision()).isEqualTo(1L);
    }

    @Test
    void payloadRuleViolationAndInvalidWireBytesAgainstRegistryDescriptorAreRefused() {
        // 1. Payload decodes under the registry-resolved EmbeddingSetKey descriptor but violates its rules.
        var addrInvalidRule = address(account, "doc-invalid-rule");
        var incompleteKey =
                EmbeddingSetKey.newBuilder()
                        .setSegmentationId("seg-only")
                        .build();
        var docInvalidRule =
                document(account, "doc-invalid-rule", "Invalid rule payload").toBuilder()
                        .setStructuredData(Any.pack(incompleteKey))
                        .build();

        var ruleRejection =
                expectRejection(
                        () ->
                                stub.saveDocument(
                                        create("op-invalid-rule", addrInvalidRule, docInvalidRule)
                                                .build()),
                        Status.Code.INVALID_ARGUMENT);
        assertThat(ruleRejection.getReason())
                .isEqualTo(RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
        assertThat(ruleRejection.getMessage())
                .isEqualTo("The composed document failed validation");
        assertThat(ruleRejection.getViolationsList())
                .isNotEmpty()
                .anySatisfy(
                        violation -> {
                            assertThat(violation.getFieldPath()).isEqualTo("structured_data.embedding_set_id");
                            assertThat(violation.getRuleId()).isEqualTo("required");
                        });
        assertThat(fixture.catalog().current(addrInvalidRule)).isEmpty();

        // 2. Payload carries undecodable wire bytes for the registry-resolved descriptor.
        var addrCorruptBytes = address(account, "doc-corrupt-bytes");
        var docCorruptBytes =
                document(account, "doc-corrupt-bytes", "Corrupt wire payload").toBuilder()
                        .setStructuredData(
                                Any.newBuilder()
                                        .setTypeUrl(KEY_URL)
                                        .setValue(ByteString.copyFrom(new byte[] {(byte) 0x80}))
                                        .build())
                        .build();

        var bytesRejection =
                expectRejection(
                        () ->
                                stub.saveDocument(
                                        create(
                                                        "op-corrupt-bytes",
                                                        addrCorruptBytes,
                                                        docCorruptBytes)
                                                .build()),
                        Status.Code.INVALID_ARGUMENT);
        assertThat(bytesRejection.getReason())
                .isEqualTo(RejectionReason.REJECTION_REASON_INVALID_BYTES);
        assertThat(bytesRejection.getMessage())
                .isEqualTo("Any value does not decode as " + KEY_URL);
        assertThat(fixture.catalog().current(addrCorruptBytes)).isEmpty();
    }

    private static SaveRejection expectRejection(Runnable call, Status.Code expectedCode) {
        try {
            call.run();
        } catch (StatusRuntimeException failure) {
            assertThat(failure.getStatus().getCode()).isEqualTo(expectedCode);
            var status = StatusProto.fromThrowable(failure);
            assertThat(status).isNotNull();
            assertThat(status.getDetailsCount()).isPositive();
            return unpack(status.getDetails(0), SaveRejection.class);
        }
        throw new AssertionError("Expected call to fail with " + expectedCode);
    }

    private static <T extends Message> T unpack(Any detail, Class<T> type) {
        try {
            return detail.unpack(type);
        } catch (InvalidProtocolBufferException invalid) {
            throw new AssertionError("Failed to unpack " + type.getSimpleName(), invalid);
        }
    }

    private static DocumentSchemaAdmission.Definition int64Definition() {
        var closure = DescriptorFingerprints.closure(Int64Value.getDescriptor());
        var bytes = closure.toByteString();
        var metadata =
                RepositorySchemaAsset.newBuilder()
                        .setTypeUrl(INT64_URL)
                        .setArtifactSha256(DocumentSaves.sha256(bytes))
                        .setSchema(
                                PublicationSchemaCondition.newBuilder()
                                        .setTypeName(Int64Value.getDescriptor().getFullName())
                                        .setDescriptorFingerprint(
                                                DescriptorFingerprints.fingerprint(closure)))
                        .setCompilation(
                                SchemaCompilationProvenance.newBuilder()
                                        .setOrigin(
                                                SchemaCompilationOrigin
                                                        .SCHEMA_COMPILATION_ORIGIN_IMPORTED_DESCRIPTOR)
                                        .setEvidence(
                                                SchemaCompilerEvidence
                                                        .SCHEMA_COMPILER_EVIDENCE_UNKNOWN)
                                        .setUnknownCompilerReason(
                                                "well-known type compiled into protobuf-java")
                                        .setAdmissionRuntime(
                                                SchemaToolIdentity.newBuilder()
                                                        .setName("protobuf-java")
                                                        .setVersion("test")))
                        .build();
        return new DocumentSchemaAdmission.Definition(metadata, bytes, Optional.empty());
    }
}
