package ai.protomolt.proto.repo.recovery;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.repo.admission.DocumentAdmissionPolicy;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.admission.DocumentSchemaArtifactCache;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.codec.PartLayouts;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.container.ledger.Tx;
import ai.protomolt.proto.repo.schema.registry.RegistrySchemaResolver;
import ai.protomolt.proto.repo.service.HistoricalReadAccess;
import ai.protomolt.proto.repo.service.ManagedPublicationOptions;
import ai.protomolt.proto.repo.service.ManagedSchemaAccess;
import ai.protomolt.proto.repo.service.ManagedStoragePolicy;
import ai.protomolt.proto.repo.service.RepoServiceConfig;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.schema.registry.git.GitSchemaRegistryStore;
import ai.protomolt.proto.validate.CelRule;
import ai.protomolt.proto.validate.MessageRules;
import ai.protomolt.proto.validate.ValidateProto;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import com.google.protobuf.TimestampProto;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Shared production composition for the seed and recovered hosts. Everything here goes
 * through public repo-service APIs; the only SQL writes are the trusted policy-catalog
 * rows that the existing runtime probes also administer because no policy API exists.
 */
final class RehearsalFixture {
    static final String TYPE_PREFIX = "type.test";
    static final String TYPE_NAME = "protomolt.recovery.RecoveryRecord";
    static final String VALIDATION_PROFILE = "protomolt-retained-schema-admission/v1";
    static final String MEMBER_IDENTITY = "recovery-member";
    static final String API_TOKEN_ENV = "PROTOMOLT_RECOVERY_API_TOKEN";

    private RehearsalFixture() {}

    static String env(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) throw new RehearsalFailure("Missing required environment variable " + name);
        return value;
    }

    static void requireNoTestFramework() {
        for (String name : new String[] {"org.junit.jupiter.api.Test", "org.testcontainers.containers.GenericContainer", "org.assertj.core.api.Assertions"}) {
            try { Class.forName(name); throw new RehearsalFailure("Test framework class on the production host classpath: " + name); }
            catch (ClassNotFoundException expected) { /* The host runs on production JARs only. */ }
        }
    }

    static RepoServiceConfig config(String endpoint, String generation, String realm) {
        return new RepoServiceConfig(0, new LedgerConfig(env("PROTOMOLT_RECOVERY_JDBC"),
                env("PROTOMOLT_RECOVERY_DB_USER"), env("PROTOMOLT_RECOVERY_DB_PASSWORD")),
                endpoint, env("PROTOMOLT_RECOVERY_S3_REGION"), env("PROTOMOLT_RECOVERY_S3_ACCESS"),
                env("PROTOMOLT_RECOVERY_S3_SECRET"), "recovery", 0, null, null, null, null, 0, 0L)
                .withManagedStorage(new ManagedStoragePolicy(generation, realm, true));
    }

    static RepositoryCaller operator() { return new RepositoryCaller("operator", true); }

    /** Account member with the ACL identity the documents grant. */
    static RepositoryCaller member(String account) {
        return new RepositoryCaller("recovery-member", false, Set.of(account),
                Set.of(Principal.newBuilder().setIdentityType("user").setIdentity(MEMBER_IDENTITY).build()));
    }

    /** Same account, no granted identity: only a public rule can admit this caller. */
    static RepositoryCaller stranger(String account) {
        return new RepositoryCaller("recovery-stranger", false, Set.of(account), Set.of());
    }

    /** Different account: never admitted regardless of ACL. */
    static RepositoryCaller outsider() {
        return new RepositoryCaller("recovery-outsider", false, Set.of("other-account"), Set.of());
    }

    static HistoricalReadAccess historicalAccess() {
        var operator = operator();
        return new HistoricalReadAccess(caller -> operator, 32L * 1024 * 1024, 2);
    }

    static ManagedPublicationOptions publicationOptions(Path bundle) {
        var operator = operator();
        return new ManagedPublicationOptions(bundle, Duration.ofMinutes(5), Duration.ofSeconds(5),
                (account, principal, operation) -> operator)
                .withTransport(new ManagedPublicationOptions.Transport(auth -> operator, 32L * 1024 * 1024, 2));
    }

    /** Dynamic type with retained imports (timestamp, validate) and a CEL rule that must actually run. */
    static Descriptors.Descriptor recordType() throws Descriptors.DescriptorValidationException {
        var file = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("protomolt/recovery/rehearsal.proto").setPackage("protomolt.recovery").setSyntax("proto3")
                .addDependency(TimestampProto.getDescriptor().getName())
                .addDependency(ValidateProto.getDescriptor().getName());
        var message = DescriptorProtos.DescriptorProto.newBuilder().setName("RecoveryRecord")
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder().setName("label").setNumber(1)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder().setName("captured_at").setNumber(2)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(".google.protobuf.Timestamp")
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder().setName("note").setNumber(3)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
        message.setOptions(DescriptorProtos.MessageOptions.newBuilder().setExtension(ValidateProto.message,
                MessageRules.newBuilder().addCel(CelRule.newBuilder().setId("recovery-label-required")
                        .setExpression("this.label != ''")).build()));
        file.addMessageType(message);
        return Descriptors.FileDescriptor.buildFrom(file.build(),
                new Descriptors.FileDescriptor[] {TimestampProto.getDescriptor(), ValidateProto.getDescriptor()})
                .findMessageTypeByName("RecoveryRecord");
    }

    static DocumentSchemaAdmission.Definition definition(Descriptors.Descriptor type) {
        var closure = DescriptorFingerprints.closure(type);
        var bytes = closure.toByteString();
        var metadata = RepositorySchemaAsset.newBuilder().setTypeUrl(TYPE_PREFIX + "/" + type.getFullName())
                .setArtifactSha256(DocumentPartCodec.sha256Hex(bytes.toByteArray()))
                .setSchema(PublicationSchemaCondition.newBuilder().setTypeName(type.getFullName())
                        .setDescriptorFingerprint(DescriptorFingerprints.fingerprint(closure)))
                .setCompilation(SchemaCompilationProvenance.newBuilder()
                        .setOrigin(SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_IMPORTED_DESCRIPTOR)
                        .setEvidence(SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_UNKNOWN)
                        .setUnknownCompilerReason("dynamic rehearsal descriptor; compiler identity unknown")
                        .setAdmissionRuntime(SchemaToolIdentity.newBuilder().setName("repository-recovery-rehearsal").setVersion("1")))
                .build();
        return new DocumentSchemaAdmission.Definition(metadata, bytes, Optional.empty());
    }

    /** Live registry access for seeding; counts resolutions so the evidence can show when it was used. */
    record LiveSchemas(ManagedSchemaAccess access, AtomicInteger opens, GitSchemaRegistryStore store) {}

    static LiveSchemas liveSchemas(Path registryDirectory, DocumentSchemaAdmission.Definition definition) throws Exception {
        var git = GitSchemaRegistryStore.builder().repositoryDir(registryDirectory).build();
        git.putDescriptorSet(definition.metadata().getArtifactSha256(), definition.descriptors());
        var resolver = new RegistrySchemaResolver(git, new DocumentSchemaArtifactCache.Limits(8_000_000, 16, 4_000_000), 4, 16);
        var opens = new AtomicInteger();
        var access = new ManagedSchemaAccess() {
            public DocumentSchemaAdmission.Resolution open(RepositoryCaller caller, DocumentPublicationMember member, RepositoryReadControl control) {
                opens.incrementAndGet();
                return resolver.open(occurrence -> new RegistrySchemaResolver.Selected(definition.metadata(), definition.source()), control::check);
            }
            public void close() { resolver.close(); }
            public boolean awaitIdle(Duration timeout) throws InterruptedException { return resolver.awaitLoads(timeout); }
        };
        return new LiveSchemas(access, opens, git);
    }

    /**
     * No registry exists on the recovered host: while unarmed, any live resolution attempt is counted
     * and refused. A later fresh typed write arms a registry rebuilt from the retained catalog bytes;
     * historical reads never pass through here at all.
     */
    static final class SwitchableSchemas implements ManagedSchemaAccess {
        final AtomicInteger offlineAttempts = new AtomicInteger();
        private volatile LiveSchemas armed;

        public DocumentSchemaAdmission.Resolution open(RepositoryCaller caller, DocumentPublicationMember member, RepositoryReadControl control) {
            var live = armed;
            if (live == null) {
                offlineAttempts.incrementAndGet();
                throw new IllegalStateException("Recovered host attempted live schema resolution; no registry is configured");
            }
            return live.access().open(caller, member, control);
        }
        void arm(LiveSchemas live) { armed = live; }
        LiveSchemas disarm() { var live = armed; armed = null; return live; }
        public void close() { var live = armed; if (live != null) live.access().close(); }
        public boolean awaitIdle(Duration timeout) throws InterruptedException { var live = armed; return live == null || live.access().awaitIdle(timeout); }
    }

    /** Trusted policy-catalog administration (no policy API exists); the same rows the runtime probes insert. */
    static DocumentAdmissionPolicy installTypedPolicy(Tx tx, String account) {
        var policy = DocumentAdmissionPolicy.of(DocumentSchemaPolicy.newBuilder()
                .setEncodingVersion(1).setAccountId(account)
                .setMode(DocumentSchemaPolicyMode.DOCUMENT_SCHEMA_POLICY_MODE_TYPED_REQUIRED).setAnyResolvedSchema(true)
                .setValidationProfile(VALIDATION_PROFILE)
                .setLimits(DocumentSchemaPolicyLimits.newBuilder().setMaxFragments(32).setMaxFragmentBytes(4_000_000)
                        .setMaxRoots(100).setMaxEvidenceBytes(4_000_000).setMaxBindings(20).setMaxRetainedBytes(16_000_000)
                        .setMaxDecodedBytes(1_000_000)).build(), () -> {});
        tx.inTransaction(em -> {
            em.createNativeQuery("SELECT lock_document_schema_policy_account(:account,true)").setParameter("account", account).getSingleResult();
            em.createNativeQuery("""
                    INSERT INTO document_schema_policies(account_id,policy_sha256,policy_codec,policy_version,policy_bytes)
                    VALUES(:account,:sha,:codec,:version,:bytes)
                    """).setParameter("account", account).setParameter("sha", HexFormat.of().parseHex(policy.sha256()))
                    .setParameter("codec", DocumentAdmissionPolicy.CODEC).setParameter("version", DocumentAdmissionPolicy.VERSION)
                    .setParameter("bytes", policy.bytes().toByteArray()).executeUpdate();
            em.createNativeQuery("INSERT INTO document_schema_policy_current(account_id,policy_sha256,policy_revision) VALUES(:account,:sha,1)")
                    .setParameter("account", account).setParameter("sha", HexFormat.of().parseHex(policy.sha256())).executeUpdate();
        });
        return policy;
    }

    static DocumentSecurity openSecurity() {
        return DocumentSecurity.newBuilder()
                .addPermissions(AccessRule.newBuilder().setIdentityType("user").setIdentity(MEMBER_IDENTITY).setAccess(Access.ACCESS_READ))
                .addPermissions(AccessRule.newBuilder().setIdentityType("public").setIdentity("public").setAccess(Access.ACCESS_READ))
                .build();
    }

    /** Revision 2 revokes the public grant: only the named member may read, including revision 1 afterwards. */
    static DocumentSecurity memberOnlySecurity() {
        return DocumentSecurity.newBuilder()
                .addPermissions(AccessRule.newBuilder().setIdentityType("user").setIdentity(MEMBER_IDENTITY).setAccess(Access.ACCESS_READ))
                .build();
    }

    static Any payload(Descriptors.Descriptor type, String label, long epochSeconds, String note) {
        var value = DynamicMessage.newBuilder(type)
                .setField(type.findFieldByName("label"), label)
                .setField(type.findFieldByName("captured_at"), Timestamp.newBuilder().setSeconds(epochSeconds).build())
                .setField(type.findFieldByName("note"), note).build();
        return Any.pack(value, TYPE_PREFIX);
    }

    record Fixture(PublishDocumentRequest request, Document document) {}

    static NodeAddress address(String account) {
        return NodeAddress.newBuilder().setAccountId(account).setDocId("recovery-document").setGraphId("recovery-graph").setGraphAddressId("node").build();
    }

    /** One typed publication request for the document at its address; expectedMutationRevision < 0 means if-absent. */
    static Fixture fixture(String account, String driveId, Any structured, DocumentSecurity security, long expectedMutationRevision) {
        var ownership = OwnershipContext.newBuilder().setAccountId(account).setDatasourceId("recovery-source").setSecurity(security).build();
        var document = Document.newBuilder().setDocId("recovery-document").setOwnership(ownership).setStructuredData(structured).build();
        var destination = DocumentRevisionCondition.newBuilder().setAddress(address(account));
        if (expectedMutationRevision < 0) destination.setIfAbsent(true); else destination.setExpectedMutationRevision(expectedMutationRevision);
        var member = DocumentPublicationMember.newBuilder().setMemberId("document").setDriveId(driveId).setOwnership(ownership)
                .setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE).setDestination(destination);
        var request = PublishDocumentRequest.newBuilder();
        for (var part : DocumentPartCodec.split(document, PartLayouts.document())) {
            int ordinal = member.getPartsCount();
            member.addParts(DocumentPublicationPart.newBuilder().setSlot(DocumentPublicationSlot.newBuilder().setPart(part.part()).setSubKey(part.subKey()))
                    .setUpload(PublicationUpload.newBuilder().setSizeBytes(part.bytes().length)
                            .setSha256(DocumentPartCodec.sha256Hex(part.bytes())).setContentType("application/protobuf")));
            request.addPayloads(DocumentPublicationPayload.newBuilder().setMemberId("document").setRevisionOrdinal(ordinal)
                    .setContent(ByteString.copyFrom(part.bytes())));
        }
        request.setIntent(DocumentPublicationIntent.newBuilder().setEncodingVersion(1).setAccountId(account)
                .setOperationId(UUID.randomUUID().toString()).addMembers(member));
        request.addModes(DocumentPublicationMemberMode.newBuilder().setMemberId("document").setMode(DocumentPublicationMode.DOCUMENT_PUBLICATION_MODE_TYPED));
        return new Fixture(request.build(), document);
    }

    static Map<String, String> s3Options(String endpoint) {
        return Map.of("endpoint", endpoint, "region", env("PROTOMOLT_RECOVERY_S3_REGION"), "path-style", "true",
                "conditional-writes", "false", "access-key", env("PROTOMOLT_RECOVERY_S3_ACCESS"), "secret-key", env("PROTOMOLT_RECOVERY_S3_SECRET"));
    }
}
