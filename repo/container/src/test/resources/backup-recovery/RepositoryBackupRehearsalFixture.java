package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.repo.admission.DocumentAdmissionPolicy;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.admission.DocumentSchemaArtifactCache;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.codec.PartLayouts;
import ai.protomolt.proto.repo.schema.registry.RegistrySchemaResolver;
import ai.protomolt.proto.repo.service.HistoricalReadAccess;
import ai.protomolt.proto.repo.service.ManagedPublicationOptions;
import ai.protomolt.proto.repo.service.ManagedSchemaAccess;
import ai.protomolt.proto.repo.service.ManagedStoragePolicy;
import ai.protomolt.proto.repo.service.RepoServiceConfig;
import ai.protomolt.proto.repo.spi.HistoricalMaterializationRepository;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.schema.registry.git.GitSchemaRegistryStore;
import ai.protomolt.proto.validate.CelRule;
import ai.protomolt.proto.validate.MessageRules;
import ai.protomolt.proto.validate.ValidateProto;
import com.google.protobuf.Any;
import com.google.protobuf.AnyProto;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import com.google.protobuf.TimestampProto;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Shared production composition for the seed and recovered hosts. Everything goes through
 * public repo-service APIs except the trusted policy-catalog rows, which the existing runtime
 * probes also administer because no policy API exists. Adapted from the draft harness on
 * GitHub PR #413 (commit 4862f35f9): two accounts and a nested Any type were added.
 */
final class RepositoryBackupRehearsalFixture {
    static final String TYPE_PREFIX = "type.test";
    static final String RECORD_TYPE = "protomolt.rehearsal.Record";
    static final String ATTACHMENT_TYPE = "protomolt.rehearsal.Attachment";
    static final String VALIDATION_PROFILE = "protomolt-retained-schema-admission/v1";
    static final String API_TOKEN_ENV = "PROTOMOLT_REHEARSAL_API_TOKEN";
    static final HistoricalMaterializationRepository.Limits LIMITS =
            new HistoricalMaterializationRepository.Limits(4_000_000, 4_000_000, 16_000_000, 64, 8_000_000, 64);

    private RepositoryBackupRehearsalFixture() {}

    static String env(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) throw new RepositoryBackupRehearsalFailure("Missing required environment variable " + name);
        return value;
    }

    /** The hosts run on the production JAR set plus this probe; no test framework may be present. */
    static void requireNoTestFramework() {
        for (String name : new String[] {"org.junit.jupiter.api.Test", "org.testcontainers.containers.GenericContainer",
                "org.assertj.core.api.Assertions"}) {
            try { Class.forName(name); throw new RepositoryBackupRehearsalFailure("Test framework class on the production host classpath: " + name); }
            catch (ClassNotFoundException expected) { /* production JARs only */ }
        }
    }

    static RepoServiceConfig config(String endpoint, String generation, String realm) {
        return config(endpoint, generation, realm, env("PROTOMOLT_REHEARSAL_S3_ACCESS"), env("PROTOMOLT_REHEARSAL_S3_SECRET"));
    }

    static RepoServiceConfig config(String endpoint, String generation, String realm, String access, String secret) {
        return new RepoServiceConfig(0, new ai.protomolt.proto.repo.container.ledger.LedgerConfig(env("PROTOMOLT_REHEARSAL_JDBC"),
                env("PROTOMOLT_REHEARSAL_DB_USER"), env("PROTOMOLT_REHEARSAL_DB_PASSWORD")),
                endpoint, env("PROTOMOLT_REHEARSAL_S3_REGION"), access, secret, "rehearsal", 0, null, null, null, null, 0, 0L)
                .withManagedStorage(new ManagedStoragePolicy(generation, realm, true));
    }

    static RepositoryCaller operator() { return new RepositoryCaller("operator", true); }

    /** Account member carrying the ACL identity the account's documents grant. */
    static RepositoryCaller member(String account) {
        return new RepositoryCaller("member-" + account, false, Set.of(account),
                Set.of(Principal.newBuilder().setIdentityType("user").setIdentity(memberIdentity(account)).build()));
    }

    static String memberIdentity(String account) { return "rehearsal-member-" + account; }

    /** Same account, no granted identity: only a public rule can admit this caller. */
    static RepositoryCaller stranger(String account) {
        return new RepositoryCaller("stranger-" + account, false, Set.of(account), Set.of());
    }

    /** Different account: never admitted regardless of ACL. */
    static RepositoryCaller outsider() {
        return new RepositoryCaller("outsider", false, Set.of("rehearsal-other-account"), Set.of());
    }

    static HistoricalReadAccess historicalAccess() {
        var operator = operator();
        return new HistoricalReadAccess(caller -> operator, 32L * 1024 * 1024, 2).withMaterialization(LIMITS);
    }

    static ManagedPublicationOptions publicationOptions(Path bundle) {
        var operator = operator();
        return new ManagedPublicationOptions(bundle, Duration.ofMinutes(5), Duration.ofSeconds(5),
                (account, principal, operation) -> operator)
                .withTransport(new ManagedPublicationOptions.Transport(auth -> operator, 32L * 1024 * 1024, 2));
    }

    /** The two dynamic types: a record carrying a nested Any whose payload is an attachment from a separate file. */
    record Types(Descriptors.Descriptor record, Descriptors.Descriptor attachment) {}

    static Types types() throws Descriptors.DescriptorValidationException {
        var attachmentFile = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("protomolt/rehearsal/attachment.proto").setPackage("protomolt.rehearsal").setSyntax("proto3")
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("Attachment")
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder().setName("name").setNumber(1)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder().setName("payload").setNumber(2)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES)
                                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)));
        var attachment = Descriptors.FileDescriptor.buildFrom(attachmentFile.build(), new Descriptors.FileDescriptor[0])
                .findMessageTypeByName("Attachment");
        var recordFile = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("protomolt/rehearsal/record.proto").setPackage("protomolt.rehearsal").setSyntax("proto3")
                .addDependency(TimestampProto.getDescriptor().getName())
                .addDependency(AnyProto.getDescriptor().getName())
                .addDependency(ValidateProto.getDescriptor().getName());
        var message = DescriptorProtos.DescriptorProto.newBuilder().setName("Record")
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder().setName("label").setNumber(1)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder().setName("captured_at").setNumber(2)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(".google.protobuf.Timestamp")
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder().setName("note").setNumber(3)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder().setName("attachment").setNumber(4)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(".google.protobuf.Any")
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
        message.setOptions(DescriptorProtos.MessageOptions.newBuilder().setExtension(ValidateProto.message,
                MessageRules.newBuilder().addCel(CelRule.newBuilder().setId("rehearsal-label-required")
                        .setExpression("this.label != ''")).build()));
        recordFile.addMessageType(message);
        var record = Descriptors.FileDescriptor.buildFrom(recordFile.build(), new Descriptors.FileDescriptor[] {
                TimestampProto.getDescriptor(), AnyProto.getDescriptor(), ValidateProto.getDescriptor()})
                .findMessageTypeByName("Record");
        return new Types(record, attachment);
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
                        .setAdmissionRuntime(SchemaToolIdentity.newBuilder().setName("repository-backup-rehearsal").setVersion("1")))
                .build();
        return new DocumentSchemaAdmission.Definition(metadata, bytes, Optional.empty());
    }

    /** Definitions keyed by type URL: the registry serves whichever occurrence admission asks for. */
    static Map<String, DocumentSchemaAdmission.Definition> definitions(Types types) {
        var map = new LinkedHashMap<String, DocumentSchemaAdmission.Definition>();
        for (var type : new Descriptors.Descriptor[] {types.record(), types.attachment()}) {
            var definition = definition(type);
            map.put(definition.metadata().getTypeUrl(), definition);
        }
        return map;
    }

    /** Live registry access for seeding; counts resolutions so the evidence can show when it was used. */
    record LiveSchemas(ManagedSchemaAccess access, AtomicInteger opens, GitSchemaRegistryStore store) {}

    static LiveSchemas liveSchemas(Path registryDirectory, Map<String, DocumentSchemaAdmission.Definition> definitions) throws Exception {
        var git = GitSchemaRegistryStore.builder().repositoryDir(registryDirectory).build();
        for (var definition : definitions.values()) git.putDescriptorSet(definition.metadata().getArtifactSha256(), definition.descriptors());
        var resolver = new RegistrySchemaResolver(git, new DocumentSchemaArtifactCache.Limits(8_000_000, 16, 4_000_000), 4, 16);
        var opens = new AtomicInteger();
        var access = new ManagedSchemaAccess() {
            public DocumentSchemaAdmission.Resolution open(RepositoryCaller caller, DocumentPublicationMember member, RepositoryReadControl control) {
                opens.incrementAndGet();
                return resolver.open(occurrence -> {
                    var definition = definitions.get(occurrence.typeUrl());
                    if (definition == null) throw new IllegalStateException("Registry has no definition for " + occurrence.typeUrl());
                    return new RegistrySchemaResolver.Selected(definition.metadata(), definition.source());
                }, control::check);
            }
            public void close() { resolver.close(); }
            public boolean awaitIdle(Duration timeout) throws InterruptedException { return resolver.awaitLoads(timeout); }
        };
        return new LiveSchemas(access, opens, git);
    }

    /**
     * No registry exists on the recovered host: while unarmed, any live resolution attempt is counted
     * and refused. Negative modes arm a registry holding a definition that must never be substituted
     * for the retained one; historical reads must not pass through here at all.
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

    static DocumentSecurity openSecurity(String account) {
        return DocumentSecurity.newBuilder()
                .addPermissions(AccessRule.newBuilder().setIdentityType("user").setIdentity(memberIdentity(account)).setAccess(Access.ACCESS_READ))
                .addPermissions(AccessRule.newBuilder().setIdentityType("public").setIdentity("public").setAccess(Access.ACCESS_READ))
                .build();
    }

    /** Revokes the public grant: only the named member may read, including earlier revisions afterwards. */
    static DocumentSecurity memberOnlySecurity(String account) {
        return DocumentSecurity.newBuilder()
                .addPermissions(AccessRule.newBuilder().setIdentityType("user").setIdentity(memberIdentity(account)).setAccess(Access.ACCESS_READ))
                .build();
    }

    /** A record whose attachment field is a nested Any of the separately defined attachment type. */
    static Any payload(Types types, String label, long epochSeconds, String note, String attachmentName, byte[] attachmentBytes) {
        var attachment = DynamicMessage.newBuilder(types.attachment())
                .setField(types.attachment().findFieldByName("name"), attachmentName)
                .setField(types.attachment().findFieldByName("payload"), ByteString.copyFrom(attachmentBytes)).build();
        var value = DynamicMessage.newBuilder(types.record())
                .setField(types.record().findFieldByName("label"), label)
                .setField(types.record().findFieldByName("captured_at"), Timestamp.newBuilder().setSeconds(epochSeconds).build())
                .setField(types.record().findFieldByName("note"), note)
                .setField(types.record().findFieldByName("attachment"), Any.pack(attachment, TYPE_PREFIX)).build();
        return Any.pack(value, TYPE_PREFIX);
    }

    record Fixture(PublishDocumentRequest request, Document document) {}

    static NodeAddress address(String account) {
        return NodeAddress.newBuilder().setAccountId(account).setDocId("rehearsal-document").setGraphId("rehearsal-graph").setGraphAddressId("node").build();
    }

    /** One typed publication request for the account's document; expectedMutationRevision < 0 means if-absent. */
    static Fixture fixture(String account, String driveId, Any structured, DocumentSecurity security, long expectedMutationRevision) {
        var ownership = OwnershipContext.newBuilder().setAccountId(account).setDatasourceId("rehearsal-source").setSecurity(security).build();
        var document = Document.newBuilder().setDocId("rehearsal-document").setOwnership(ownership).setStructuredData(structured).build();
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

    /** Direct provider client with the recorded endpoint identity, for version-exact reads and injections. */
    static software.amazon.awssdk.services.s3.S3Client s3(String endpoint) {
        return s3(endpoint, env("PROTOMOLT_REHEARSAL_S3_ACCESS"), env("PROTOMOLT_REHEARSAL_S3_SECRET"));
    }

    static software.amazon.awssdk.services.s3.S3Client s3(String endpoint, String access, String secret) {
        return software.amazon.awssdk.services.s3.S3Client.builder().endpointOverride(java.net.URI.create(endpoint))
                .region(software.amazon.awssdk.regions.Region.of(env("PROTOMOLT_REHEARSAL_S3_REGION"))).forcePathStyle(true)
                .httpClientBuilder(software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient.builder())
                .credentialsProvider(software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                        software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(access, secret))).build();
    }
}
