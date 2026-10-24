package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.config.ConceptSchemeMounts;
import ai.protomolt.proto.config.ConfigSource;
import ai.protomolt.proto.config.DistributedConfig;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.AccessRule;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.DriveServiceGrpc;
import ai.protomolt.proto.repo.v1.DriveType;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.ConceptFilter;
import ai.protomolt.proto.repo.v2.DocumentFilter;
import ai.protomolt.proto.repo.v2.DocumentLink;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.DocumentTarget;
import ai.protomolt.proto.repo.v2.DynamicTarget;
import ai.protomolt.proto.repo.v2.ExpectedRevision;
import ai.protomolt.proto.repo.v2.FieldViolation;
import ai.protomolt.proto.repo.v2.GetDocumentRequest;
import ai.protomolt.proto.repo.v2.IntakeOrigin;
import ai.protomolt.proto.repo.v2.LinkField;
import ai.protomolt.proto.repo.v2.LinksProto;
import ai.protomolt.proto.repo.v2.ListDocumentsRequest;
import ai.protomolt.proto.repo.v2.ListDocumentsResponse;
import ai.protomolt.proto.repo.v2.MountedConceptScheme;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.PartWrite;
import ai.protomolt.proto.repo.v2.ResolveLinkRequest;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v2.SaveOwner;
import ai.protomolt.proto.repo.v2.SaveRejection;
import ai.protomolt.proto.repo.v2.WriteAction;
import ai.protomolt.proto.types.Concept;
import ai.protomolt.proto.types.ConceptExpansion;
import ai.protomolt.proto.types.ConceptRef;
import ai.protomolt.proto.types.ConceptScheme;
import ai.protomolt.proto.types.ConceptsProto;
import ai.protomolt.proto.types.LangLabel;
import ai.protomolt.proto.validate.FieldRules;
import ai.protomolt.proto.validate.ValidateProto;
import com.google.protobuf.Any;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.protobuf.StatusProto;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.groups.Tuple.tuple;

/**
 * Concept schemes through the served repository (requirements §17): the production
 * {@link RepoServices} composition over real PostgreSQL and LocalStack S3, so the
 * mounts reach the saves, {@code ListDocuments} and {@code ResolveLink} exactly as
 * {@link RepoV2Wiring} wires them. A host given mounts with
 * {@link RepoServices#withConceptSchemes} admits a bound {@code ConceptRef}, refuses a
 * concept outside the mounted scheme, and expands a {@code DocumentFilter.concept} at
 * query time; a host given none keeps the fail-closed default.
 */
@Testcontainers
class RepoV2WiringIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    private static final String REF = ".ai.protomolt.proto.types.v1.ConceptRef";
    private static final String LINK = ".ai.protomolt.proto.repo.v2.DocumentLink";
    private static final FileDescriptor FILE = file();
    private static final Descriptor PAPER = FILE.findMessageTypeByName("Paper");
    private static final String PAPER_URL = "type.googleapis.com/test.wiring.v1.Paper";
    private static final String DATASOURCE = "ds-wiring";

    @Test
    void mountedSchemesReachSavesListingsAndResolveLink() throws Exception {
        var source = new Source();
        source.put("subject", "s1", subjects());
        String account = "wiring-mounted";
        try (var config = DistributedConfig.over(source)) {
            var mounts = ConceptSchemeMounts.follow(config, List.of("subject"));
            assertThat(config.refresh().refused()).isEmpty();
            try (var services = RepoServices.build(config()).withConceptSchemes(mounts)) {
                services.withAdmittedSchemas(List.of(FILE));
                var channel = start(services);
                try {
                    var documents = DocumentServiceGrpc.newBlockingStub(channel);
                    intakeDrive(channel, account);
                    documents.saveDocument(save(account, "d1", p -> p.setField(PAPER.findFieldByName("subject"), ref("physics"))));
                    documents.saveDocument(save(account, "d2", p -> p.setField(PAPER.findFieldByName("subject"), ref("quantum"))));
                    documents.saveDocument(save(account, "d3", p -> p.setField(PAPER.findFieldByName("subject"), ref("chemistry"))));
                    documents.saveDocument(save(account, "untagged", p -> {}));

                    // Membership is decided against the mounted scheme at save.
                    assertThat(rejectedRules(() -> documents.saveDocument(save(account, "bad",
                            p -> p.setField(PAPER.findFieldByName("subject"), ref("biology"))))))
                            .containsExactly("concept.member");

                    var narrower = ConceptExpansion.newBuilder().setNarrower(true).build();
                    var physics = documents.listDocuments(list(account, concept("physics", narrower)));
                    assertThat(docIds(physics)).containsExactly("d1", "d2");
                    assertThat(physics.getConceptEvidence().getMountedSchemesList())
                            .extracting(MountedConceptScheme::getSchemeId, MountedConceptScheme::getVersion)
                            .containsExactly(tuple("subject", "s1"));
                    assertThat(physics.getConceptEvidence().getExpandedConcepts()).isEqualTo(2);
                    assertThat(docIds(documents.listDocuments(list(account, concept("science", narrower)))))
                            .containsExactly("d1", "d2", "d3");
                    // A concept the mounted scheme lacks is the caller's error, not an empty page.
                    assertCode(() -> documents.listDocuments(list(account,
                            concept("biology", ConceptExpansion.getDefaultInstance()))), Status.Code.INVALID_ARGUMENT);

                    // ResolveLink runs a dynamic link's selector through the same listing.
                    var selector = DynamicTarget.newBuilder().setFilter(concept("physics", narrower)).build();
                    var src = save(account, "src", p -> p.setField(PAPER.findFieldByName("related"),
                            dynamic(DocumentLink.newBuilder().setDynamicTarget(selector).build())));
                    documents.saveDocument(src);
                    var resolved = documents.resolveLink(ResolveLinkRequest.newBuilder()
                            .setAddress(address(account, "src")).setFieldPath("structured_data.related")
                            .setPageSize(10).build());
                    assertThat(resolved.getDocumentsList()).extracting(summary -> summary.getAddress().getDocId())
                            .containsExactly("d1", "d2");
                    assertThat(resolved.getConceptEvidence().getMountedSchemesList())
                            .extracting(MountedConceptScheme::getSchemeId).containsExactly("subject");
                } finally {
                    channel.shutdownNow();
                }
            }
        }
    }

    @Test
    void aHostGivenNoMountsRefusesBoundTagsAndConceptFilters() {
        String account = "wiring-unmounted";
        try (var services = RepoServices.build(config())) {
            services.withAdmittedSchemas(List.of(FILE));
            var channel = start(services);
            try {
                var documents = DocumentServiceGrpc.newBlockingStub(channel);
                intakeDrive(channel, account);
                assertThat(rejectedRules(() -> documents.saveDocument(save(account, "cold",
                        p -> p.setField(PAPER.findFieldByName("subject"), ref("physics"))))))
                        .containsExactly("concept.unmounted");
                // An untagged document still saves.
                documents.saveDocument(save(account, "untagged", p -> {}));
                assertCode(() -> documents.listDocuments(list(account,
                        concept("physics", ConceptExpansion.getDefaultInstance()))), Status.Code.FAILED_PRECONDITION);
                assertThat(docIds(documents.listDocuments(list(account, DocumentFilter.getDefaultInstance()))))
                        .containsExactly("untagged");
            } finally {
                channel.shutdownNow();
            }
        }
    }

    @Test
    void saveBudgetReportsSaveExhaustionAndIsIsolatedFromTheReadBudget() {
        String account = "wiring-save-budget";
        long budgetBytes = 200_000L;
        try (var services = RepoServices.build(config(budgetBytes))) {
            services.withAdmittedSchemas(List.of(FILE));
            var channel = start(services);
            try {
                var documents = DocumentServiceGrpc.newBlockingStub(channel);
                intakeDrive(channel, account);

                assertThatThrownBy(() -> documents.saveDocument(save(account, "over-budget",
                        p -> p.setField(PAPER.findFieldByName("title"), "x".repeat(250_000)))))
                        .isInstanceOfSatisfying(StatusRuntimeException.class, failure -> {
                            assertThat(failure.getStatus().getCode())
                                    .isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
                            assertThat(failure.getStatus().getDescription())
                                    .contains("Save buffer budget exhausted");
                        });

                try (var heldRead = services.readBudget().reserve(budgetBytes)) {
                    var saved = documents.saveDocument(save(account, "within-budget", p -> {}));
                    assertThat(saved.getAccession().getRevision()).isEqualTo(1L);

                    assertThatThrownBy(() -> documents.getDocument(GetDocumentRequest.newBuilder()
                            .setAddress(address(account, "within-budget"))
                            .build()))
                            .isInstanceOfSatisfying(StatusRuntimeException.class, failure -> {
                                assertThat(failure.getStatus().getCode())
                                        .isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
                                assertThat(failure.getStatus().getDescription())
                                        .contains("Read buffer budget exhausted");
                            });
                }

                var fetched = documents.getDocument(GetDocumentRequest.newBuilder()
                        .setAddress(address(account, "within-budget"))
                        .build());
                assertThat(fetched.getDocument().getDocId()).isEqualTo("within-budget");
            } finally {
                channel.shutdownNow();
            }
        }
    }

    // ------------------------------------------------------------------
    // Fixtures
    // ------------------------------------------------------------------

    private static RepoServiceConfig config() {
        return config(0L);
    }

    private static RepoServiceConfig config(long readBudgetBytes) {
        return new RepoServiceConfig(
                0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                LOCALSTACK.getEndpoint().toString(),
                LOCALSTACK.getRegion(),
                LOCALSTACK.getAccessKey(),
                LOCALSTACK.getSecretKey(),
                "it-wiring",
                0,
                null, null, 0, 0L,
                false, 0L, 0L, false, true, 0L,
                null, null, null, null, null, null,
                false, ManagedStoragePolicy.disabled(),
                readBudgetBytes);
    }

    private static ManagedChannel start(RepoServices services) {
        String name = "repo-v2-wiring-" + UUID.randomUUID();
        services.startInProcess(name);
        return InProcessChannelBuilder.forName(name).directExecutor().build();
    }

    private static void intakeDrive(ManagedChannel channel, String account) {
        DriveServiceGrpc.newBlockingStub(channel).createDrive(CreateDriveRequest.newBuilder().setName("intake")
                .setAccountId(account).setDriveType(DriveType.DRIVE_TYPE_INTAKE).build());
    }

    private static NodeAddress address(String account, String doc) {
        return NodeAddress.newBuilder().setAccountId(account).setDocId(doc)
                .setGraphId("intake:" + account).setGraphAddressId(DATASOURCE).build();
    }

    private static SaveDocumentRequest save(String account, String doc, Consumer<DynamicMessage.Builder> fill) {
        var paper = DynamicMessage.newBuilder(PAPER).setField(PAPER.findFieldByName("title"), doc);
        fill.accept(paper);
        var document = Document.newBuilder().setDocId(doc)
                .setOwnership(OwnershipContext.newBuilder().setAccountId(account).setDatasourceId(DATASOURCE)
                        .setSecurity(DocumentSecurity.newBuilder().addPermissions(AccessRule.newBuilder()
                                .setIdentityType("public").setIdentity("public").setAccess(Access.ACCESS_READ))))
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle(doc))
                .setStructuredData(Any.newBuilder().setTypeUrl(PAPER_URL).setValue(paper.build().toByteString()))
                .build();
        return SaveDocumentRequest.newBuilder().setOperationId("op-" + account + "-" + doc)
                .setTarget(DocumentTarget.newBuilder().setAddress(address(account, doc)))
                .setExpected(ExpectedRevision.newBuilder().setNone(true))
                .setDocument(document)
                .addWrites(PartWrite.newBuilder()
                        .setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE))
                        .setAction(WriteAction.WRITE_ACTION_PUT))
                .setOwner(SaveOwner.newBuilder().setIntake(IntakeOrigin.newBuilder().setDatasourceId(DATASOURCE)))
                .build();
    }

    private static ListDocumentsRequest list(String account, DocumentFilter filter) {
        return ListDocumentsRequest.newBuilder().setAccountId(account).setPageSize(100).setFilter(filter).build();
    }

    private static DocumentFilter concept(String concept, ConceptExpansion expansion) {
        return DocumentFilter.newBuilder().setConcept(ConceptFilter.newBuilder()
                .setConcept(ConceptRef.newBuilder().setSchemeId("subject").setConceptId(concept))
                .setExpansion(expansion)).build();
    }

    private static List<String> docIds(ListDocumentsResponse page) {
        assertThat(page.getNextPageToken()).isEmpty();
        var ids = new ArrayList<String>();
        page.getDocumentsList().forEach(summary -> ids.add(summary.getAddress().getDocId()));
        return ids;
    }

    private static Message ref(String concept) {
        return ConceptRef.newBuilder().setSchemeId("subject").setConceptId(concept).build();
    }

    private static Message dynamic(DocumentLink link) {
        try {
            return DynamicMessage.parseFrom(DocumentLink.getDescriptor(), link.toByteString());
        } catch (InvalidProtocolBufferException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    /** The rule ids of the VALIDATION_FAILED save rejection {@code call} answers with. */
    private static List<String> rejectedRules(Runnable call) {
        var failure = (StatusRuntimeException) org.assertj.core.api.Assertions.catchThrowable(call::run);
        assertThat(failure).as("the save was expected to be refused").isNotNull();
        assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        var details = StatusProto.fromThrowable(failure).getDetailsList();
        var rules = new ArrayList<String>();
        for (var detail : details) {
            if (!detail.is(SaveRejection.class)) continue;
            try {
                detail.unpack(SaveRejection.class).getViolationsList().stream()
                        .map(FieldViolation::getRuleId).forEach(rules::add);
            } catch (InvalidProtocolBufferException malformed) {
                throw new AssertionError("an unreadable SaveRejection detail", malformed);
            }
        }
        return rules;
    }

    private static void assertCode(org.assertj.core.api.ThrowableAssert.ThrowingCallable call, Status.Code code) {
        assertThatThrownBy(call).isInstanceOfSatisfying(StatusRuntimeException.class,
                failure -> assertThat(failure.getStatus().getCode()).isEqualTo(code));
    }

    /** subject: physics and chemistry under science, quantum under physics. */
    private static ConceptScheme subjects() {
        return ConceptScheme.newBuilder()
                .addConcepts(concept("science"))
                .addConcepts(concept("physics").addBroader("science"))
                .addConcepts(concept("chemistry").addBroader("science"))
                .addConcepts(concept("quantum").addBroader("physics"))
                .build();
    }

    private static Concept.Builder concept(String id) {
        return Concept.newBuilder().setId(id).addPrefLabel(LangLabel.newBuilder().setLanguage("en").setText(id));
    }

    /** A config source over a map, one subject per scheme. */
    private static final class Source implements ConfigSource {
        private final Map<String, Fetched> documents = new HashMap<>();

        @Override public synchronized Optional<Fetched> fetch(String subject) {
            return Optional.ofNullable(documents.get(subject));
        }

        synchronized void put(String schemeId, String version, ConceptScheme scheme) {
            documents.put(ConceptSchemeMounts.SUBJECT_PREFIX + schemeId, new Fetched(version, scheme.toByteArray()));
        }
    }

    /** test.wiring.v1.Paper: subject is bound to the subject scheme, related is a dynamic link. */
    private static FileDescriptor file() {
        var subject = FieldOptions.newBuilder().setExtension(ValidateProto.field,
                FieldRules.newBuilder().addConceptSchemes("subject").build()).build();
        var related = FieldOptions.newBuilder().setExtension(LinksProto.link,
                LinkField.newBuilder().setRelation("related").build()).build();
        var file = FileDescriptorProto.newBuilder().setName("test/wiring/v1/paper.proto").setPackage("test.wiring.v1")
                .setSyntax("proto3").addDependency(ConceptsProto.getDescriptor().getName())
                .addDependency(ValidateProto.getDescriptor().getName())
                .addDependency(LinksProto.getDescriptor().getName())
                .addMessageType(DescriptorProto.newBuilder().setName("Paper")
                        .addField(FieldDescriptorProto.newBuilder().setName("title").setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .addField(message("subject", 2, REF).setOptions(subject))
                        .addField(message("related", 3, LINK).setOptions(related)))
                .build();
        try {
            return FileDescriptor.buildFrom(file, new FileDescriptor[]{ConceptsProto.getDescriptor(),
                    ValidateProto.getDescriptor(), LinksProto.getDescriptor()});
        } catch (DescriptorValidationException invalid) {
            throw new IllegalStateException(invalid);
        }
    }

    private static FieldDescriptorProto.Builder message(String name, int number, String type) {
        return FieldDescriptorProto.newBuilder().setName(name).setNumber(number).setTypeName(type)
                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE).setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL);
    }
}
