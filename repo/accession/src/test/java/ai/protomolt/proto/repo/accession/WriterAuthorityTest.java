package ai.protomolt.proto.repo.accession;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.repo.admission.BuiltinDocumentSchema;
import ai.protomolt.proto.repo.admission.DocumentSchemaRetention;
import ai.protomolt.proto.repo.catalog.Catalog;
import ai.protomolt.proto.repo.catalog.CatalogRecords.Credential;
import ai.protomolt.proto.repo.catalog.CatalogRecords.CredentialCheck;
import ai.protomolt.proto.repo.catalog.CatalogRecords.Grant;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryCredentialBinding;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.WriteProvenance;
import ai.protomolt.proto.repo.v2.IntakeOrigin;
import ai.protomolt.proto.repo.v2.ProcessAuthorityEvidence;
import ai.protomolt.proto.repo.v2.RejectionReason;
import ai.protomolt.proto.repo.v2.SaveOwner;
import ai.protomolt.proto.repo.v2.SaveRejection;
import ai.protomolt.proto.repo.v2.StepWriterScope;
import ai.protomolt.proto.repo.v2.WriterCredential;
import ai.protomolt.proto.repo.v2.WriterGrant;
import ai.protomolt.proto.repo.v2.WriterGrantEvidence;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.grpc.protobuf.StatusProto;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;

/**
 * The write-authority decision table (requirements §5, decisions 25-27 and 31) over a
 * fake catalog: which caller, credential and grant combinations authorize a save, an
 * opaque producer or a delete, what evidence an authorization carries, and the code,
 * message and rejection detail of each refusal, including how the refusal crosses the
 * wire through {@link AccessionGrpcService#toStatus}.
 */
class WriterAuthorityTest {
    private static final String ACCOUNT = "account-1";
    private static final String ISSUER = "issuer-1";
    private static final UUID CREDENTIAL = UUID.fromString("00000000-0000-4000-8000-000000000001");
    private static final long GENERATION = 3;
    private static final RepositoryCaller PROCESS = new RepositoryCaller("ingest-process", true);
    private static final RepositoryCaller WRITER = new RepositoryCaller("writer", false, Set.of(ACCOUNT), Set.of(),
            Optional.of(new RepositoryCredentialBinding(ISSUER, CREDENTIAL, GENERATION)));
    private static final RepositoryCaller UNBOUND = new RepositoryCaller("principal-only", false, Set.of(ACCOUNT),
            Set.of());
    private static final SaveOwner INTAKE_OWNER = intakeOwner("datasource-1", null);
    private static final WriteProvenance STEP = WriteProvenance.newBuilder().setModuleId("module-1")
            .setNodeId("node-1").setGraphId("graph-1").setGraphVersion(1).build();
    private static final SaveOwner STEP_OWNER = SaveOwner.newBuilder().setStep(STEP).build();
    private static final String TYPE_URL = "type.googleapis.com/example.v1.Opaque";
    private static final DocumentSchemaRetention.Retained GRANT_SCHEMA = DocumentSchemaRetention.retain(
            BuiltinDocumentSchema.definition(WriterGrant.getDescriptor()));

    private final FakeCatalog catalog = new FakeCatalog();
    private final WriterAuthority authority = new WriterAuthority(catalog.proxy(), GRANT_SCHEMA);

    // ------------------------------------------------------------------ authorize

    @Test
    void processAuthoritySavesAsAnIntakeOwnerWithoutConsultingTheCatalog() {
        var authorized = authority.authorize(PROCESS, INTAKE_OWNER, ACCOUNT, Optional.empty(), true);

        assertThat(authorized.process()).contains(ProcessAuthorityEvidence.newBuilder()
                .setPrincipal("ingest-process").build());
        assertThat(authorized.grant()).isEmpty();
        assertThat(authorized.grantIds()).isEmpty();
        assertThat(authorized.credential()).isEmpty();
        assertThat(catalog.calls).isEmpty();
    }

    @Test
    void processAuthorityIsRefusedAsAStepOwner() {
        assertDenied(() -> authority.authorize(PROCESS, STEP_OWNER, ACCOUNT, Optional.empty(), false),
                "Process authority may save only as an intake owner");
        assertThat(catalog.calls).isEmpty();
    }

    @Test
    void callerWithoutCredentialBindingIsRefused() {
        assertDenied(() -> authority.authorize(UNBOUND, INTAKE_OWNER, ACCOUNT, Optional.empty(), false),
                "Saving requires a credential-bound caller or process authority");
        assertThat(catalog.calls).isEmpty();
    }

    @Test
    void unregisteredCredentialIsRefused() {
        assertDenied(() -> authority.authorize(WRITER, INTAKE_OWNER, ACCOUNT, Optional.empty(), false),
                "The caller's credential is not registered");
        assertThat(catalog.calls).containsExactly("credential " + ISSUER + " " + CREDENTIAL);
    }

    @Test
    void revokedCredentialIsRefused() {
        catalog.credential = Optional.of(new Credential(GENERATION, "writer", true));
        catalog.hold(intakeGrant("grant-a", "datasource-1", null, true), false);
        assertDenied(() -> authority.authorize(WRITER, INTAKE_OWNER, ACCOUNT, Optional.empty(), false),
                "The caller's credential generation is not current");
    }

    @Test
    void rotatedCredentialGenerationIsRefused() {
        catalog.credential = Optional.of(new Credential(GENERATION + 1, "writer", false));
        catalog.hold(intakeGrant("grant-a", "datasource-1", null, true), false);
        assertDenied(() -> authority.authorize(WRITER, INTAKE_OWNER, ACCOUNT, Optional.empty(), false),
                "The caller's credential generation is not current");
    }

    @Test
    void coveringGrantAuthorizesWithItsEvidenceAndTheCredentialRecheck() {
        catalog.currentCredential();
        var grant = intakeGrant("grant-a", "datasource-1", null, false).build();
        catalog.hold(grant.toBuilder(), false);

        var authorized = authority.authorize(WRITER, INTAKE_OWNER, ACCOUNT, Optional.empty(), false);

        assertThat(authorized.grant()).contains(WriterGrantEvidence.newBuilder().setWriterGrantId("grant-a")
                .setWriterGrantSha256(sha256(grant.toByteArray())).setGrantSnapshot(grant.toByteString())
                .setSnapshotSchema(GRANT_SCHEMA.reference()).setCredentialGeneration(GENERATION).build());
        assertThat(authorized.process()).isEmpty();
        assertThat(authorized.grantIds()).containsExactly("grant-a");
        assertThat(authorized.credential()).contains(new CredentialCheck(ISSUER, CREDENTIAL, GENERATION));
        assertThat(catalog.calls).containsExactly("credential " + ISSUER + " " + CREDENTIAL,
                "grants " + ACCOUNT + " " + ISSUER + " " + CREDENTIAL);
    }

    @Test
    void revokedAndNonCoveringGrantsArePassedOverForTheNextEligibleGrant() {
        catalog.currentCredential();
        catalog.hold(intakeGrant("grant-a", "datasource-1", null, true), true);
        catalog.hold(intakeGrant("grant-b", "datasource-2", null, true), false);
        catalog.hold(stepGrant("grant-c", "module-1", null, true), false);
        catalog.hold(intakeGrant("grant-d", "datasource-1", null, true), false);
        catalog.hold(intakeGrant("grant-e", "datasource-1", null, true), false);

        var authorized = authority.authorize(WRITER, INTAKE_OWNER, ACCOUNT, Optional.empty(), true);

        assertThat(authorized.grantIds()).containsExactly("grant-d");
        assertThat(authorized.grant().orElseThrow().getWriterGrantId()).isEqualTo("grant-d");
    }

    @Test
    void creationPassesOverGrantsWithoutAllowCreate() {
        catalog.currentCredential();
        catalog.hold(stepGrant("grant-a", "module-1", null, false), false);
        catalog.hold(stepGrant("grant-b", "module-1", "node-1", true), false);

        assertThat(authority.authorize(WRITER, STEP_OWNER, ACCOUNT, Optional.empty(), true).grantIds())
                .containsExactly("grant-b");
        assertThat(authority.authorize(WRITER, STEP_OWNER, ACCOUNT, Optional.empty(), false).grantIds())
                .containsExactly("grant-a");
    }

    @Test
    void creationWithoutAnAllowCreateGrantIsRefusedWhenTheOwnerIsKnown() {
        catalog.currentCredential();
        catalog.hold(intakeGrant("grant-a", "datasource-1", null, false), false);
        assertDenied(() -> authority.authorize(WRITER, INTAKE_OWNER, ACCOUNT, Optional.empty(), true),
                "The caller holds no active grant that may create this document");
    }

    @Test
    void ownerCoveredOnlyByAnotherCredentialIsRefused() {
        catalog.currentCredential();
        catalog.otherCredential(intakeGrant("grant-z", "datasource-1", null, true));
        assertDenied(() -> authority.authorize(WRITER, INTAKE_OWNER, ACCOUNT, Optional.empty(), false),
                "The caller holds no active grant for this owner");
    }

    @Test
    void ownerCoveredOnlyByARevokedGrantIsStillKnown() {
        catalog.currentCredential();
        catalog.hold(intakeGrant("grant-a", "datasource-1", null, true), true);
        assertDenied(() -> authority.authorize(WRITER, INTAKE_OWNER, ACCOUNT, Optional.empty(), false),
                "The caller holds no active grant for this owner");
    }

    @Test
    void ownerNoGrantCoversIsAFailedPreconditionWithOwnerUnknown() {
        catalog.currentCredential();
        catalog.hold(intakeGrant("grant-a", "datasource-2", null, true), false);
        catalog.otherCredential(stepGrant("grant-z", "module-1", null, true));

        assertThatThrownBy(() -> authority.authorize(WRITER, INTAKE_OWNER, ACCOUNT, Optional.empty(), false))
                .isInstanceOfSatisfying(AccessionException.class, refusal -> assertRefusal(refusal,
                        Status.Code.FAILED_PRECONDITION, RejectionReason.REJECTION_REASON_OWNER_UNKNOWN,
                        "No writer grant covers this owner in the account"));
        assertThat(catalog.calls).containsExactly("credential " + ISSUER + " " + CREDENTIAL,
                "grants " + ACCOUNT + " " + ISSUER + " " + CREDENTIAL, "ownerKnown " + ACCOUNT);
    }

    // ------------------------------------------------------------------ covers

    @Test
    void intakeOwnerCoverage() {
        var anyConnector = intakeGrant("g", "datasource-1", null, false).build();
        var connectorC = intakeGrant("g", "datasource-1", "connector-c", false).build();
        var ownerAny = intakeOwner("datasource-1", null);
        var ownerC = intakeOwner("datasource-1", "connector-c");

        assertThat(WriterAuthority.covers(anyConnector, ownerAny, Optional.empty())).isTrue();
        assertThat(WriterAuthority.covers(anyConnector, ownerAny, Optional.of("connector-x"))).isTrue();
        assertThat(WriterAuthority.covers(anyConnector, intakeOwner("datasource-2", null), Optional.empty())).isFalse();
        assertThat(WriterAuthority.covers(anyConnector, ownerC, Optional.of("connector-c"))).isTrue();
        assertThat(WriterAuthority.covers(anyConnector, ownerC, Optional.of("connector-x"))).isFalse();
        assertThat(WriterAuthority.covers(anyConnector, ownerC, Optional.empty())).isFalse();
        assertThat(WriterAuthority.covers(connectorC, ownerAny, Optional.of("connector-c"))).isTrue();
        assertThat(WriterAuthority.covers(connectorC, ownerAny, Optional.of("connector-x"))).isFalse();
        assertThat(WriterAuthority.covers(connectorC, ownerAny, Optional.empty())).isFalse();
        assertThat(WriterAuthority.covers(stepGrant("g", "module-1", null, false).build(), ownerAny, Optional.empty()))
                .isFalse();
    }

    @Test
    void stepOwnerCoverage() {
        assertThat(WriterAuthority.covers(stepGrant("g", "module-1", null, false).build(), STEP_OWNER,
                Optional.empty())).isTrue();
        assertThat(WriterAuthority.covers(stepGrant("g", "module-1", "node-1", false).build(), STEP_OWNER,
                Optional.empty())).isTrue();
        assertThat(WriterAuthority.covers(stepGrant("g", "module-1", "node-2", false).build(), STEP_OWNER,
                Optional.empty())).isFalse();
        assertThat(WriterAuthority.covers(stepGrant("g", "module-2", null, false).build(), STEP_OWNER,
                Optional.empty())).isFalse();
        assertThat(WriterAuthority.covers(intakeGrant("g", "module-1", null, false).build(), STEP_OWNER,
                Optional.empty())).isFalse();
    }

    // ------------------------------------------------------------------ authorizeOpaqueProducer

    @Test
    void processAuthorityHoldsNoProducerGrant() {
        assertThat(authority.authorizeOpaqueProducer(PROCESS, STEP, ACCOUNT, TYPE_URL)).isEmpty();
        assertThat(catalog.calls).isEmpty();
    }

    @Test
    void producerCredentialRefusals() {
        assertDenied(() -> authority.authorizeOpaqueProducer(UNBOUND, STEP, ACCOUNT, TYPE_URL),
                "Saving requires a credential-bound caller or process authority");
        assertDenied(() -> authority.authorizeOpaqueProducer(WRITER, STEP, ACCOUNT, TYPE_URL),
                "The caller's credential is not registered");
        catalog.credential = Optional.of(new Credential(GENERATION - 1, "writer", false));
        assertDenied(() -> authority.authorizeOpaqueProducer(WRITER, STEP, ACCOUNT, TYPE_URL),
                "The caller's credential generation is not current");
        catalog.credential = Optional.of(new Credential(GENERATION, "writer", true));
        assertDenied(() -> authority.authorizeOpaqueProducer(WRITER, STEP, ACCOUNT, TYPE_URL),
                "The caller's credential generation is not current");
    }

    @Test
    void producerGrantIsTakenInUnsignedUtf8GrantIdOrderWhateverTheCatalogOrder() {
        catalog.currentCredential();
        // UTF-16 order puts the surrogate pair first; unsigned UTF-8 order puts U+FF21 first.
        var emoji = stepGrant("😀", "module-1", null, false).addOpaqueTypeUrls(TYPE_URL);
        var fullwidth = stepGrant("Ａ", "module-1", null, false).addOpaqueTypeUrls(TYPE_URL);
        catalog.hold(emoji, false);
        catalog.hold(fullwidth, false);

        var selected = authority.authorizeOpaqueProducer(WRITER, STEP, ACCOUNT, TYPE_URL).orElseThrow();

        assertThat(selected.grantId()).isEqualTo("Ａ");
        assertThat(selected.evidence()).isEqualTo(WriterGrantEvidence.newBuilder().setWriterGrantId("Ａ")
                .setWriterGrantSha256(sha256(fullwidth.build().toByteArray()))
                .setGrantSnapshot(fullwidth.build().toByteString()).setSnapshotSchema(GRANT_SCHEMA.reference())
                .setCredentialGeneration(GENERATION).build());
    }

    @Test
    void producerGrantNeedsTheExactTypeUrlAndACoveringActiveStepScope() {
        catalog.currentCredential();
        catalog.hold(stepGrant("grant-a", "module-1", null, true).addOpaqueTypeUrls(TYPE_URL), true);
        catalog.hold(stepGrant("grant-b", "module-2", null, true).addOpaqueTypeUrls(TYPE_URL), false);
        catalog.hold(stepGrant("grant-c", "module-1", "node-2", true).addOpaqueTypeUrls(TYPE_URL), false);
        catalog.hold(stepGrant("grant-d", "module-1", null, true).addOpaqueTypeUrls(TYPE_URL + ".Inner")
                .addOpaqueTypeUrls("type.googleapis.com/example.v1.Opaqu"), false);
        catalog.hold(intakeGrant("grant-e", "datasource-1", null, true), false);

        assertThat(authority.authorizeOpaqueProducer(WRITER, STEP, ACCOUNT, TYPE_URL)).isEmpty();

        catalog.hold(stepGrant("grant-f", "module-1", "node-1", false).addOpaqueTypeUrls(TYPE_URL), false);
        assertThat(authority.authorizeOpaqueProducer(WRITER, STEP, ACCOUNT, TYPE_URL).orElseThrow().grantId())
                .isEqualTo("grant-f");
    }

    // ------------------------------------------------------------------ authorizeDelete

    @Test
    void deleteDerivesTheIntakeScopeFromStoredOwnership() {
        catalog.currentCredential();
        catalog.hold(intakeGrant("grant-a", "datasource-1", "connector-x", false), false);
        catalog.hold(intakeGrant("grant-b", "datasource-1", "connector-c", false), false);

        var authorized = authority.authorizeDelete(WRITER, ownership("connector-c"));

        assertThat(authorized.grantIds()).containsExactly("grant-b");
        assertThat(authorized.credential()).contains(new CredentialCheck(ISSUER, CREDENTIAL, GENERATION));
    }

    @Test
    void deleteWithoutAStoredConnectorNeedsADatasourceWideGrant() {
        catalog.currentCredential();
        catalog.hold(intakeGrant("grant-a", "datasource-1", "connector-c", false), false);
        // A connector-scoped grant does not cover the datasource-wide owner, so the owner is unknown.
        assertDenied(() -> authority.authorizeDelete(WRITER, ownership(null)),
                "The caller holds no intake authority for this document");

        catalog.hold(intakeGrant("grant-b", "datasource-1", null, false), false);
        assertThat(authority.authorizeDelete(WRITER, ownership(null)).grantIds()).containsExactly("grant-b");
    }

    @Test
    void processAuthorityDeletesAsTheStoredIntakeOwner() {
        var authorized = authority.authorizeDelete(PROCESS, ownership("connector-c"));
        assertThat(authorized.process().orElseThrow().getPrincipal()).isEqualTo("ingest-process");
        assertThat(authorized.grantIds()).isEmpty();
    }

    @Test
    void deleteTurnsAnUnknownOwnerIntoAWriteDenialKeepingTheCause() {
        catalog.currentCredential();

        assertThatThrownBy(() -> authority.authorizeDelete(WRITER, ownership("connector-c")))
                .isInstanceOfSatisfying(AccessionException.class, refusal -> {
                    assertRefusal(refusal, Status.Code.PERMISSION_DENIED,
                            RejectionReason.REJECTION_REASON_WRITE_DENIED,
                            "The caller holds no intake authority for this document");
                    assertThat(refusal.getCause()).isInstanceOfSatisfying(AccessionException.class,
                            cause -> assertRefusal(cause, Status.Code.FAILED_PRECONDITION,
                                    RejectionReason.REJECTION_REASON_OWNER_UNKNOWN,
                                    "No writer grant covers this owner in the account"));
                });
        assertThat(catalog.calls).last().isEqualTo("ownerKnown " + ACCOUNT);
    }

    @Test
    void deletePassesOtherRefusalsThroughUnchanged() {
        assertThatThrownBy(() -> authority.authorizeDelete(WRITER, ownership(null)))
                .isInstanceOfSatisfying(AccessionException.class, refusal -> {
                    assertRefusal(refusal, Status.Code.PERMISSION_DENIED,
                            RejectionReason.REJECTION_REASON_WRITE_DENIED, "The caller's credential is not registered");
                    assertThat(refusal.getCause()).isNull();
                });
    }

    // ------------------------------------------------------------------ toStatus

    @Test
    void refusalCrossesTheWireWithItsCodeMessageAndRejectionDetail() throws InvalidProtocolBufferException {
        var refusal = WriterAuthority.denied("The caller holds no active grant for this owner");

        var status = StatusProto.fromThrowable(AccessionGrpcService.toStatus(refusal));

        assertThat(status).isNotNull();
        assertThat(status.getCode()).isEqualTo(Status.Code.PERMISSION_DENIED.value());
        assertThat(status.getMessage()).isEqualTo("The caller holds no active grant for this owner");
        assertThat(status.getDetailsList()).hasSize(1);
        assertThat(status.getDetails(0).unpack(SaveRejection.class)).isEqualTo(SaveRejection.newBuilder()
                .setReason(RejectionReason.REJECTION_REASON_WRITE_DENIED)
                .setMessage("The caller holds no active grant for this owner").build());
        assertThat(Status.fromThrowable(AccessionGrpcService.toStatus(refusal)).getDescription())
                .isEqualTo("The caller holds no active grant for this owner");
    }

    @Test
    void refusalWithoutDetailOrMessageCrossesTheWireBare() {
        var status = StatusProto.fromThrowable(AccessionGrpcService.toStatus(
                new AccessionException(Status.Code.UNAVAILABLE, null, null)));

        assertThat(status).isNotNull();
        assertThat(status.getCode()).isEqualTo(Status.Code.UNAVAILABLE.value());
        assertThat(status.getMessage()).isEmpty();
        assertThat(status.getDetailsList()).isEmpty();
    }

    // ------------------------------------------------------------------ helpers

    private static void assertDenied(Runnable call, String message) {
        assertThatThrownBy(call::run).isInstanceOfSatisfying(AccessionException.class, refusal -> assertRefusal(
                refusal, Status.Code.PERMISSION_DENIED, RejectionReason.REJECTION_REASON_WRITE_DENIED, message));
    }

    private static void assertRefusal(AccessionException refusal, Status.Code code, RejectionReason reason,
            String message) {
        assertThat(refusal.code()).isEqualTo(code);
        assertThat(refusal.getMessage()).isEqualTo(message);
        assertThat(refusal.detail()).contains(SaveRejection.newBuilder().setReason(reason).setMessage(message).build());
    }

    private static SaveOwner intakeOwner(String datasource, String connector) {
        var intake = IntakeOrigin.newBuilder().setDatasourceId(datasource);
        if (connector != null) intake.setConnectorId(connector);
        return SaveOwner.newBuilder().setIntake(intake).build();
    }

    private static OwnershipContext ownership(String connector) {
        var ownership = OwnershipContext.newBuilder().setAccountId(ACCOUNT).setDatasourceId("datasource-1");
        if (connector != null) ownership.setConnectorId(connector);
        return ownership.build();
    }

    private static WriterGrant.Builder grant(String grantId, boolean allowCreate) {
        return WriterGrant.newBuilder().setGrantId(grantId).setAccountId(ACCOUNT)
                .setCredential(WriterCredential.newBuilder().setIssuer(ISSUER).setCredentialId(CREDENTIAL.toString()))
                .setAllowCreate(allowCreate);
    }

    private static WriterGrant.Builder intakeGrant(String grantId, String datasource, String connector,
            boolean allowCreate) {
        var intake = IntakeOrigin.newBuilder().setDatasourceId(datasource);
        if (connector != null) intake.setConnectorId(connector);
        return grant(grantId, allowCreate).setIntake(intake);
    }

    private static WriterGrant.Builder stepGrant(String grantId, String module, String node, boolean allowCreate) {
        var step = StepWriterScope.newBuilder().setModuleId(module);
        if (node != null) step.setNodeId(node);
        return grant(grantId, allowCreate).setStep(step);
    }

    private static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException missing) {
            throw new IllegalStateException(missing);
        }
    }

    /**
     * Answers the three writer-authority lookups from in-memory rows and records each
     * lookup; any other catalog operation fails the test.
     */
    private static final class FakeCatalog {
        Optional<Credential> credential = Optional.empty();
        private final List<Grant> held = new ArrayList<>();
        private final List<WriterGrant> account = new ArrayList<>();
        final List<String> calls = new ArrayList<>();

        void currentCredential() { credential = Optional.of(new Credential(GENERATION, "writer", false)); }

        /** A grant the caller's credential holds, returned in insertion order. */
        void hold(WriterGrant.Builder builder, boolean revoked) {
            var grant = builder.build();
            held.add(new Grant(grant, grant.toByteString(), sha256(grant.toByteArray()), revoked));
            account.add(grant);
        }

        /** A grant in the account held by another credential. */
        void otherCredential(WriterGrant.Builder builder) {
            account.add(builder.setCredential(WriterCredential.newBuilder().setIssuer(ISSUER)
                    .setCredentialId(UUID.fromString("00000000-0000-4000-8000-000000000002").toString())).build());
        }

        Catalog proxy() {
            return (Catalog) Proxy.newProxyInstance(Catalog.class.getClassLoader(), new Class<?>[] {Catalog.class},
                    (self, method, args) -> answer(method, args));
        }

        @SuppressWarnings("unchecked")
        private Object answer(Method method, Object[] args) {
            return switch (method.getName()) {
                case "credential" -> {
                    calls.add("credential " + args[0] + " " + args[1]);
                    yield credential;
                }
                case "grants" -> {
                    calls.add("grants " + args[0] + " " + args[1] + " " + args[2]);
                    yield List.copyOf(held);
                }
                case "ownerKnown" -> {
                    calls.add("ownerKnown " + args[0]);
                    yield account.stream().anyMatch((Predicate<WriterGrant>) args[1]);
                }
                default -> throw new AssertionError("Writer authority must not call Catalog." + method.getName());
            };
        }
    }
}
