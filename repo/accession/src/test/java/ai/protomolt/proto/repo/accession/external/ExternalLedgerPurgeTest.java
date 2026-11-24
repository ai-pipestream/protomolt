package ai.protomolt.proto.repo.accession.external;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.repo.external.v1.ExternalAttemptTrigger;
import ai.protomolt.proto.repo.external.v1.ExternalBindingLifecycle;
import ai.protomolt.proto.repo.external.v1.ExternalEndpointSummary;
import ai.protomolt.proto.repo.external.v1.ExternalIdentityProof;
import ai.protomolt.proto.repo.external.v1.ExternalPendingPhase;
import ai.protomolt.proto.repo.external.v1.ExternalProviderEvidence;
import ai.protomolt.proto.repo.external.v1.ExternalProviderSettlement;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationAction;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationAttempt;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationIntent;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationLedger;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationOperationRecord;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationOutcome;
import ai.protomolt.proto.repo.external.v1.ExternalPurgePreparation;
import ai.protomolt.proto.repo.external.v1.ExternalRetentionPolicy;
import ai.protomolt.proto.repo.external.v1.ExternalSystemBinding;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.Accession;
import ai.protomolt.proto.repo.v2.DocumentManifest;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.GetDocumentRequest;
import ai.protomolt.proto.repo.v2.GetDocumentResponse;
import ai.protomolt.proto.repo.v2.IntakeOrigin;
import ai.protomolt.proto.repo.v2.PurgeDocumentRequest;
import ai.protomolt.proto.repo.v2.PurgeDocumentResponse;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v2.SaveDocumentResponse;
import ai.protomolt.proto.repo.v2.SaveOwner;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.protovalidate.ProtovalidateRuleSource;
import ai.protomolt.proto.validate.source.ProtomoltRuleSource;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Ledger saves and the {@code PURGE_WITH_SOURCE} ledger purge against a repository that holds
 * one ledger document: which refusals are retried, and when the ledger is purged.
 */
class ExternalLedgerPurgeTest {

    private static final Timestamp T0 = Timestamp.newBuilder().setSeconds(1_770_000_000L).build();
    private static final String SHA = "a".repeat(64);
    private static final NodeAddress SOURCE = NodeAddress.newBuilder()
            .setAccountId("tenant-alpha").setGraphId("g").setGraphAddressId("ga").setDocId("doc-1")
            .build();
    private static final NodeAddress LEDGER = ExternalPublicationCoordinates.ledgerAddress(
            "tenant-alpha", "search", SOURCE);
    private static final ProtoValidator VALIDATOR = ProtoValidator.create(
            List.of(new ProtomoltRuleSource(), new ProtovalidateRuleSource()));

    private FakeDocuments repository;
    private Server server;
    private ManagedChannel channel;
    private ExternalLedgerStore ledgers;
    private ExternalReceiptSettlement settlement;

    @BeforeEach
    void start() throws Exception {
        repository = new FakeDocuments();
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).directExecutor().addService(repository).build().start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        DocumentServiceGrpc.DocumentServiceBlockingStub stub = DocumentServiceGrpc.newBlockingStub(channel);
        Clock clock = Clock.fixed(Instant.ofEpochSecond(1_770_000_100L), ZoneOffset.UTC);
        ledgers = new ExternalLedgerStore(stub, "default", SaveOwner.newBuilder()
                .setIntake(IntakeOrigin.newBuilder().setDatasourceId("ds-ledgers")).build(), clock);
        settlement = new ExternalReceiptSettlement(stub, ledgers, new ExternalProviderVerifier(), clock);
    }

    @AfterEach
    void stop() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    @Test
    void aRefusedSaveIsReportedAsItIsAndNotRetried() {
        repository.store(purgedLedger(ExternalRetentionPolicy.EXTERNAL_RETENTION_POLICY_PURGE_WITH_SOURCE));
        repository.refusals.add(Status.FAILED_PRECONDITION
                .withDescription("No writer grant covers this owner in the account"));

        assertThatThrownBy(() -> ledgers.updateLedgerWithCas(LEDGER, SOURCE, "search",
                LedgerAccess.keep(), "test", withPendingDeletion()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, refused -> {
                    assertThat(refused.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
                    assertThat(refused.getStatus().getDescription()).contains("No writer grant");
                });
        assertThat(repository.saveCalls).isEqualTo(1);
    }

    @Test
    void aStaleRevisionIsRetriedAgainstTheLedgerAsItIsThen() {
        repository.store(purgedLedger(ExternalRetentionPolicy.EXTERNAL_RETENTION_POLICY_PURGE_WITH_SOURCE));
        repository.refusals.add(Status.ABORTED.withDescription("Expected revision is stale"));

        ExternalPublicationLedger saved = ledgers.updateLedgerWithCas(LEDGER, SOURCE, "search",
                LedgerAccess.keep(), "test", withPendingDeletion());

        assertThat(saved.getOperationsCount()).isEqualTo(2);
        assertThat(repository.saveCalls).isEqualTo(2);
    }

    @Test
    void aLedgerBeingPurgedTakesNoFurtherSave() {
        repository.store(purgedLedger(ExternalRetentionPolicy.EXTERNAL_RETENTION_POLICY_PURGE_WITH_SOURCE)
                .toBuilder().setPurgeWithSourceStartedAt(T0).build());

        assertThatThrownBy(() -> ledgers.updateLedgerWithCas(LEDGER, SOURCE, "search",
                LedgerAccess.keep(), "test", withPendingDeletion()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, refused ->
                        assertThat(refused.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION));
        assertThat(repository.saveCalls).isZero();
    }

    @Test
    void theLedgerIsMarkedBeforeItIsPurged() {
        ExternalPublicationLedger settled =
                purgedLedger(ExternalRetentionPolicy.EXTERNAL_RETENTION_POLICY_PURGE_WITH_SOURCE);
        repository.store(settled);

        boolean purged = settlement.maybePurgeLedgerIfEligible(LEDGER, settled);

        assertThat(purged).isTrue();
        assertThat(repository.savedBeforePurge.hasPurgeWithSourceStartedAt()).isTrue();
        assertThat(repository.purgeCalls).isEqualTo(1);
    }

    @Test
    void aDeletionRegisteredAfterTheCheckKeepsTheLedger() {
        // The settling call read the ledger with nothing outstanding. Before its purge, another
        // purge handler registered a deletion of another object; that deletion is still out.
        ExternalPublicationLedger settled =
                purgedLedger(ExternalRetentionPolicy.EXTERNAL_RETENTION_POLICY_PURGE_WITH_SOURCE);
        repository.store(withPendingDeletion().apply(settled));

        boolean purged = settlement.maybePurgeLedgerIfEligible(LEDGER, settled);

        assertThat(purged).isFalse();
        assertThat(repository.purgeCalls).isZero();
        assertThat(repository.current().getOperationsCount()).isEqualTo(2);
        assertThat(repository.current().hasPurgeWithSourceStartedAt()).isFalse();
    }

    @Test
    void anOperationRecordedUnderRetainHistoryKeepsTheLedgerWhicheverDeletionSettlesLast() {
        ExternalPublicationLedger retained =
                purgedLedger(ExternalRetentionPolicy.EXTERNAL_RETENTION_POLICY_RETAIN_HISTORY);
        ExternalPublicationOperationRecord laterUnderPurge = retained.getOperations(0).toBuilder()
                .setOperationId("op-purge-2")
                .setBindingSnapshot(binding(ExternalRetentionPolicy.EXTERNAL_RETENTION_POLICY_PURGE_WITH_SOURCE))
                .build();
        ExternalPublicationLedger mixed = retained.toBuilder().addOperations(laterUnderPurge).build();
        repository.store(mixed);

        assertThat(settlement.maybePurgeLedgerIfEligible(LEDGER, mixed)).isFalse();
        assertThat(repository.purgeCalls).isZero();
    }

    @Test
    void aLedgerAlreadyMarkedIsPurgedWhenTheDeletionIsReplayed() {
        ExternalPublicationLedger marked =
                purgedLedger(ExternalRetentionPolicy.EXTERNAL_RETENTION_POLICY_PURGE_WITH_SOURCE)
                        .toBuilder().setPurgeWithSourceStartedAt(T0).build();
        repository.store(marked);

        assertThat(settlement.maybePurgeLedgerIfEligible(LEDGER, marked)).isTrue();
        assertThat(repository.saveCalls).isZero();
        assertThat(repository.purgeCalls).isEqualTo(1);
    }

    @Test
    void purgesLedgerWhenPendingPublicationSettlesRejectedAfterPurgeDeletionSettled() {
        ExternalSystemBinding purgeBinding =
                binding(ExternalRetentionPolicy.EXTERNAL_RETENTION_POLICY_PURGE_WITH_SOURCE);
        ExternalPublicationIntent pubIntent = ExternalPublicationIntent.newBuilder()
                .setEncodingVersion(1).setAccountId("tenant-alpha").setSourceAddress(SOURCE)
                .setSourceRevision(1L).setBindingId("search").setBindingRevision(1L)
                .setAction(ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_PUBLISH)
                .setExternalObjectId("object-a")
                .build();
        String pubSha = ExternalOperationKeys.intentSha256(pubIntent);
        ExternalPublicationOperationRecord pendingPub = ExternalPublicationOperationRecord.newBuilder()
                .setOperationId("op-pub-1").setIntentSha256(pubSha).setIntent(pubIntent)
                .setBindingSnapshot(purgeBinding)
                .addDispatchCredentialBindingSha256S(SHA)
                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING)
                .setPendingPhase(ExternalPendingPhase.EXTERNAL_PENDING_PHASE_UNRESOLVED_LOST_REPLY)
                .addAttempts(ExternalPublicationAttempt.newBuilder()
                        .setAttemptNumber(1)
                        .setTrigger(ExternalAttemptTrigger.EXTERNAL_ATTEMPT_TRIGGER_INITIAL)
                        .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING)
                        .setPendingPhase(ExternalPendingPhase.EXTERNAL_PENDING_PHASE_UNRESOLVED_LOST_REPLY)
                        .setStartedAt(T0))
                .setCreatedAt(T0).setUpdatedAt(T0)
                .build();
        ExternalPublicationLedger withPendingPub =
                purgedLedger(ExternalRetentionPolicy.EXTERNAL_RETENTION_POLICY_PURGE_WITH_SOURCE)
                        .toBuilder()
                        .addOperations(pendingPub)
                        .build();
        repository.store(withPendingPub);
        assertThat(settlement.maybePurgeLedgerIfEligible(LEDGER, withPendingPub)).isFalse();

        ExternalProviderEvidence rejected = ExternalProviderEvidence.newBuilder()
                .setProvider("protomolt-search").setContractVersion("managed-search/v1")
                .setVerifiedServiceProfileId("profile").setVerifiedServiceEndpointId("ingest")
                .setCredentialBindingSha256(SHA).setExternalObjectId("object-a")
                .setRequestIntentSha256(pubSha).setReceiptSha256(SHA)
                .setProviderReceiptId("receipt-rejected")
                .setSettlement(ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_REJECTED)
                .setDiagnosticCode("NOT_FOUND_AT_PROVIDER")
                .setVerifiedAt(T0)
                .build();
        var endpoint = ai.protomolt.proto.grpc.profile.v1.ServiceEndpoint.newBuilder()
                .setName("ingest").setHost("search.internal.example.org").setPort(8443)
                .setTransport(ai.protomolt.proto.grpc.profile.v1.Transport.TRANSPORT_TLS)
                .build();
        var profile = ai.protomolt.proto.grpc.profile.v1.ServiceProfile.newBuilder()
                .setName("profile").addEndpoints(endpoint).build();
        var auth = new ExternalProviderGateway.OutboundAuthContext(
                purgeBinding, profile, endpoint, new io.grpc.Metadata(), SHA);

        ExternalPublicationCoordinator.OperationResult reconciled = settlement.applyProviderReceipt(
                LEDGER, SOURCE, "search", LedgerAccess.keep(), "op-pub-1", 2,
                ExternalAttemptTrigger.EXTERNAL_ATTEMPT_TRIGGER_RECONCILIATION, T0,
                pubSha, auth, rejected);
        assertThat(reconciled.operation().getOutcome())
                .isEqualTo(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_FAILED);
        assertThat(reconciled.ledgerPurged()).isTrue();
        assertThat(repository.purgeCalls).isEqualTo(1);
    }

    @Test
    void nonAdminWriterMarksPurgeStartedWithoutThrowingAndAdminReplayFinishesPurge() {
        ExternalPublicationLedger settled =
                purgedLedger(ExternalRetentionPolicy.EXTERNAL_RETENTION_POLICY_PURGE_WITH_SOURCE);
        repository.store(settled);
        repository.purgeRefusals.add(
                Status.PERMISSION_DENIED.withDescription("Purge requires administrator status"));

        boolean writerPurged = settlement.maybePurgeLedgerIfEligible(LEDGER, settled);
        assertThat(writerPurged).isFalse();
        assertThat(repository.purgeCalls).isEqualTo(1);
        assertThat(repository.current().hasPurgeWithSourceStartedAt()).isTrue();

        boolean adminPurged = settlement.maybePurgeLedgerIfEligible(LEDGER, repository.current());
        assertThat(adminPurged).isTrue();
        assertThat(repository.purgeCalls).isEqualTo(2);
    }

    @Test
    void failBeforeDispatchOnRemovedLedgerThrowsFailedPreconditionWithoutSavingEmptyLedger() {
        Clock clock = Clock.fixed(Instant.ofEpochSecond(1_770_000_100L), ZoneOffset.UTC);
        ExternalPublishAttempts attempts =
                new ExternalPublishAttempts(ledgers, settlement, null, null, clock);
        ExternalPublicationIntent pubIntent = ExternalPublicationIntent.newBuilder()
                .setEncodingVersion(1).setAccountId("tenant-alpha").setSourceAddress(SOURCE)
                .setSourceRevision(1L).setBindingId("search").setBindingRevision(1L)
                .setAction(ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_PUBLISH)
                .setExternalObjectId("object-a")
                .build();

        assertThatThrownBy(() -> attempts.failBeforeDispatch(
                LEDGER, LedgerAccess.keep(), "op-missing", pubIntent,
                "SOURCE_REMOVED_BEFORE_DISPATCH", "Source removed"))
                .isInstanceOfSatisfying(StatusRuntimeException.class, refused -> {
                    assertThat(refused.getStatus().getCode())
                            .isEqualTo(Status.Code.FAILED_PRECONDITION);
                    assertThat(refused.getStatus().getDescription()).contains("no longer recorded");
                });
        assertThat(repository.saveCalls).isZero();
    }

    @Test
    void theFixturesAreValidLedgers() {
        for (ExternalPublicationLedger ledger : List.of(
                purgedLedger(ExternalRetentionPolicy.EXTERNAL_RETENTION_POLICY_PURGE_WITH_SOURCE),
                withPendingDeletion().apply(
                        purgedLedger(ExternalRetentionPolicy.EXTERNAL_RETENTION_POLICY_PURGE_WITH_SOURCE)))) {
            assertThat(VALIDATOR.validate(ledger).violations()).isEmpty();
        }
    }

    private static ExternalSystemBinding binding(ExternalRetentionPolicy retention) {
        return ExternalSystemBinding.newBuilder()
                .setBindingId("search").setAccountId("tenant-alpha").setBindingRevision(1L)
                .setProvider("protomolt-search").setServiceProfileId("profile")
                .setServiceEndpointId("ingest").setDataset("corpus").setNamespace("ns")
                .setContractVersion("managed-search/v1")
                .setEndpointSummary(ExternalEndpointSummary.newBuilder()
                        .setHost("search.internal.example.org").setPort(8443).setTlsEnabled(true))
                .setLifecycle(ExternalBindingLifecycle.EXTERNAL_BINDING_LIFECYCLE_ACTIVE)
                .setRetentionPolicy(retention)
                .setRequiredIdentityProof(ExternalIdentityProof.EXTERNAL_IDENTITY_PROOF_MANAGED_SEARCH)
                .setCreatedAt(T0)
                .build();
    }

    private static ExternalPublicationIntent purgeDeletion(String objectId) {
        return ExternalPublicationIntent.newBuilder()
                .setEncodingVersion(1).setAccountId("tenant-alpha").setSourceAddress(SOURCE)
                .setSourceRevision(0L).setPurgeId("purge-1").setBindingId("search").setBindingRevision(1L)
                .setAction(ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_DELETE)
                .setExternalObjectId(objectId)
                .build();
    }

    /** A purged ledger whose one operation, a purge deletion, settled as DELETED. */
    private static ExternalPublicationLedger purgedLedger(ExternalRetentionPolicy retention) {
        ExternalPublicationIntent intent = purgeDeletion("object-a");
        String intentSha = ExternalOperationKeys.intentSha256(intent);
        ExternalProviderEvidence deleted = ExternalProviderEvidence.newBuilder()
                .setProvider("protomolt-search").setContractVersion("managed-search/v1")
                .setVerifiedServiceProfileId("profile").setVerifiedServiceEndpointId("ingest")
                .setCredentialBindingSha256(SHA).setExternalObjectId("object-a")
                .setRequestIntentSha256(intentSha).setReceiptSha256(SHA).setProviderReceiptId("receipt-1")
                .setSettlement(ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_DELETED)
                .setVerifiedAt(T0)
                .build();
        ExternalPublicationOperationRecord deletion = ExternalPublicationOperationRecord.newBuilder()
                .setOperationId("op-purge-1").setIntentSha256(intentSha).setIntent(intent)
                .setBindingSnapshot(binding(retention))
                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_DELETED)
                .addAttempts(ExternalPublicationAttempt.newBuilder()
                        .setAttemptNumber(1).setTrigger(ExternalAttemptTrigger.EXTERNAL_ATTEMPT_TRIGGER_INITIAL)
                        .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_DELETED)
                        .setStartedAt(T0).setFinishedAt(T0).setEvidence(deleted))
                .setTerminalEvidence(deleted)
                .setCreatedAt(T0).setUpdatedAt(T0)
                .build();
        return ExternalPublicationLedger.newBuilder()
                .setAccountId("tenant-alpha").setBindingId("search").setSourceAddress(SOURCE)
                .setLogicalSourceKey(ByteString.copyFrom(ExternalPublicationCoordinates.logicalSourceKey(SOURCE)))
                .setSourcePurged(true).setLastPurgeId("purge-1")
                .setPurgePreparation(ExternalPurgePreparation.newBuilder()
                        .setPurgeId("purge-1").setRulesFromSourceRevision(1L).setPreparedAt(T0))
                .addOperations(deletion)
                .build();
    }

    /** Registers a second purge deletion, of another object, still in flight. */
    private static UnaryOperator<ExternalPublicationLedger> withPendingDeletion() {
        return ledger -> {
            ExternalPublicationIntent intent = purgeDeletion("object-b");
            return ledger.toBuilder().addOperations(ExternalPublicationOperationRecord.newBuilder()
                    .setOperationId("op-purge-b").setIntentSha256(ExternalOperationKeys.intentSha256(intent))
                    .setIntent(intent)
                    .setBindingSnapshot(binding(ExternalRetentionPolicy.EXTERNAL_RETENTION_POLICY_PURGE_WITH_SOURCE))
                    .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_DELETION_PENDING)
                    .setPendingPhase(ExternalPendingPhase.EXTERNAL_PENDING_PHASE_DELETION_IN_FLIGHT)
                    .addAttempts(ExternalPublicationAttempt.newBuilder()
                            .setAttemptNumber(1).setTrigger(ExternalAttemptTrigger.EXTERNAL_ATTEMPT_TRIGGER_INITIAL)
                            .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_DELETION_PENDING)
                            .setPendingPhase(ExternalPendingPhase.EXTERNAL_PENDING_PHASE_DELETION_IN_FLIGHT)
                            .setStartedAt(T0))
                    .setCreatedAt(T0).setUpdatedAt(T0)
                    .build()).build();
        };
    }

    /** A repository holding the one ledger document, with compare-and-swap on its revision. */
    private static final class FakeDocuments extends DocumentServiceGrpc.DocumentServiceImplBase {
        private Document document;
        private long revision;
        final Deque<Status> refusals = new ArrayDeque<>();
        final Deque<Status> purgeRefusals = new ArrayDeque<>();
        int saveCalls;
        int purgeCalls;
        ExternalPublicationLedger savedBeforePurge;

        void store(ExternalPublicationLedger ledger) {
            document = Document.newBuilder().setDocId(LEDGER.getDocId())
                    .setStructuredData(Any.pack(ledger)).build();
            revision++;
        }

        ExternalPublicationLedger current() {
            try {
                return document.getStructuredData().unpack(ExternalPublicationLedger.class);
            } catch (InvalidProtocolBufferException corrupt) {
                throw new IllegalStateException(corrupt);
            }
        }

        @Override
        public void getDocument(GetDocumentRequest request, StreamObserver<GetDocumentResponse> response) {
            if (document == null) {
                response.onError(Status.NOT_FOUND.asRuntimeException());
                return;
            }
            response.onNext(GetDocumentResponse.newBuilder().setDocument(document)
                    .setManifest(DocumentManifest.newBuilder().setAddress(LEDGER).setRevision(revision))
                    .build());
            response.onCompleted();
        }

        @Override
        public void saveDocument(SaveDocumentRequest request, StreamObserver<SaveDocumentResponse> response) {
            saveCalls++;
            if (!refusals.isEmpty()) {
                response.onError(refusals.pop().asRuntimeException());
                return;
            }
            if (request.getExpected().getRevision() != revision) {
                response.onError(Status.ABORTED.withDescription("stale").asRuntimeException());
                return;
            }
            document = request.getDocument();
            revision++;
            savedBeforePurge = current();
            response.onNext(SaveDocumentResponse.newBuilder().setOperationId(request.getOperationId())
                    .setAccession(Accession.newBuilder().setAddress(LEDGER).setRevision(revision)).build());
            response.onCompleted();
        }

        @Override
        public void purgeDocument(PurgeDocumentRequest request, StreamObserver<PurgeDocumentResponse> response) {
            purgeCalls++;
            if (!purgeRefusals.isEmpty()) {
                response.onError(purgeRefusals.pop().asRuntimeException());
                return;
            }
            document = null;
            response.onNext(PurgeDocumentResponse.newBuilder().setPurgeId(request.getPurgeId()).build());
            response.onCompleted();
        }
    }
}
