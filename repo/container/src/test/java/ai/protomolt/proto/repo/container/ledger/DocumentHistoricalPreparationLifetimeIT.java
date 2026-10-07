package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.Any;
import com.google.protobuf.StringValue;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentHistoricalRestoreAssessmentIT.*;
import static org.assertj.core.api.Assertions.*;

/** SQL scopes and real schema assessment; source provider observations remain fixture supplied. */
@Testcontainers
class DocumentHistoricalPreparationLifetimeIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;
    private static final RepositoryCaller ADMIN = new RepositoryCaller("operator", true);
    private static final DocumentSecurity POLICY = DocumentSecurity.newBuilder()
            .addPermissions(AccessRule.newBuilder().setIdentityType("public").setIdentity("public").setAccess(Access.ACCESS_READ))
            .addPermissions(AccessRule.newBuilder().setIdentityType("public").setIdentity("public").setAccess(Access.ACCESS_WRITE)).build();

    @ParameterizedTest @ValueSource(strings = {"closed-admission", "held-resolver", "revoked-key", "resolver-failure"})
    void preparationRetainsExactRegisteredSourcesAndRechecksAuthorityAtDelivery(String variant) throws Exception {
        try (var c = context(POSTGRES)) {
            var document = Document.newBuilder().setDocId("historical-lifetime")
                    .setOwnership(OwnershipContext.newBuilder().setAccountId("account").setDatasourceId("source").setSecurity(POLICY))
                    .setStructuredData(Any.pack(StringValue.of("retained value"), "type.test")).build();
            var original = DocumentSchemaRetentionFixture.prepare(c, true, false, document, "source", "storage",
                    WriteProvenance.newBuilder().setNodeId("retained-producer").build());
            new DocumentSchemaPolicies(c.tx()).activate(original.batch().policy().policy(), 0, () -> {});
            var revision = DocumentSchemaRetentionFixture.publishBound(c, original, (em, candidate) -> {},
                    (em, id, manifest) -> original.retention().write(em, original.owner(), id, () -> {}));
            var fixture = new Fixture(original, revision);
            var binding = new RepositoryCredentialBinding("preparation-test", UUID.randomUUID(), 1);
            var caller = new RepositoryCaller("scoped", false, Set.of("account"), Set.of(), Optional.of(binding));
            var credentials = new RepositoryCredentialAuthorities(c.tx());
            credentials.register(ADMIN, binding, caller.principalName());
            var reads = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = reads.captureHistorical(caller, fixture.address(), revision);
            var budget = new PayloadBudget(64L * 1024 * 1024);
            try {
                var member = member(fixture, history).toBuilder();
                member.setOwnership(member.getOwnership().toBuilder().setSecurity(POLICY));
                long current = new DocumentLedger(c.tx()).findByNodeId(DocumentIds.nodeId(fixture.address())).orElseThrow().mutationRevision;
                member.setDestination(member.getDestination().toBuilder().setExpectedMutationRevision(current));
                var parsed = Document.newBuilder().setDocId(fixture.address().getDocId()).putParserResults("fresh",
                        ParserResult.newBuilder().setDocument(ParserDocument.newBuilder()
                                .setShape(Any.pack(StringValue.of("new parsed value"), "type.test"))).build()).build().toByteString();
                int ordinal = member.getPartsCount();
                member.addParts(DocumentPublicationPart.newBuilder().setSlot(
                        DocumentPublicationSlot.newBuilder().setPart(DocumentPart.DOCUMENT_PART_PARSED))
                        .setUpload(PublicationUpload.newBuilder().setSizeBytes(parsed.size())
                                .setSha256(DocumentPartCodec.sha256Hex(parsed.toByteArray())).setContentType("application/protobuf")));
                var command = new DocumentPublicationCommand(original.command().intent().toBuilder()
                        .setOperationId(UUID.randomUUID().toString()).setMembers(0, member).build());
                var key = new RepositoryOperationLedger.Key("account", caller.principalName(), command.operationId());
                var placement = original.prepared().members().getFirst().placement();
                var record = new DocumentPublicationPreparationRecord(key, command, DocumentPublicationSeeds.mint(key, command),
                        Map.of(placement.drive().id(), placement), Duration.ofMinutes(5), 0);
                var fragments = new HashMap<>(fixture.fragments()); fragments.put(ordinal, parsed);
                var scopes = new DocumentPublicationScopeCalls();
                try (var sources = DocumentHistoricalAssessmentSources.open(command, caller, List.of(history), NONE)) {
                    var registration = DocumentPublicationRegistration.historical(c.tx(), budget, record, sources,
                            UUID.randomUUID(), scopes, new DriveLedger(c.tx()), NONE);
                    var modes = Map.of("member", DocumentPublicationCandidate.Mode.TYPED);
                    var owner = registration.admitInitial(caller, modes, NONE).orElseThrow();
                    try (var execution = registration.historicalExecution(caller, owner, modes, NONE)) {
                        var entered = new CountDownLatch(1); var finish = new CountDownLatch(1);
                        var calls = new java.util.concurrent.atomic.AtomicInteger();
                        DocumentPublicationCandidate.Resolver resolver = (m, occurrence) -> {
                            calls.incrementAndGet();
                            assertThat(occurrence.ordinal()).isEqualTo(ordinal);
                            if (variant.equals("held-resolver")) {
                                entered.countDown();
                                try { if (!finish.await(15, TimeUnit.SECONDS)) throw new IllegalStateException("resolver gate timeout"); }
                                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IllegalStateException(interrupted); }
                            }
                            if (variant.equals("revoked-key")) credentials.revoke(ADMIN, binding, caller.principalName());
                            if (variant.equals("resolver-failure")) throw new IllegalStateException("injected schema resolver failure");
                            return DocumentSchemaRetentionFixture.definition(StringValue.getDescriptor(), true);
                        };
                        Callable<DocumentPublicationAssessment.Historical> prepare = () -> execution.prepareAssessment(caller,
                                original.batch().policy(), Map.of("member", fragments),
                                Optional.of(DocumentSchemaRetentionFixture.definition(Document.getDescriptor())), resolver,
                                new DocumentRevisionAssembly.Limits(4_000_000, 32, 100, 100, 100_000), Instant.now(), NONE);
                        long before = budget.reservedBytes();
                        if (variant.equals("revoked-key") || variant.equals("resolver-failure")) {
                            var failure = catchThrowable(prepare::call);
                            if (variant.equals("revoked-key")) assertThat(failure).isInstanceOfSatisfying(RepositoryException.class,
                                    denied -> assertThat(denied.code()).isEqualTo(RepositoryException.Code.UNAUTHENTICATED));
                            else assertThat(failure).hasStackTraceContaining("injected schema resolver failure");
                            assertThat(budget.reservedBytes()).isEqualTo(before);
                        } else {
                            DocumentPublicationAssessment.Historical assessment;
                            if (variant.equals("held-resolver")) {
                                try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
                                    var future = workers.submit(prepare);
                                    try {
                                        assertThat(entered.await(15, TimeUnit.SECONDS)).isTrue();
                                        sources.close(); history.close(); scopes.close();
                                        assertThat(sources.awaitDrained(Duration.ZERO)).isFalse();
                                        assertThat(scopes.awaitIdle(Duration.ZERO)).isFalse();
                                        assertThat(history.isDrained()).isFalse();
                                    } finally { finish.countDown(); }
                                    assessment = future.get(15, TimeUnit.SECONDS);
                                }
                            } else {
                                sources.close(); history.close(); scopes.close();
                                assessment = prepare.call();
                            }
                            try (assessment) {
                                execution.close();
                                assertThat(sources.awaitDrained(Duration.ZERO)).isFalse();
                                assertThat(scopes.awaitIdle(Duration.ZERO)).isFalse();
                                assertThat(history.isDrained()).isFalse();
                                assessment.inspect(access -> {
                                    assertThat(access.snapshot().typed().get("member").rootCount()).isEqualTo(2);
                                    assertThat(access.snapshot().failure()).isEmpty();
                                }, NONE);
                                assessment.verifySchemas(NONE);
                                var prepared = assessment.preparePhysical(record.placements(), record.seeds().attempts(), record.lease(),
                                        record.seeds().uploadTokens(), NONE);
                                // Still gated, before any runtime evidence or schema claims can be staged.
                                assertThatThrownBy(() -> assessment.create(caller, owner, prepared, Map.of(), null, null, null,
                                        UUID.randomUUID(), Instant.now(), NONE))
                                        .isInstanceOf(UnsupportedOperationException.class)
                                        .hasMessage("Claimed historical CREATE is not implemented");
                                assessment.inspect(access -> assertThat(access.snapshot().typed()).hasSize(1), NONE);
                            }
                            assertThat(scopes.awaitIdle(Duration.ZERO)).isTrue();
                            assertThat(sources.awaitDrained(Duration.ZERO)).isTrue();
                            assertThat(history.isDrained()).isTrue();
                        }
                        assertThat(calls).hasValue(1);
                    }
                    scopes.close();
                    assertThat(scopes.awaitIdle(Duration.ZERO)).isTrue();
                }
            } finally {
                assertThat(budget.reservedBytes()).isZero();
                release(reads, history);
            }
        }
    }
}
