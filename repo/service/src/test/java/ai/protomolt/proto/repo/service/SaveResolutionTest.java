package ai.protomolt.proto.repo.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.repo.container.ledger.DocumentRowKind;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.SaveDocumentRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;

/**
 * Where a save lands is decided from the request alone, and a wrong decision is the kind
 * that does not announce itself: the document stores successfully at an address nobody
 * will look for it at. So these tests are mostly about refusals, and about refusals that
 * name the field, because the caller cannot fix what the error does not identify.
 */
class SaveResolutionTest {

    private static final String ACCOUNT = "acct-1";
    private static final String DATASOURCE = "ds-1";

    private static SaveDocumentRequest.Builder request() {
        return SaveDocumentRequest.newBuilder()
                .setDrive("primary")
                .setDocument(Document.newBuilder()
                        .setDocId("doc-1")
                        .setOwnership(OwnershipContext.newBuilder()
                                .setAccountId(ACCOUNT)
                                .setDatasourceId(DATASOURCE)));
    }

    private static SaveDocumentRequest.Builder intake() {
        return request()
                .setUseDatasourceId(true)
                .setGraphId(SaveResolution.intakeGraphId(ACCOUNT));
    }

    private static SaveDocumentRequest.Builder pipeline() {
        return request().setGraphLocationId("node-7").setGraphId("graph-a");
    }

    private static void assertRefusesNaming(ThrowingCallable call, String... fragments) {
        assertThatThrownBy(call)
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(t -> assertThat(Status.fromThrowable(t).getCode())
                        .isEqualTo(Status.Code.INVALID_ARGUMENT))
                .hasMessageContainingAll(fragments);
    }

    // --- the shape every save needs ---------------------------------------------

    @Test
    void aRequestWithoutADocumentIsRefused() {
        assertRefusesNaming(() -> SaveResolution.resolve(
                SaveDocumentRequest.newBuilder().setDrive("primary").build()), "document");
    }

    @Test
    void aDocumentWithoutOwnershipIsRefused() {
        assertRefusesNaming(() -> SaveResolution.resolve(intake()
                .setDocument(Document.newBuilder().setDocId("doc-1"))
                .build()), "document.ownership");
    }

    @Test
    void aBlankAccountIsRefusedRatherThanDefaulted() {
        assertRefusesNaming(() -> SaveResolution.resolve(intake()
                .setDocument(Document.newBuilder().setDocId("doc-1")
                        .setOwnership(OwnershipContext.newBuilder().setDatasourceId(DATASOURCE)))
                .build()), "document.ownership.account_id");
    }

    @Test
    void aBlankDriveIsRefused() {
        assertRefusesNaming(() -> SaveResolution.resolve(intake().setDrive("").build()), "drive");
    }

    @Test
    void neitherArmOfTheOriginDiscriminatorIsRefused() {
        assertRefusesNaming(() -> SaveResolution.resolve(request().setGraphId("graph-a").build()),
                "use_datasource_id", "graph_location_id");
    }

    /**
     * The arm is a oneof, so its presence is tracked separately from its value: a caller
     * that sets the intake flag to {@code false} has still chosen the intake arm. Reading
     * the value instead of the case would silently route such a save to the fallthrough.
     */
    @Test
    void theIntakeArmIsChosenByPresenceNotByTheFlagsValue() {
        SaveResolution.Resolved resolved = SaveResolution.resolve(
                intake().setUseDatasourceId(false).build());
        assertThat(resolved.rowKind()).isEqualTo(DocumentRowKind.INTAKE);
    }

    // --- the document id --------------------------------------------------------

    @Test
    void aBlankDocumentIdIsMintedAndCarriedIntoTheAddress() {
        SaveResolution.Resolved resolved = SaveResolution.resolve(intake()
                .setDocument(Document.newBuilder()
                        .setOwnership(OwnershipContext.newBuilder()
                                .setAccountId(ACCOUNT).setDatasourceId(DATASOURCE)))
                .build());

        String minted = resolved.doc().getDocId();
        assertThat(minted).isNotBlank();
        assertThat(UUID.fromString(minted)).isNotNull();
        assertThat(resolved.address().getDocId()).isEqualTo(minted);
    }

    @Test
    void aSuppliedDocumentIdIsLeftAlone() {
        assertThat(SaveResolution.resolve(intake().build()).doc().getDocId()).isEqualTo("doc-1");
    }

    // --- intake saves -----------------------------------------------------------

    @Test
    void anIntakeSaveIsAddressedByItsDatasource() {
        SaveResolution.Resolved resolved = SaveResolution.resolve(intake().build());

        assertThat(resolved.rowKind()).isEqualTo(DocumentRowKind.INTAKE);
        assertThat(resolved.clusterId()).isNull();
        assertThat(resolved.address().getDocId()).isEqualTo("doc-1");
        assertThat(resolved.address().getGraphAddressId()).isEqualTo(DATASOURCE);
        assertThat(resolved.address().getAccountId()).isEqualTo(ACCOUNT);
        assertThat(resolved.address().getGraphId()).isEqualTo("intake:" + ACCOUNT);
    }

    @Test
    void anIntakeSaveWithoutADatasourceIsRefused() {
        assertRefusesNaming(() -> SaveResolution.resolve(intake()
                .setDocument(Document.newBuilder().setDocId("doc-1")
                        .setOwnership(OwnershipContext.newBuilder().setAccountId(ACCOUNT)))
                .build()), "datasource_id");
    }

    @Test
    void anIntakeSaveWithoutAGraphIdSaysWhichOneItWanted() {
        assertRefusesNaming(() -> SaveResolution.resolve(
                request().setUseDatasourceId(true).build()),
                "graph_id", "intake:" + ACCOUNT);
    }

    @Test
    void anIntakeSaveUnderAnotherAccountsGraphIsRefused() {
        assertRefusesNaming(() -> SaveResolution.resolve(
                request().setUseDatasourceId(true).setGraphId("intake:someone-else").build()),
                "intake:" + ACCOUNT, "intake:someone-else");
    }

    @Test
    void anIntakeSaveCarryingAClusterIsRefused() {
        // Intake is its own single-node graph, so a routing hint here is a category error
        // rather than a harmless extra.
        assertRefusesNaming(() -> SaveResolution.resolve(intake().setClusterId("c-1").build()),
                "cluster_id", "intake");
    }

    @Test
    void anIntakeSaveWithAnExplicitlyBlankClusterIsAllowed() {
        // The field is present but says nothing, which is not the same as naming a cluster.
        assertThat(SaveResolution.resolve(intake().setClusterId("").build()).clusterId()).isNull();
    }

    // --- pipeline saves ---------------------------------------------------------

    @Test
    void aPipelineSaveIsAddressedByItsGraphNode() {
        SaveResolution.Resolved resolved = SaveResolution.resolve(pipeline().build());

        assertThat(resolved.rowKind()).isEqualTo(DocumentRowKind.PIPELINE);
        assertThat(resolved.address().getGraphAddressId()).isEqualTo("node-7");
        assertThat(resolved.address().getGraphId()).isEqualTo("graph-a");
        assertThat(resolved.clusterId()).isNull();
    }

    @Test
    void aPipelineSaveKeepsItsClusterHint() {
        assertThat(SaveResolution.resolve(pipeline().setClusterId("c-1").build()).clusterId())
                .isEqualTo("c-1");
    }

    @Test
    void aBlankClusterHintIsNoHint() {
        assertThat(SaveResolution.resolve(pipeline().setClusterId("  ").build()).clusterId())
                .isNull();
    }

    @Test
    void aPipelineSaveWithoutANodeIsRefused() {
        assertRefusesNaming(() -> SaveResolution.resolve(
                pipeline().setGraphLocationId("").build()), "graph_location_id");
    }

    @Test
    void aPipelineSaveWithoutAGraphIdIsRefused() {
        assertRefusesNaming(() -> SaveResolution.resolve(
                request().setGraphLocationId("node-7").build()), "graph_id", "node-7");
    }

    /**
     * The two origins share an address space, so a pipeline row claiming an intake graph
     * would collide with the intake row at the same node id. The prefix is reserved.
     */
    @Test
    void aPipelineSaveMayNotClaimAnIntakeGraph() {
        assertRefusesNaming(() -> SaveResolution.resolve(
                pipeline().setGraphId("intake:" + ACCOUNT).build()), "intake");
    }

    @Test
    void theIntakeGraphIdIsBuiltFromTheAccount() {
        assertThat(SaveResolution.intakeGraphId("acct-9")).isEqualTo("intake:acct-9");
        assertThat(SaveResolution.intakeGraphId("acct-9"))
                .startsWith(SaveResolution.INTAKE_GRAPH_PREFIX);
    }

    // --- what a resolved save carries onward ------------------------------------

    @Test
    void theProviderMetadataCarriesEveryAddressSegment() {
        SaveResolution.Resolved resolved = SaveResolution.resolve(pipeline().build());

        assertThat(SaveResolution.s3Metadata(resolved))
                .containsEntry("doc-id", "doc-1")
                .containsEntry("account-id", ACCOUNT)
                .containsEntry("graph-id", "graph-a")
                .containsEntry("graph-address-id", "node-7")
                .containsEntry("row-kind", DocumentRowKind.PIPELINE);
    }
}
