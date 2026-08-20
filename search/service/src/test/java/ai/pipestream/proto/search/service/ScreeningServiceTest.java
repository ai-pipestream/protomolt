package ai.pipestream.proto.search.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.repo.v1.NodeAddress;
import ai.pipestream.proto.repo.v1.SearchMetadata;
import ai.pipestream.proto.screening.Screener;
import ai.pipestream.proto.screening.ScreeningEngine;
import ai.pipestream.proto.search.v1.IndexDocumentRequest;
import ai.pipestream.proto.search.v1.IndexDocumentResponse;
import ai.pipestream.proto.search.v1.SearchIndexServiceGrpc;
import ai.pipestream.proto.types.ScreeningConfig;
import ai.pipestream.proto.types.ScreeningPolicy;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;

/**
 * The screening mount at the indexing boundary: a service built with a screening
 * supplier refuses fail-closed until a mount is live, masks detected spans
 * in the declared screened field on the way in, carries the model version
 * and threshold as evidence on the response, and refuses outright only
 * under the explicit REFUSE policy. The engine is a deterministic fake —
 * the mount seam is the subject here; the OpenNLP engine has its own pins.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ScreeningServiceTest {

    @TempDir
    static Path work;

    static final Document DOCUMENT = Document.newBuilder()
            .setDocId("memo")
            .setSearchMetadata(SearchMetadata.newBuilder()
                    .setTitle("Meeting notes")
                    .setBody("Meeting notes by Ada about the engine"))
            .build();

    /** Detects the word "Ada" with confidence 0.9. */
    static final ScreeningEngine ADA_ENGINE = new ScreeningEngine() {
        @Override
        public List<Detection> detect(String text) {
            int at = text.indexOf("Ada");
            return at < 0 ? List.of()
                    : List.of(new Detection("person", at, at + 3, 0.9));
        }

        @Override
        public String modelVersion() {
            return "fake-3.7";
        }
    };

    static final AtomicReference<Screener> MOUNT = new AtomicReference<>();

    static SearchServices service;
    static ManagedChannel channel;
    static SearchIndexServiceGrpc.SearchIndexServiceBlockingStub stub;

    @BeforeAll
    static void bootScreenedService() throws Exception {
        DocumentFetcher fetcher = address -> DOCUMENT;
        service = SearchServices.build(
                new SearchServiceConfig(0, work.resolve("screened"), Map.of(
                        "repo-document-lexical", RepoDocumentMapping.served())),
                fetcher,
                null,
                MOUNT::get);
        String name = InProcessServerBuilder.generateName();
        service.startInProcess(name);
        channel = InProcessChannelBuilder.forName(name).build();
        stub = SearchIndexServiceGrpc.newBlockingStub(channel);
    }

    @AfterAll
    static void shutdown() {
        if (channel != null) {
            channel.shutdownNow();
        }
        if (service != null) {
            service.close();
        }
    }

    static IndexDocumentRequest index() {
        return IndexDocumentRequest.newBuilder()
                .setAddress(NodeAddress.newBuilder().setDocId("memo"))
                .setMappingSubject("repo-document-lexical")
                .build();
    }

    static Screener screener(ScreeningPolicy policy) {
        return new Screener(ADA_ENGINE, ScreeningConfig.newBuilder()
                .setSensitivityClass("screened")
                .setModelRef("file:unused")
                .setThreshold(0.5)
                .setPolicy(policy)
                .build());
    }

    @Test
    @Order(1)
    void aConfiguredServiceWithoutALiveMountRefusesFailClosed() {
        assertThatThrownBy(() -> stub.indexDocument(index()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode())
                            .isEqualTo(Status.Code.FAILED_PRECONDITION);
                    assertThat(e.getStatus().getDescription())
                            .contains("no mount is live");
                });
    }

    @Test
    @Order(2)
    void theMaskMountRedactsTheScreenedFieldAndCarriesEvidence() {
        MOUNT.set(screener(ScreeningPolicy.SCREENING_POLICY_MASK));

        IndexDocumentResponse response = stub.indexDocument(index());

        assertThat(response.getDocId()).isEqualTo("memo");
        // Evidence: the screened path, the model version, and the threshold.
        assertThat(response.getScreenedFieldsList())
                .containsExactly("search_metadata.body");
        assertThat(response.getScreeningModelVersion()).isEqualTo("fake-3.7");
        assertThat(response.getScreeningThreshold()).isEqualTo(0.5);

        // The MASKED form is what indexed: the detected name is not findable,
        // the rest of the body is.
        var search = ai.pipestream.proto.search.v1.SearchServiceGrpc
                .newBlockingStub(channel);
        assertThat(search.search(ai.pipestream.proto.search.v1.SearchRequest.newBuilder()
                .setMappingSubject("repo-document-lexical").setQuery("Ada").setK(10)
                .setLane(ai.pipestream.proto.search.v1.SearchLane.SEARCH_LANE_LEXICAL)
                .build())
                .getHitsList())
                .noneMatch(hit -> hit.getDocId().equals("memo"));
        assertThat(search.search(ai.pipestream.proto.search.v1.SearchRequest.newBuilder()
                .setMappingSubject("repo-document-lexical").setQuery("engine").setK(10)
                .setLane(ai.pipestream.proto.search.v1.SearchLane.SEARCH_LANE_LEXICAL)
                .build())
                .getHitsList())
                .anyMatch(hit -> hit.getDocId().equals("memo"));
    }

    @Test
    @Order(3)
    void theRefuseMountRefusesNamingItsEvidenceButNeverTheText() {
        MOUNT.set(screener(ScreeningPolicy.SCREENING_POLICY_REFUSE));

        assertThatThrownBy(() -> stub.indexDocument(index()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode())
                            .isEqualTo(Status.Code.FAILED_PRECONDITION);
                    assertThat(e.getStatus().getDescription())
                            .contains("person")
                            .contains("search_metadata.body")
                            .contains("fake-3.7")
                            .doesNotContain("Ada");
                });
    }
}
