package ai.protomolt.proto.search.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.search.v1.IndexDocumentRequest;
import ai.protomolt.proto.search.v1.IndexDocumentResponse;
import ai.protomolt.proto.search.v1.SearchIndexServiceGrpc;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.model.CelConstraint;
import ai.protomolt.proto.validate.model.FieldConstraints;
import ai.protomolt.proto.validate.model.MessageConstraints;
import ai.protomolt.proto.validate.spi.ValidationRuleSource;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The opt-in document gate at the indexing boundary: a service built with a gate
 * validates every fetched document against the gate's declared rules
 * before anything indexes and refuses violations as failed preconditions
 * naming them; a service built without one behaves exactly as before. The
 * gate here carries a deliberately simple rule so the mechanism is pinned
 * independently of any particular dialect; the platform builds its gate
 * over the live taxonomy mounts.
 */
class DocumentGateTest {

    @TempDir
    static Path work;

    static final Map<String, Document> DOCUMENTS = Map.of(
            "titled", Document.newBuilder()
                    .setDocId("titled")
                    .setSearchMetadata(SearchMetadata.newBuilder()
                            .setTitle("A Proper Title")
                            .setBody("The archive keeps every treaty ever signed."))
                    .build(),
            "untitled", Document.newBuilder()
                    .setDocId("untitled")
                    .setSearchMetadata(SearchMetadata.newBuilder()
                            .setBody("A body with no title at all."))
                    .build());

    static SearchServices gated;
    static SearchServices ungated;
    static ManagedChannel gatedChannel;
    static ManagedChannel ungatedChannel;
    static SearchIndexServiceGrpc.SearchIndexServiceBlockingStub gatedStub;
    static SearchIndexServiceGrpc.SearchIndexServiceBlockingStub ungatedStub;

    /** A rule source demanding a title on every gated document. */
    static final ValidationRuleSource TITLE_RULE = new ValidationRuleSource() {
        @Override
        public Optional<FieldConstraints> fieldConstraints(FieldDescriptor field) {
            return Optional.empty();
        }

        @Override
        public Optional<MessageConstraints> messageConstraints(Descriptor message) {
            return Document.getDescriptor().getFullName().equals(message.getFullName())
                    ? Optional.of(new MessageConstraints(List.of(new CelConstraint(
                            "doc.title_required",
                            "this.search_metadata.title != ''",
                            "a gated document needs a title"))))
                    : Optional.empty();
        }
    };

    @BeforeAll
    static void bootBothServices() throws Exception {
        DocumentFetcher fetcher = address -> {
            Document document = DOCUMENTS.get(address.getDocId());
            if (document == null) {
                throw Status.NOT_FOUND.withDescription(address.getDocId())
                        .asRuntimeException();
            }
            return document;
        };
        gated = SearchServices.build(
                new SearchServiceConfig(0, work.resolve("gated"), Map.of(
                        "repo-document-lexical", RepoDocumentMapping.served())),
                fetcher,
                ProtoValidator.create(List.of(TITLE_RULE)));
        ungated = SearchServices.build(
                new SearchServiceConfig(0, work.resolve("ungated"), Map.of(
                        "repo-document-lexical", RepoDocumentMapping.served())),
                fetcher);

        String gatedName = InProcessServerBuilder.generateName();
        gated.startInProcess(gatedName);
        gatedChannel = InProcessChannelBuilder.forName(gatedName).build();
        gatedStub = SearchIndexServiceGrpc.newBlockingStub(gatedChannel);

        String ungatedName = InProcessServerBuilder.generateName();
        ungated.startInProcess(ungatedName);
        ungatedChannel = InProcessChannelBuilder.forName(ungatedName).build();
        ungatedStub = SearchIndexServiceGrpc.newBlockingStub(ungatedChannel);
    }

    @AfterAll
    static void shutdown() {
        if (gatedChannel != null) {
            gatedChannel.shutdownNow();
        }
        if (ungatedChannel != null) {
            ungatedChannel.shutdownNow();
        }
        if (gated != null) {
            gated.close();
        }
        if (ungated != null) {
            ungated.close();
        }
    }

    static IndexDocumentRequest index(String docId) {
        return IndexDocumentRequest.newBuilder()
                .setAddress(NodeAddress.newBuilder().setDocId(docId))
                .setMappingSubject("repo-document-lexical")
                .build();
    }

    @Test
    void aCleanDocumentIndexesThroughTheGate() {
        IndexDocumentResponse response = gatedStub.indexDocument(index("titled"));
        assertThat(response.getDocId()).isEqualTo("titled");
    }

    @Test
    void theGateRefusesAViolatingFetchedDocumentByName() {
        assertThatThrownBy(() -> gatedStub.indexDocument(index("untitled")))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode())
                            .isEqualTo(Status.Code.FAILED_PRECONDITION);
                    assertThat(e.getStatus().getDescription())
                            .contains("doc.title_required")
                            .contains("a gated document needs a title");
                });
    }

    @Test
    void withoutAGateTheServiceIndexesEverythingItFetches() {
        IndexDocumentResponse response = ungatedStub.indexDocument(index("untitled"));
        assertThat(response.getDocId()).isEqualTo("untitled");
    }
}
