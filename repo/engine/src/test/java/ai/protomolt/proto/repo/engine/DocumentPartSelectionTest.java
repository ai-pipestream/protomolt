package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.blob.spi.BackendIdentity;
import ai.protomolt.proto.repo.container.ledger.DocumentPublicationLedger;
import ai.protomolt.proto.repo.container.ledger.ManagedBackendLedger;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.v1.DocumentManifest;
import ai.protomolt.proto.repo.v1.DocumentPart;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/** Pure selection tests; no provider or successful publication is simulated. */
class DocumentPartSelectionTest {
    private static DocumentPublicationLedger.BoundPart part(String slot,String namespace) {
        var profile=new ManagedBackendLedger.Profile(new BackendIdentity("test","test/v1",Map.of("location","synthetic")),"realm");
        return new DocumentPublicationLedger.BoundPart(new DocumentPublicationLedger.Part(DocumentPart.DOCUMENT_PART_CHUNKS,
                slot,"same-key",1,"a".repeat(64),"version",null),new DocumentPublicationLedger.Binding("generation",profile,namespace));
    }
    @Test void overlappingKeysRemainDistinctAndSelectionPreservesPublicationOrder() {
        var first=part("first","one"); var second=part("second","two");
        var publication=new DocumentPublicationLedger.Publication(UUID.randomUUID(),DocumentManifest.getDefaultInstance(),List.of(second,first));
        var firstSlot=new DocumentPartReader.PartSlot(DocumentPart.DOCUMENT_PART_CHUNKS,"first");
        var secondSlot=new DocumentPartReader.PartSlot(DocumentPart.DOCUMENT_PART_CHUNKS,"second");
        assertThat(DocumentPartReader.selectSlots(publication,Set.of(firstSlot))).containsExactly(first);
        assertThat(DocumentPartReader.selectSlots(publication,Set.of(firstSlot,secondSlot))).containsExactly(second,first);
        assertThat(DocumentPartReader.selectSlots(publication,Set.of())).isEmpty();
        assertThatThrownBy(() -> DocumentPartReader.selectSlots(publication,Set.of(new DocumentPartReader.PartSlot(DocumentPart.DOCUMENT_PART_CHUNKS,"missing"))))
                .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.FAILED_PRECONDITION));
    }
    @Test void duplicateSlotsCannotMasqueradeAsMissingSelection() {
        assertThatThrownBy(() -> new DocumentPublicationLedger.Publication(UUID.randomUUID(),DocumentManifest.getDefaultInstance(),
                List.of(part("duplicate","one"),part("duplicate","two"))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("slots must be unique");
    }
}
