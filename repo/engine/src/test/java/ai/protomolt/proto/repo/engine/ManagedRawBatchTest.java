package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.codec.PartObject;
import ai.protomolt.proto.repo.v1.*;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/** Parser boundary only; these constructed batches do not stand in for provider integration tests. */
class ManagedRawBatchTest {
    private static PartObject part(byte[] bytes) {
        return new PartObject(DocumentPart.DOCUMENT_PART_BLOBS, "", bytes, DocumentPartCodec.sha256Hex(bytes));
    }
    private static DocumentReadBatch batch(List<PartObject> parts) {
        var batch = new DocumentReadBatch(new PayloadBudget(1024).reserve(0));
        batch.complete(parts);
        return batch;
    }
    @Test void extractsExactBagWithoutStorageAccess() {
        var bag = BlobBag.newBuilder().setBlob(Blob.newBuilder().setSizeBytes(3)).build();
        var bytes = Document.newBuilder().setBlobBag(bag).build().toByteArray();
        try (var batch = batch(List.of(part(bytes)))) {
            assertThat(ManagedRawBindings.readVerifiedBag(batch)).isEqualTo(bag);
        }
    }
    @Test void absenceIsEmptyButDuplicateFragmentsAreRejected() {
        try (var empty = batch(List.of())) {
            assertThat(ManagedRawBindings.readVerifiedBag(empty)).isEqualTo(BlobBag.getDefaultInstance());
        }
        var part = part(Document.getDefaultInstance().toByteArray());
        try (var duplicate = batch(List.of(part, part))) {
            assertThatThrownBy(() -> ManagedRawBindings.readVerifiedBag(duplicate)).hasMessageContaining("duplicate BLOBS");
        }
    }
    @Test void mutatedVerifiedBytesAndMalformedProtobufAreRejected() {
        byte[] bytes = Document.newBuilder().setDocId("before").build().toByteArray();
        try (var batch = batch(List.of(part(bytes)))) {
            bytes[bytes.length - 1] ^= 1;
            assertThatThrownBy(() -> ManagedRawBindings.readVerifiedBag(batch)).hasMessageContaining("changed after verification");
        }
        try (var malformed = batch(List.of(part(new byte[]{(byte) 0x80})))) {
            assertThatThrownBy(() -> ManagedRawBindings.readVerifiedBag(malformed))
                    .hasCauseInstanceOf(com.google.protobuf.InvalidProtocolBufferException.class);
        }
    }
}
