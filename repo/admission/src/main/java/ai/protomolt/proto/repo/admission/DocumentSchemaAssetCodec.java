package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.repo.v1.RepositorySchemaAsset;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Objects;

/** Canonical provenance metadata, not authentication of compiler claims or source retention. */
final class DocumentSchemaAssetCodec {
    static final String CODEC = "repository-schema-asset";
    static final int VERSION = 1;
    static final int MAX_BYTES = 512 * 1024;
    private DocumentSchemaAssetCodec() {}

    record Encoded(ByteString bytes, String sha256) {}

    static Encoded encode(RepositorySchemaAsset asset, Runnable control) {
        requireSize(DocumentSchemaEvidenceCodec.measureAndValidate(asset, control));
        var encoded = DocumentSchemaEvidenceCodec.encode(asset, control);
        return new Encoded(encoded.bytes(), encoded.sha256());
    }

    static DocumentSchemaEvidenceCodec.OwnedEncoded encodeOwned(RepositorySchemaAsset asset,
            DocumentAdmissionReservations reservations, Runnable control) {
        requireSize(DocumentSchemaEvidenceCodec.measureAndValidate(asset, control));
        return DocumentSchemaEvidenceCodec.encodeOwned(asset, reservations, control);
    }

    static RepositorySchemaAsset decode(String codec, int version, ByteString bytes, String sha256,
            DocumentAdmissionReservations reservations, Runnable control) throws InvalidProtocolBufferException {
        Objects.requireNonNull(control, "control").run();
        requireSize(Objects.requireNonNull(bytes, "bytes").size());
        return DocumentSchemaEvidenceCodec.decode(CODEC, codec, version, bytes, sha256,
                RepositorySchemaAsset.getDescriptor(), RepositorySchemaAsset.parser(), reservations, control);
    }

    static RepositorySchemaAsset decode(String codec, int version, ByteString bytes, String sha256,
            Runnable control) throws InvalidProtocolBufferException {
        Objects.requireNonNull(control, "control").run();
        requireSize(Objects.requireNonNull(bytes, "bytes").size());
        return DocumentSchemaEvidenceCodec.decode(CODEC, codec, version, bytes, sha256,
                RepositorySchemaAsset.getDescriptor(), RepositorySchemaAsset.parser(), control);
    }

    private static void requireSize(int size) {
        if (size < 1 || size > MAX_BYTES) throw new IllegalArgumentException("schema asset metadata byte bound exceeded");
    }
}
