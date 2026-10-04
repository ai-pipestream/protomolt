package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.repo.v1.DocumentSchemaRootLocator;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/** Exact root locator encoding, separate from revision authorization and durable retention. */
final class DocumentSchemaRootCodec {
    static final String CODEC = "document-schema-root";
    static final int VERSION = 1;
    static final int MAX_BYTES = DocumentSchemaEvidenceCodec.MAX_BYTES;
    private DocumentSchemaRootCodec() {}

    record Encoded(ByteString bytes, String sha256) {}

    static Encoded encode(DocumentSchemaRootLocator root, Runnable control) {
        var encoded = DocumentSchemaEvidenceCodec.encode(root, control);
        return new Encoded(encoded.bytes(), encoded.sha256());
    }

    static DocumentSchemaRootLocator decode(String codec, int version, ByteString bytes, String sha256,
            Runnable control) throws InvalidProtocolBufferException {
        return DocumentSchemaEvidenceCodec.decode(CODEC, codec, version, bytes, sha256,
                DocumentSchemaRootLocator.getDescriptor(), DocumentSchemaRootLocator.parser(), control);
    }
}
