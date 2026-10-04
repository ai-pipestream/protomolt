package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.repo.v1.RepositorySchemaOccurrencePath;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/** Single relative path codec. Does not prove candidate identity, access or retention. */
final class DocumentSchemaOccurrenceCodec {
    static final String CODEC = "repository-schema-occurrence";
    static final int VERSION = 1;
    static final int MAX_BYTES = DocumentSchemaEvidenceCodec.MAX_BYTES;
    static final int MAX_WIRE_VALUES = DocumentSchemaEvidenceCodec.MAX_WIRE_VALUES;
    static final int MAX_DEPTH = DocumentSchemaEvidenceCodec.MAX_DEPTH;
    private DocumentSchemaOccurrenceCodec() {}

    record Encoded(ByteString bytes, String sha256) {}

    static Encoded encode(RepositorySchemaOccurrencePath path, Runnable control) {
        var encoded = DocumentSchemaEvidenceCodec.encode(path, control);
        return new Encoded(encoded.bytes(), encoded.sha256());
    }

    static RepositorySchemaOccurrencePath decode(String codec, int version, ByteString bytes, String sha256,
            Runnable control) throws InvalidProtocolBufferException {
        return DocumentSchemaEvidenceCodec.decode(CODEC, codec, version, bytes, sha256,
                RepositorySchemaOccurrencePath.getDescriptor(), RepositorySchemaOccurrencePath.parser(), control);
    }
}
