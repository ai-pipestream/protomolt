package ai.protomolt.proto.delegation.repository;

import ai.protomolt.proto.delegation.storage.v1.EncryptedRepositoryState;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.DocumentTarget;
import ai.protomolt.proto.repo.v2.ExpectedRevision;
import ai.protomolt.proto.repo.v2.GetDocumentRequest;
import ai.protomolt.proto.repo.v2.GetDocumentResponse;
import ai.protomolt.proto.repo.v2.IntakeOrigin;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.PartSelection;
import ai.protomolt.proto.repo.v2.PartState;
import ai.protomolt.proto.repo.v2.PartWrite;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v2.SaveDocumentResponse;
import ai.protomolt.proto.repo.v2.SaveOwner;
import ai.protomolt.proto.repo.v2.WriteAction;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import java.nio.charset.StandardCharsets;

/**
 * The v2 DocumentService mapping of one encrypted repository-state object (issue #320,
 * decision 26). The legacy blob coordinates become the document identity the same way
 * an intake save derives it: the drive is the account, the object key is the document
 * and the intake datasource, and the graph is the account's intake graph. The envelope
 * travels as the CORE part's {@code structured_data} Any, written by one PUT with
 * {@link ExpectedRevision} compare-and-set: {@code none} while no write is confirmed,
 * then the confirmed revision. The CAS token is the revision, never a storage key.
 */
public final class RepositoryStateDocuments {

    /** The only type URL a state document's structured_data may carry. */
    public static final String ENVELOPE_TYPE_URL =
            "type.googleapis.com/" + EncryptedRepositoryState.getDescriptor().getFullName();

    private RepositoryStateDocuments() {}

    /** The document address of one state object: account = drive, document = object key. */
    public static NodeAddress address(String driveName, String objectKey) {
        return NodeAddress.newBuilder().setDocId(objectKey).setGraphAddressId(objectKey)
                .setAccountId(driveName).setGraphId("intake:" + driveName).build();
    }

    /** Reads the current state document, CORE part only. */
    public static GetDocumentRequest get(String driveName, String objectKey) {
        return GetDocumentRequest.newBuilder().setAddress(address(driveName, objectKey))
                .setSelection(PartSelection.newBuilder().addSelectors(PartSelector.newBuilder()
                        .setPart(DocumentPart.DOCUMENT_PART_CORE)))
                .build();
    }

    /**
     * The save that stores one encrypted envelope at {@code expectedRevision}
     * (0 = the document must not exist yet). The operation ID is deterministic over
     * the coordinates, the expected revision and the exact ciphertext, so a re-issued
     * identical write is an idempotent replay and no two logical writes share an ID.
     */
    public static SaveDocumentRequest save(String driveName, String objectKey,
            EncryptedRepositoryState envelope, long expectedRevision) {
        var address = address(driveName, objectKey);
        var document = Document.newBuilder().setDocId(objectKey)
                .setOwnership(OwnershipContext.newBuilder().setAccountId(driveName).setDatasourceId(objectKey))
                .setStructuredData(Any.pack(envelope)).build();
        var expected = ExpectedRevision.newBuilder();
        if (expectedRevision == 0) expected.setNone(true);
        else expected.setRevision(expectedRevision);
        var stored = envelope.toByteArray();
        return SaveDocumentRequest.newBuilder()
                .setOperationId(operationId(driveName, objectKey, expectedRevision, stored))
                .setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(expected)
                .setDocument(document)
                .addWrites(PartWrite.newBuilder().setSelector(PartSelector.newBuilder()
                        .setPart(DocumentPart.DOCUMENT_PART_CORE)).setAction(WriteAction.WRITE_ACTION_PUT))
                .setDrive(driveName)
                .setOwner(SaveOwner.newBuilder().setIntake(
                        IntakeOrigin.newBuilder().setDatasourceId(objectKey)))
                .build();
    }

    /** The encrypted envelope a state document carries; refuses another type URL. */
    public static EncryptedRepositoryState envelopeOf(Document document, String what) {
        var data = document.getStructuredData();
        if (!ENVELOPE_TYPE_URL.equals(data.getTypeUrl()))
            throw new IllegalStateException("stored " + what + " has an unexpected media type");
        try {
            var envelope = data.unpack(EncryptedRepositoryState.class);
            rejectUnknownFields(envelope, what);
            return envelope;
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalStateException("stored " + what + " envelope is not valid protobuf", failure);
        }
    }

    private static void rejectUnknownFields(com.google.protobuf.Message message, String what) {
        if (!message.getUnknownFields().asMap().isEmpty())
            throw new IllegalStateException("stored " + what + " envelope carries unknown fields");
        for (var field : message.getAllFields().entrySet()) {
            if (field.getKey().getJavaType() != com.google.protobuf.Descriptors.FieldDescriptor.JavaType.MESSAGE)
                continue;
            if (field.getKey().isRepeated()) {
                for (var value : (java.util.List<?>) field.getValue())
                    rejectUnknownFields((com.google.protobuf.Message) value, what);
            } else {
                rejectUnknownFields((com.google.protobuf.Message) field.getValue(), what);
            }
        }
    }

    /**
     * Confirms a save: the response answers this exact request, is no replay of an
     * earlier operation, and lands the revision right after the expected one.
     *
     * @return the confirmed revision
     */
    public static long confirmSaved(SaveDocumentRequest request, SaveDocumentResponse response, String what) {
        var accession = response.getAccession();
        if (!response.getOperationId().equals(request.getOperationId()) || response.getReplayed()
                || accession.getTombstone() || !accession.getAddress().equals(request.getTarget().getAddress())
                || accession.getRevision() != expectedRevisionOf(request) + 1)
            throw new IllegalStateException("repository service did not confirm the persisted " + what);
        return accession.getRevision();
    }

    /**
     * Confirms a read: the manifest answers this exact address, pins a live revision,
     * the CORE part is present, and the document carries the mapped coordinates.
     *
     * @return the pinned revision
     */
    public static long confirmLoaded(GetDocumentRequest request, GetDocumentResponse response, String what) {
        var manifest = response.getManifest();
        if (!manifest.getAddress().equals(request.getAddress()) || manifest.getRevision() < 1
                || manifest.getTombstone() || manifest.getPartsList().stream().noneMatch(part ->
                        part.getSelector().getPart() == DocumentPart.DOCUMENT_PART_CORE
                                && part.getState() == PartState.PART_STATE_PRESENT))
            throw new IllegalStateException("repository service did not confirm the stored " + what);
        var document = response.getDocument();
        if (!document.getDocId().equals(request.getAddress().getDocId())
                || !document.getOwnership().getAccountId().equals(request.getAddress().getAccountId())
                || !document.getOwnership().getDatasourceId().equals(request.getAddress().getGraphAddressId()))
            throw new IllegalStateException("repository service did not confirm the stored " + what);
        return manifest.getRevision();
    }

    private static long expectedRevisionOf(SaveDocumentRequest request) {
        return request.getExpected().hasNone() ? 0 : request.getExpected().getRevision();
    }

    private static String operationId(String driveName, String objectKey, long expectedRevision, byte[] stored) {
        var prefix = (driveName + "\n" + objectKey + "\n" + expectedRevision + "\n")
                .getBytes(StandardCharsets.UTF_8);
        var bytes = new byte[prefix.length + stored.length];
        System.arraycopy(prefix, 0, bytes, 0, prefix.length);
        System.arraycopy(stored, 0, bytes, prefix.length, stored.length);
        return "repo-state-" + EncryptedRepositoryStateCodec.sha256Hex(bytes);
    }
}
