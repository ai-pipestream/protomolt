package ai.protomolt.proto.repo.accession;

import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.IntentMetadata;
import ai.protomolt.proto.repo.v2.IntentWrite;
import ai.protomolt.proto.repo.v2.OpaqueAttestation;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v2.SaveIntent;
import ai.protomolt.proto.repo.v2.ValidationMode;
import ai.protomolt.proto.repo.v2.WriteAction;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/**
 * The canonical retry identity of a save (requirements §7). The encoding is the
 * binary SaveIntent: protobuf-java writes fields in field-number order;
 * metadata is sorted by unsigned UTF-8 key; an unspecified validation mode is
 * TYPED; writes and sources keep request order; attestations are sorted by
 * their locator's encoding with omitted owners set to the save's step owner.
 * PUTs bind content SHA-256 and length, never a storage key.
 */
final class SaveIntents {
    private SaveIntents() {}

    /** The digest of one encoded fragment, keyed by write index. */
    record Content(String sha256, long length) {}

    static SaveIntent intent(SaveDocumentRequest request, NodeAddress address, Map<Integer, Content> contents) {
        var intent = SaveIntent.newBuilder().setEncodingVersion(1).setAddress(address)
                .setExpected(request.getExpected())
                .setValidationMode(request.getValidationMode() == ValidationMode.VALIDATION_MODE_UNSPECIFIED
                        ? ValidationMode.VALIDATION_MODE_TYPED : request.getValidationMode())
                .addAllSources(request.getSourcesList()).setDrive(request.getDrive()).setOwner(request.getOwner())
                .addAllDerivedFrom(request.getDerivedFromList()).setOperationKind("save");
        for (int index = 0; index < request.getWritesCount(); index++) {
            var write = request.getWrites(index);
            var entry = IntentWrite.newBuilder().setWrite(write);
            if (write.getAction() == WriteAction.WRITE_ACTION_PUT) {
                var content = contents.get(index);
                if (content == null) throw new IllegalStateException("PUT content digest missing");
                entry.setContentSha256(content.sha256()).setContentLength(content.length());
            }
            intent.addWrites(entry);
        }
        var keys = new ArrayList<>(request.getMetadataMap().keySet());
        keys.sort(SaveIntents::compareUtf8);
        for (var key : keys) intent.addMetadata(IntentMetadata.newBuilder().setKey(key).setValue(request.getMetadataOrThrow(key)));
        var attestations = new ArrayList<OpaqueAttestation>();
        for (var attestation : request.getOpaqueAttestationsList()) {
            if (!attestation.hasOwner() && request.getOwner().hasStep())
                attestation = attestation.toBuilder().setOwner(request.getOwner().getStep()).build();
            attestations.add(attestation);
        }
        attestations.sort((left, right) -> Arrays.compareUnsigned(
                left.getOccurrence().toByteArray(), right.getOccurrence().toByteArray()));
        for (int i = 1; i < attestations.size(); i++)
            if (attestations.get(i).getOccurrence().equals(attestations.get(i - 1).getOccurrence()))
                throw new AccessionException(io.grpc.Status.Code.INVALID_ARGUMENT, "Duplicate opaque occurrence locator",
                        DocumentSaves.malformed("Duplicate opaque occurrence locator"));
        intent.addAllOpaqueAttestations(attestations);
        return intent.build();
    }

    static String sha256(SaveIntent intent) { return ai.protomolt.proto.descriptors.MessageFingerprints.fingerprint(intent); }

    static MessageDigest newSha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    static String sha256Hex(byte[] bytes) {
        return HexFormat.of().formatHex(newSha256().digest(bytes));
    }

    /** Unsigned UTF-8 byte order, which equals code point order. */
    static int compareUtf8(String left, String right) {
        int i = 0, j = 0;
        while (i < left.length() && j < right.length()) {
            int x = left.codePointAt(i), y = right.codePointAt(j);
            if (x != y) return Integer.compare(x, y);
            i += Character.charCount(x);
            j += Character.charCount(y);
        }
        return Integer.compare(left.length() - i, right.length() - j);
    }

    static List<String> sortedUtf8(java.util.Collection<String> values) {
        var sorted = new ArrayList<>(values);
        sorted.sort(SaveIntents::compareUtf8);
        return sorted;
    }
}
