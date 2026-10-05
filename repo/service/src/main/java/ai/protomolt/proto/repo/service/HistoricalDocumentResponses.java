package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.HistoricalDocumentRepository;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/** Bounded immutable response snapshots; no repository lookup or authorization policy lives here. */
final class HistoricalDocumentResponses {
    static final int MAX_BYTES = 8 * 1024 * 1024;
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    record Snapshot(ReadRevisionResponse response, PayloadBudget.Lease lease) implements AutoCloseable {
        @Override public void close() { lease.close(); }
    }

    static Snapshot capture(ReadRevisionRequest request, HistoricalDocumentRepository.RevisionRead read,
            PayloadBudget budget, RepositoryReadControl control) {
        control.check();
        if (!request.getAddress().equals(read.address()) || !request.getRevisionId().equals(read.revision().toString()))
            throw invalid("Historical result identity differs from request");
        var response = ReadRevisionResponse.newBuilder().setAddress(read.address()).setRevisionId(read.revision().toString())
                .setMutationRevision(read.publicationRevision()).setManifest(read.manifest());
        long headerSize = response.build().getSerializedSize();
        checkSize(headerSize);
        long size;
        ValidatedHistoricalDocument typed = null;
        if (read instanceof HistoricalDocumentRepository.RawRead raw
                && request.getMode() == HistoricalDocumentReadMode.HISTORICAL_DOCUMENT_READ_MODE_RAW) {
            long rawSize = 0;
            var fragments = raw.fragments();
            if (fragments.size() > 10000 || read.manifest().getPartsCount() > 10000) throw invalid("Historical fragment count exceeds contract");
            int next = 0;
            for (int ordinal = 0; ordinal < read.manifest().getPartsCount(); ordinal++) {
                control.check();
                var entry = read.manifest().getParts(ordinal);
                if (entry.getState() != PartState.PART_STATE_PRESENT) continue;
                if (next == fragments.size() || fragments.get(next).revisionOrdinal() != ordinal)
                    throw invalid("Historical fragments differ from present manifest slots");
                var bytes = fragments.get(next++).bytes();
                int length = bytes.remaining();
                if (entry.getSizeBytes() != length) throw invalid("Historical fragment size differs from manifest");
                var hash = sha256();
                hash.update(bytes);
                if (!HexFormat.of().formatHex(hash.digest()).equals(entry.getSha256()))
                    throw invalid("Historical fragment checksum differs from manifest");
                long fragmentSize = ordinal == 0 ? 0 : CodedOutputStream.computeUInt32Size(1, ordinal);
                if (length != 0) fragmentSize += framed(length);
                rawSize += framed(fragmentSize);
                checkSize(headerSize + framed(rawSize));
            }
            if (next != fragments.size()) throw invalid("Historical result has unexpected fragments");
            size = headerSize + framed(rawSize);
        } else if (read instanceof HistoricalDocumentRepository.ValidatedRead validated
                && request.getMode() == HistoricalDocumentReadMode.HISTORICAL_DOCUMENT_READ_MODE_VALIDATED) {
            if (!validated.document().getDocId().equals(read.address().getDocId())
                    || !validated.document().getOwnership().getAccountId().equals(read.address().getAccountId()))
                throw invalid("Historical document identity differs from captured address");
            typed = ValidatedHistoricalDocument.newBuilder().setDocument(validated.document())
                    .setValidationProfile(validated.validationProfile()).setPolicySha256(validated.policySha256())
                    .setCommandSha256(validated.commandSha256()).build();
            size = headerSize + framed(typed.getSerializedSize());
        } else throw invalid("Historical representation differs from request");
        checkSize(size);
        control.check();
        PayloadBudget.Lease lease;
        try { lease = budget.reserve(Math.multiplyExact(size, 2)); }
        catch (PayloadBudget.CapacityExceededException full) {
            throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED, "Historical response capacity exhausted");
        }
        boolean delivered = false;
        try {
            if (typed != null) response.setValidated(typed);
            else {
                var raw = RawHistoricalDocument.newBuilder();
                for (var fragment : ((HistoricalDocumentRepository.RawRead) read).fragments()) {
                    control.check();
                    raw.addFragments(HistoricalDocumentFragment.newBuilder().setRevisionOrdinal(fragment.revisionOrdinal())
                            .setContent(ByteString.copyFrom(fragment.bytes())));
                }
                response.setRaw(raw);
            }
            var message = response.build();
            if (message.getSerializedSize() != size || !VALIDATOR.validate(message).valid())
                throw invalid("Historical response violates its contract");
            control.check();
            var result = new Snapshot(message, lease);
            delivered = true;
            return result;
        } finally {
            if (!delivered) lease.close();
        }
    }

    // All length-delimited fields in this envelope have one-byte tags.
    private static long framed(long size) {
        checkSize(size);
        return 1L + CodedOutputStream.computeUInt32SizeNoTag((int) size) + size;
    }
    private static void checkSize(long bytes) {
        if (bytes > MAX_BYTES) throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED,
                "Historical response exceeds 8 MiB");
    }
    private static RepositoryException invalid(String message) {
        return new RepositoryException(RepositoryException.Code.DATA_LOSS, message);
    }
    private static MessageDigest sha256() {
        try { return MessageDigest.getInstance("SHA-256"); }
        catch (NoSuchAlgorithmException unavailable) { throw new IllegalStateException("SHA-256 is unavailable", unavailable); }
    }
}
