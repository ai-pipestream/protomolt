package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.codec.PartObject;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;

/** Recovery-only private input snapshot, retained through synchronous publication. */
final class DocumentRecoveryPayloads implements AutoCloseable {
    private final PayloadBudget.Lease lease;
    private Map<DocumentUploadPayloads.Key,PartObject> bodies;

    private DocumentRecoveryPayloads(PayloadBudget.Lease lease, Map<DocumentUploadPayloads.Key,PartObject> bodies) {
        this.lease=lease;
        this.bodies=Map.copyOf(bodies);
    }

    static DocumentRecoveryPayloads prepare(DocumentPublicationCommand command,
            Map<DocumentUploadPayloads.Key,PartObject> supplied, PayloadBudget budget, RepositoryReadControl control) {
        Objects.requireNonNull(control).check();
        if (supplied.size()>DocumentPublicationCommand.MAX_PARTS) throw new IllegalArgumentException("Too many recovery payloads");
        supplied=Map.copyOf(supplied);
        var expected=new HashSet<DocumentUploadPayloads.Key>();
        long bytes=0;
        for (var member : command.intent().getMembersList()) {
            for (int ordinal=0;ordinal<member.getPartsCount();ordinal++) {
                control.check();
                var part=member.getParts(ordinal);
                if (!part.hasUpload()) continue;
                var key=new DocumentUploadPayloads.Key(member.getMemberId(),ordinal);
                expected.add(key);
                var body=supplied.get(key);
                if (body==null || body.part()!=part.getSlot().getPart()
                        || !Objects.equals(body.subKey(),part.getSlot().getSubKey()) || body.bytes()==null
                        || body.bytes().length!=part.getUpload().getSizeBytes()
                        || !Objects.equals(body.sha256(),part.getUpload().getSha256()))
                    throw new IllegalArgumentException("Recovery requires complete command upload payloads");
                bytes=Math.addExact(bytes,body.bytes().length);
            }
        }
        if (!expected.equals(supplied.keySet())) throw new IllegalArgumentException("Recovery payload keys differ from command");
        var lease=budget.reserve(bytes);
        try {
            var copies=new HashMap<DocumentUploadPayloads.Key,PartObject>();
            for (var key : expected) {
                control.check();
                var body=supplied.get(key);
                byte[] copy=body.bytes().clone();
                if (!DocumentPartCodec.sha256Hex(copy).equals(body.sha256()))
                    throw new IllegalArgumentException("Recovery payload checksum differs from command");
                copies.put(key,new PartObject(body.part(),body.subKey(),copy,body.sha256()));
            }
            control.check();
            return new DocumentRecoveryPayloads(lease,copies);
        } catch (RuntimeException | Error failure) {
            lease.close();
            throw failure;
        }
    }

    /** Trusted publication borrows these arrays; it must not mutate or retain them after return. */
    Map<DocumentUploadPayloads.Key,PartObject> bodies() {
        if (bodies==null) throw new IllegalStateException("Recovery payloads are closed");
        return bodies;
    }

    @Override public void close() {
        bodies=null;
        lease.close();
    }
}
