package ai.protomolt.proto.repo.spi;

import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.ByteString;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/** Validated buffered input shared by local and transport adapters; grants no execution authority. */
public final class DocumentPublicationInput {
    public static final int MAX_ENVELOPE_BYTES=10*1024*1024;
    public static final long MAX_UPLOAD_BYTES=8L*1024*1024;
    public record PayloadKey(String memberId, int revisionOrdinal) {}
    private final DocumentPublicationCommand command;
    private final Map<String,DocumentPublicationMode> modes;
    private final Map<PayloadKey,ByteString> payloads;
    private final long uploadBytes;

    private DocumentPublicationInput(DocumentPublicationCommand command, Map<String,DocumentPublicationMode> modes,
            Map<PayloadKey,ByteString> payloads, long uploadBytes) {
        this.command=command; this.modes=Map.copyOf(modes); this.payloads=Map.copyOf(payloads); this.uploadBytes=uploadBytes;
    }

    /**
     * References immutable protobuf bytes without allocating another payload copy.
     * The host must bound parsing and concurrent retained inputs separately. Call this
     * even for a terminal receipt retry; registry, policy and durable mode checks remain
     * obligations of the authenticated execution facade.
     */
    public static DocumentPublicationInput validate(PublishDocumentRequest request, RepositoryReadControl control) {
        return validate(request,control,(int)MAX_UPLOAD_BYTES);
    }

    /** A host may impose a smaller upload-object limit; protocol-wide bounds still apply. */
    public static DocumentPublicationInput validate(PublishDocumentRequest request, RepositoryReadControl control,
            int maxObjectBytes) {
        if (maxObjectBytes < 1 || maxObjectBytes > MAX_UPLOAD_BYTES)
            throw invalid("Upload object limit must be positive and at most 8 MiB");
        Objects.requireNonNull(request,"request"); Objects.requireNonNull(control,"control").check();
        if (!request.hasIntent() || request.getModesCount()>64 || request.getPayloadsCount()>DocumentPublicationCommand.MAX_PARTS)
            throw invalid("Publication input counts or intent are invalid");
        if (request.getSerializedSize()>MAX_ENVELOPE_BYTES)
            throw invalid("Publication envelope exceeds 10 MiB");
        if (!request.getUnknownFields().asMap().isEmpty()) throw invalid("Unknown publication input fields");
        var command=new DocumentPublicationCommand(request.getIntent());
        var members=new HashMap<String,DocumentPublicationMember>();
        command.intent().getMembersList().forEach(member -> members.put(member.getMemberId(),member));
        var modes=new HashMap<String,DocumentPublicationMode>();
        for (var choice:request.getModesList()) {
            control.check();
            if (!choice.getUnknownFields().asMap().isEmpty() || !members.containsKey(choice.getMemberId())
                    || (choice.getMode()!=DocumentPublicationMode.DOCUMENT_PUBLICATION_MODE_TYPED
                    && choice.getMode()!=DocumentPublicationMode.DOCUMENT_PUBLICATION_MODE_OPAQUE)
                    || modes.putIfAbsent(choice.getMemberId(),choice.getMode())!=null)
                throw invalid("Publication modes must match each member exactly once");
        }
        if (modes.size()!=members.size()) throw invalid("Publication modes are incomplete");
        long total=0;
        var payloads=new HashMap<PayloadKey,ByteString>();
        for (var payload:request.getPayloadsList()) {
            control.check();
            var member=members.get(payload.getMemberId());
            int ordinal=payload.getRevisionOrdinal();
            if (!payload.getUnknownFields().asMap().isEmpty() || member==null || ordinal<0 || ordinal>=member.getPartsCount())
                throw invalid("Publication payload coordinate is invalid");
            var part=member.getParts(ordinal);
            if (!part.hasUpload()) throw invalid("Publication payload does not name an upload");
            var content=payload.getContent();
            if (content.size()>maxObjectBytes) throw invalid("Publication upload exceeds configured object limit");
            if (part.getUpload().getSizeBytes()!=content.size()) throw invalid("Publication upload length differs from intent");
            total=Math.addExact(total,content.size());
            if (total>MAX_UPLOAD_BYTES) throw invalid("Publication uploads exceed 8 MiB");
            if (payloads.putIfAbsent(new PayloadKey(payload.getMemberId(),ordinal),content)!=null)
                throw invalid("Duplicate publication payload");
        }
        int expected=0;
        for (var member:members.values()) for (int ordinal=0;ordinal<member.getPartsCount();ordinal++) {
            control.check();
            var part=member.getParts(ordinal);
            if (!part.hasUpload()) continue;
            expected++;
            var content=payloads.get(new PayloadKey(member.getMemberId(),ordinal));
            if (content==null) throw invalid("Publication upload payload is missing");
        }
        if (payloads.size()!=expected) throw invalid("Publication upload coverage differs from intent");
        // Validate shape and aggregate capacity before checksum work. No body arrays
        // are cloned; read-only buffers cover both flat and segmented ByteStrings.
        for (var entry:payloads.entrySet()) {
            var declaration=members.get(entry.getKey().memberId()).getParts(entry.getKey().revisionOrdinal()).getUpload();
            if (!digest(entry.getValue(),control).equals(declaration.getSha256()))
                throw invalid("Publication upload checksum differs from intent");
        }
        control.check();
        return new DocumentPublicationInput(command,modes,payloads,total);
    }

    private static String digest(ByteString bytes, RepositoryReadControl control) {
        final MessageDigest digest;
        try { digest=MessageDigest.getInstance("SHA-256"); }
        catch (NoSuchAlgorithmException unavailable) { throw new IllegalStateException("SHA-256 unavailable",unavailable); }
        for (var buffer:bytes.asReadOnlyByteBufferList()) {
            while (buffer.hasRemaining()) {
                control.check();
                int count=Math.min(buffer.remaining(),64*1024);
                var chunk=buffer.slice(); chunk.limit(count); digest.update(chunk);
                buffer.position(buffer.position()+count);
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }
    private static IllegalArgumentException invalid(String message) { return new IllegalArgumentException(message); }
    public DocumentPublicationCommand command() { return command; }
    public Map<String,DocumentPublicationMode> modes() { return modes; }
    public Map<PayloadKey,ByteString> payloads() { return payloads; }
    public long uploadBytes() { return uploadBytes; }
    @Override public String toString() { return "DocumentPublicationInput[uploads="+payloads.size()+",bytes="+uploadBytes+"]"; }
}
