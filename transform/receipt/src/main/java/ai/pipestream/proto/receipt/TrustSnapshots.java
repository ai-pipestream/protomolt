package ai.pipestream.proto.receipt;

import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Trust-snapshot preconditions shared by everything that accepts one: the
 * snapshot's own declared rules must hold and its issuer and key names
 * must be distinct. A snapshot failing here is the holder's error, never
 * the record's, and refuses loudly naming the duplicate.
 */
public final class TrustSnapshots {

    private TrustSnapshots() {
    }

    /** Refuses an invalid or ambiguous snapshot; returns it for chaining. */
    public static TrustSnapshot requireWellFormed(TrustSnapshot trust) {
        if (trust == null) {
            throw new IllegalArgumentException("trust snapshot must not be null");
        }
        ValidationResult rules = ProtoValidator.create().validate(trust);
        if (!rules.valid()) {
            throw new IllegalArgumentException("trust snapshot is invalid: "
                    + rules.violations().stream()
                            .map(violation -> violation.path() + ": " + violation.message())
                            .collect(Collectors.joining("; ")));
        }
        Set<String> issuers = new HashSet<>();
        for (TrustedIssuer issuer : trust.getIssuersList()) {
            if (!issuers.add(issuer.getIssuer())) {
                throw new IllegalArgumentException(
                        "trust snapshot duplicates issuer '" + issuer.getIssuer() + "'");
            }
            Set<String> keyIds = new HashSet<>();
            for (TrustedKey key : issuer.getKeysList()) {
                if (!keyIds.add(key.getKeyId())) {
                    throw new IllegalArgumentException("trust snapshot duplicates key '"
                            + key.getKeyId() + "' under issuer '" + issuer.getIssuer() + "'");
                }
            }
        }
        return trust;
    }

    /**
     * Loads and verifies a pinned snapshot file — the relying party's custody model. The
     * extension decides the format: {@code .json} parses as canonical proto3 JSON refusing
     * unknown fields, {@code .binpb} or {@code .pb} parses as the binary message, anything
     * else is refused naming the accepted forms.
     */
    public static TrustSnapshot load(Path file) throws IOException {
        String name = file.getFileName().toString();
        if (name.endsWith(".json")) {
            TrustSnapshot.Builder builder = TrustSnapshot.newBuilder();
            try {
                JsonFormat.parser().merge(Files.readString(file), builder);
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalArgumentException(
                        "trust snapshot file '" + name + "' does not parse: "
                                + e.getMessage(), e);
            }
            return requireWellFormed(builder.build());
        }
        if (name.endsWith(".binpb") || name.endsWith(".pb")) {
            try {
                return requireWellFormed(TrustSnapshot.parseFrom(Files.readAllBytes(file)));
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalArgumentException(
                        "trust snapshot file '" + name + "' does not parse: "
                                + e.getMessage(), e);
            }
        }
        throw new IllegalArgumentException("trust snapshot file '" + name
                + "' must end in .json, .binpb, or .pb");
    }
}
