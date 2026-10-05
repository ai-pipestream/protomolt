package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.BackendIdentity;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.v1.DocumentPublicationIntent;
import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/** Bounded private persistence encoding. Decoding grants no permission or execution authority. */
final class DocumentPublicationPreparationCodec {
    static final int MAX_BYTES = 16 * 1024 * 1024;
    private static final int MAX_TEXT_BYTES = 1024 * 1024;
    private static final int MAX_ENTRIES = 10_000;
    private static final int MAGIC = 0x504d5050;
    private static final int VERSION = 1;
    private DocumentPublicationPreparationCodec() {}

    static ByteString encode(DocumentPublicationPreparationRecord value) {
        Objects.requireNonNull(value);
        var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(new BoundedOutput(bytes))) {
            out.writeInt(MAGIC); out.writeInt(VERSION);
            text(out, value.key().account()); text(out, value.key().principal()); uuid(out, value.key().operationId());
            text(out, DocumentPublicationCommand.CODEC); out.writeInt(DocumentPublicationCommand.ENCODING_VERSION);
            text(out, value.command().sha256());
            // The full normalized intent includes operation ID. Canonical command bytes do not.
            block(out, intentBytes(value.command()), DocumentPublicationCommand.MAX_COMMAND_BYTES);
            out.writeLong(value.lease().getSeconds()); out.writeInt(value.lease().getNano());
            out.writeLong(value.predecessorGeneration()); uuid(out, value.seeds().ownerNonce());
            identities(out, value.seeds().attempts()); identities(out, value.seeds().uploadTokens());
            count(out, value.placements().size());
            for (var entry : value.placements().entrySet().stream().sorted(Map.Entry.comparingByKey()).toList()) {
                uuid(out, entry.getKey()); placement(out, entry.getValue());
            }
        } catch (IOException failure) {
            throw new IllegalArgumentException("Cannot encode private publication preparation", failure);
        }
        return ByteString.copyFrom(bytes.toByteArray());
    }

    private static byte[] intentBytes(DocumentPublicationCommand command) throws IOException {
        var bytes = new byte[command.intent().getSerializedSize()];
        var output = com.google.protobuf.CodedOutputStream.newInstance(bytes);
        output.useDeterministicSerialization(); command.intent().writeTo(output); output.checkNoSpaceLeft();
        return bytes;
    }

    /** Expected row bindings prevent a well-formed adjacent operation's record from being substituted. */
    static DocumentPublicationPreparationRecord decode(ByteString bytes, RepositoryOperationLedger.Key expectedKey, String expectedDigest) {
        Objects.requireNonNull(bytes); Objects.requireNonNull(expectedKey); Objects.requireNonNull(expectedDigest);
        if (bytes.isEmpty() || bytes.size() > MAX_BYTES) throw new IllegalArgumentException("Invalid preparation size");
        try (var in = new DataInputStream(bytes.newInput())) {
            if (in.readInt() != MAGIC || in.readInt() != VERSION) throw new IllegalArgumentException("Unsupported preparation encoding");
            var key = new RepositoryOperationLedger.Key(text(in), text(in), uuid(in));
            if (!key.equals(expectedKey)) throw new IllegalArgumentException("Preparation differs from expected scope");
            if (!DocumentPublicationCommand.CODEC.equals(text(in)) || in.readInt() != DocumentPublicationCommand.ENCODING_VERSION)
                throw new IllegalArgumentException("Unsupported preparation command codec");
            String digest = text(in);
            var command = new DocumentPublicationCommand(DocumentPublicationIntent.parseFrom(block(in, DocumentPublicationCommand.MAX_COMMAND_BYTES)));
            if (!command.sha256().equals(digest) || !command.sha256().equals(expectedDigest))
                throw new IllegalArgumentException("Preparation differs from expected command");
            long seconds = in.readLong(); int nanos = in.readInt();
            if (nanos < 0 || nanos > 999_999_999) throw new IllegalArgumentException("Invalid lease nanoseconds");
            var lease = Duration.ofSeconds(seconds, nanos);
            long predecessor = in.readLong(); var owner = uuid(in);
            var attempts = identities(in); var tokens = identities(in);
            var seeds = DocumentPublicationSeeds.restore(key, command, owner, attempts, tokens);
            int size = count(in); var placements = new HashMap<UUID, DocumentUploadPlan.Placement>();
            for (int i = 0; i < size; i++) {
                var id = uuid(in);
                if (placements.put(id, placement(in)) != null) throw new IllegalArgumentException("Duplicate preparation placement");
            }
            if (in.read() != -1) throw new IllegalArgumentException("Trailing preparation bytes");
            var result = new DocumentPublicationPreparationRecord(key, command, seeds, placements, lease, predecessor);
            // Includes deterministic protobuf map ordering, normalized UUIDs and sorted collections.
            if (!encode(result).equals(bytes)) throw new IllegalArgumentException("Noncanonical preparation encoding");
            return result;
        } catch (IOException | NullPointerException failure) {
            throw new IllegalArgumentException("Invalid private publication preparation", failure);
        }
    }

    private static void placement(DataOutputStream out, DocumentUploadPlan.Placement placement) throws IOException {
        var drive = placement.drive();
        uuid(out, drive.id());
        for (var field : new String[]{drive.account(), drive.name(), drive.type(), drive.provider(), drive.namespace(), drive.prefix(),
                drive.region(), drive.credentials(), drive.metadata(), drive.config(), drive.status()}) text(out, field);
        text(out, placement.generation());
        var profile = placement.profile();
        text(out, profile.identity().provider()); text(out, profile.identity().schema()); text(out, profile.storageRealm());
        count(out, profile.identity().location().size());
        for (var entry : new java.util.TreeMap<>(profile.identity().location()).entrySet()) {
            text(out, entry.getKey()); text(out, entry.getValue());
        }
    }

    private static DocumentUploadPlan.Placement placement(DataInputStream in) throws IOException {
        var drive = new DocumentDriveSnapshot(uuid(in), text(in), text(in), text(in), text(in), text(in), text(in),
                text(in), text(in), text(in), text(in), text(in));
        String generation = text(in); String provider = text(in); String schema = text(in); String realm = text(in);
        int size = count(in);
        if (size > 32) throw new IllegalArgumentException("Oversized backend identity");
        var location = new HashMap<String, String>();
        for (int i = 0; i < size; i++) {
            String key = Objects.requireNonNull(text(in)); String value = Objects.requireNonNull(text(in));
            if (location.put(key, value) != null) throw new IllegalArgumentException("Duplicate backend location field");
        }
        return new DocumentUploadPlan.Placement(drive, generation, new ManagedBackendLedger.Profile(
                new BackendIdentity(provider, schema, location), realm));
    }

    private static void identities(DataOutputStream out, Map<String, UUID> values) throws IOException {
        count(out, values.size());
        for (var entry : new java.util.TreeMap<>(values).entrySet()) { text(out, entry.getKey()); uuid(out, entry.getValue()); }
    }
    private static Map<String, UUID> identities(DataInputStream in) throws IOException {
        int size = count(in); var values = new HashMap<String, UUID>();
        for (int i = 0; i < size; i++) {
            String key = Objects.requireNonNull(text(in));
            if (values.put(key, uuid(in)) != null) throw new IllegalArgumentException("Duplicate publication identity");
        }
        return values;
    }
    private static void text(DataOutputStream out, String value) throws IOException {
        if (value == null) { out.writeInt(-1); return; }
        if (value.length() > MAX_TEXT_BYTES) throw new IllegalArgumentException("Preparation text exceeds bound");
        var encoded = StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).encode(CharBuffer.wrap(value));
        if (encoded.remaining() > MAX_TEXT_BYTES) throw new IllegalArgumentException("Preparation text exceeds bound");
        var bytes = new byte[encoded.remaining()]; encoded.get(bytes); block(out, bytes, MAX_TEXT_BYTES);
    }
    private static String text(DataInputStream in) throws IOException {
        int size = in.readInt();
        if (size == -1) return null;
        byte[] bytes = block(in, size, MAX_TEXT_BYTES);
        return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
    }
    private static void block(DataOutputStream out, byte[] bytes, int limit) throws IOException {
        if (bytes.length > limit) throw new IllegalArgumentException("Preparation field exceeds bound");
        out.writeInt(bytes.length); out.write(bytes);
    }
    private static byte[] block(DataInputStream in, int limit) throws IOException { return block(in, in.readInt(), limit); }
    private static byte[] block(DataInputStream in, int size, int limit) throws IOException {
        if (size < 0 || size > limit || size > in.available()) throw new IllegalArgumentException("Invalid preparation field length");
        var bytes = new byte[size]; in.readFully(bytes); return bytes;
    }
    private static void count(DataOutputStream out, int count) throws IOException {
        if (count < 0 || count > MAX_ENTRIES) throw new IllegalArgumentException("Preparation collection exceeds bound");
        out.writeInt(count);
    }
    private static int count(DataInputStream in) throws IOException {
        int count = in.readInt();
        if (count < 0 || count > MAX_ENTRIES) throw new IllegalArgumentException("Invalid preparation collection length");
        return count;
    }
    private static void uuid(DataOutputStream out, UUID value) throws IOException {
        out.writeLong(value.getMostSignificantBits()); out.writeLong(value.getLeastSignificantBits());
    }
    private static UUID uuid(DataInputStream in) throws IOException { return new UUID(in.readLong(), in.readLong()); }

    private static final class BoundedOutput extends OutputStream {
        private final ByteArrayOutputStream bytes;
        BoundedOutput(ByteArrayOutputStream bytes) { this.bytes = bytes; }
        private void require(int length) {
            if (length < 0 || length > MAX_BYTES - bytes.size()) throw new IllegalArgumentException("Preparation exceeds total bound");
        }
        @Override public void write(int value) { require(1); bytes.write(value); }
        @Override public void write(byte[] value, int offset, int length) { require(length); bytes.write(value, offset, length); }
    }
}
