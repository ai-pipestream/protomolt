package ai.protomolt.proto.repo.codec;

import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentPart;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;

/**
 * Bounded structural assembly of PRESENT fragments in manifest order. This does
 * not authorize, verify provider receipts, validate annotations/Any payloads, or
 * publish. The repository must retain layout/descriptor identity and separately
 * check the assembled identity/ownership against its canonical command.
 */
public final class DocumentRevisionAssembly {
    private DocumentRevisionAssembly() {}

    /** Raw byte/element limits, not a heap bound; the host supplies policy and memory reservation. */
    public record Limits(long maxBytes, int maxFragments, int maxDepth, long maxChunkElements) {
        public Limits {
            if (maxBytes < 1 || maxFragments < 1 || maxFragments > 10_000
                    || maxDepth < 1 || maxDepth > 100 || maxChunkElements < 1)
                throw new IllegalArgumentException("Invalid document assembly limits");
        }
    }

    /** Exact immutable provider bytes; no normalization or invented empty object. */
    public record Fragment(DocumentPart part, String subKey, ByteString bytes) {
        public Fragment { Objects.requireNonNull(part); Objects.requireNonNull(subKey); Objects.requireNonNull(bytes); }
    }

    /** Original bytes remain distinct from the parsed, merged semantic view. */
    public record Result(Document document, List<Fragment> fragments) {
        public Result { Objects.requireNonNull(document); fragments = List.copyOf(fragments); }
    }

    private record Slot(DocumentPart part, String subKey) {}

    public static Result assemble(List<Fragment> fragments, String expectedDocId, Limits limits, Runnable control)
            throws InvalidProtocolBufferException {
        Objects.requireNonNull(fragments); Objects.requireNonNull(limits); Objects.requireNonNull(control);
        check(control);
        if (expectedDocId == null || expectedDocId.isBlank()) throw new IllegalArgumentException("Document identity required");
        if (fragments.isEmpty() || fragments.size() > limits.maxFragments())
            throw new IllegalArgumentException("Document fragment count exceeds bounds");
        var original = List.copyOf(fragments);
        long bytes = 0;
        int cores = 0;
        var slots = new HashSet<Slot>();
        for (var fragment : original) {
            check(control);
            if (fragment.bytes().size() > limits.maxBytes() - bytes)
                throw new IllegalArgumentException("Document assembly exceeds aggregate byte bound");
            bytes += fragment.bytes().size();
            if (fragment.part() == DocumentPart.DOCUMENT_PART_CORE) cores++;
            if (fragment.part() == DocumentPart.DOCUMENT_PART_CHUNKS ? fragment.subKey().isBlank() : !fragment.subKey().isEmpty())
                throw new IllegalArgumentException("Part subkey differs from slot kind");
            if (!slots.add(new Slot(fragment.part(), fragment.subKey())))
                throw new IllegalArgumentException("Duplicate document slot");
        }
        if (cores != 1) throw new IllegalArgumentException("Exactly one CORE fragment required");
        var chunks = new DocumentChunkSequence(expectedDocId, limits.maxChunkElements());
        var assembled = Document.newBuilder();
        for (var fragment : original) {
            check(control);
            var input = fragment.bytes().newCodedInput();
            input.setRecursionLimit(limits.maxDepth());
            var parsed = Document.parser().parseFrom(input);
            input.checkLastTagWas(0);
            if (input.getTotalBytesRead() != fragment.bytes().size())
                throw new InvalidProtocolBufferException("Fragment was not completely consumed");
            DocumentFragmentConfinement.requireConfined(parsed, fragment.part(), expectedDocId);
            if (fragment.part() == DocumentPart.DOCUMENT_PART_CHUNKS) chunks.accept(fragment.subKey(), parsed);
            assembled.mergeFrom(parsed);
            check(control);
        }
        var document = assembled.build();
        check(control);
        return new Result(document, original);
    }

    private static void check(Runnable control) {
        if (Thread.currentThread().isInterrupted()) throw new CancellationException("Document assembly interrupted");
        control.run();
    }
}
