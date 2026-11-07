package ai.protomolt.proto.repo.accession;

import ai.protomolt.proto.repo.stack.Stack;
import ai.protomolt.proto.repo.stack.StackDataLoss;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.StackRef;
import ai.protomolt.proto.repo.v2.StoredPartEntry;
import io.grpc.Context;
import io.grpc.Status;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/** Streams placement changes without decoding content or changing retained provenance. */
final class DocumentPlacement {
    private DocumentPlacement() {}

    /** Returns original references which the commit must fence against concurrent purge. */
    static List<StackRef> materialize(Map<PartSelector, StoredPartEntry> parts, Stack destination,
            Function<String, Stack> resolve, List<StackRef> newObjects) {
        var copied = new LinkedHashMap<StackRef, StackRef>();
        for (var entry : parts.entrySet()) {
            var part = entry.getValue().toBuilder();
            if (part.hasRef()) part.setRef(copy(part.getRef(), destination, resolve, copied, newObjects));
            for (var blob : part.getBlobRefsBuilderList())
                blob.setRef(copy(blob.getRef(), destination, resolve, copied, newObjects));
            entry.setValue(part.build());
        }
        return new ArrayList<>(copied.keySet());
    }

    private static StackRef copy(StackRef source, Stack destination, Function<String, Stack> resolve,
            Map<StackRef, StackRef> copied, List<StackRef> newObjects) {
        if (source.getStackId().equals(destination.stackId())) return source;
        var previous = copied.get(source);
        if (previous != null) return previous;
        final StackRef result;
        try (var input = cancellable(resolve.apply(source.getStackId()).open(source))) {
            result = destination.put(input, source.getLength());
            // Require verified EOF even if a provider consumed only expectedLength bytes.
            if (input.read() != -1) throw new StackDataLoss("Promotion source has unread bytes");
        } catch (StackDataLoss loss) {
            throw new AccessionException(Status.Code.DATA_LOSS, "Promotion source is corrupt or missing", null, loss);
        } catch (InterruptedIOException cancelled) {
            throw new AccessionException(Status.Code.CANCELLED, "Promotion cancelled before commit", null, cancelled);
        } catch (IOException failure) {
            throw new AccessionException(Status.Code.UNAVAILABLE, "Promotion transfer failed", null, failure);
        }
        if (!result.getStackId().equals(destination.stackId()) || result.getLength() != source.getLength()
                || !result.getSha256().equals(source.getSha256()))
            throw new AccessionException(Status.Code.DATA_LOSS, "Promotion destination returned a different object identity", null);
        copied.put(source, result);
        newObjects.add(result);
        return result;
    }

    private static InputStream cancellable(InputStream source) {
        return new FilterInputStream(source) {
            private void check() throws InterruptedIOException {
                if (Context.current().isCancelled() || Thread.currentThread().isInterrupted())
                    throw new InterruptedIOException("Promotion transfer cancelled");
            }
            @Override public int read() throws IOException { check(); return in.read(); }
            @Override public int read(byte[] bytes, int offset, int length) throws IOException {
                check(); return in.read(bytes, offset, length);
            }
        };
    }
}
