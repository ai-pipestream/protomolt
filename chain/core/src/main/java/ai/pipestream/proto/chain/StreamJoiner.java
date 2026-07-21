package ai.pipestream.proto.chain;

import ai.pipestream.proto.cel.CelMappingRule;
import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.grpc.invoke.DynamicGrpcCalls;
import ai.pipestream.proto.grpc.invoke.DynamicGrpcStream;
import ai.pipestream.proto.mapper.MappingException;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import ai.pipestream.proto.shapes.MessageJoiner;
import ai.pipestream.proto.shapes.MessageScope;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.DynamicMessage;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.Metadata;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Joins two live gRPC server streams — the streaming execution story that is ours where
 * topic-to-topic joins are Kafka Streams'. Both sides are flow-controlled
 * {@link DynamicGrpcStream}s, so a fast stream cannot flood a slow one; unmatched entries
 * wait in bounded per-side buffers whose oldest entries (by arrival order) are dropped on
 * overflow — memory is explicit, never unbounded.
 *
 * <p>{@code ZIP} pairs messages by arrival order; {@code KEYED} matches on a key read from
 * each side (a singular scalar field path, validated at construction; both sides must use
 * the same key type). A message with duplicate keys queues FIFO behind its predecessors; a
 * message whose key path cannot be read (an absent intermediate message) cannot match and
 * is dropped. Every match is joined into the target type through the standard scoped rules
 * the moment both halves exist — completed joins wait in their own queue, so a caller
 * taking few results never strands a matched pair in the input buffers. {@link #take} is
 * poll-shaped, like the stream it wraps: block up to the timeout, return up to {@code max}
 * joined messages.</p>
 */
public final class StreamJoiner implements AutoCloseable {

    public enum Mode { ZIP, KEYED }

    /**
     * One side of the join. {@code keyPath} is the field path whose value matches the other
     * side (KEYED mode); {@code name} is the scope name the join rules read it as.
     */
    public record Side(String name, Channel channel, MethodDescriptor method,
                       DynamicMessage request, String keyPath) {

        public Side {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(channel, "channel");
            Objects.requireNonNull(method, "method");
            Objects.requireNonNull(request, "request");
        }
    }

    private static final Duration PULL_SLICE = Duration.ofMillis(25);

    /** An unmatched message plus its arrival bookkeeping (taken = consumed by a match). */
    private static final class Buffered {
        final DynamicMessage message;
        final String key;
        boolean taken;

        Buffered(DynamicMessage message, String key) {
            this.message = message;
            this.key = key;
        }
    }

    private final Mode mode;
    private final Side left;
    private final Side right;
    private final DynamicGrpcStream leftStream;
    private final DynamicGrpcStream rightStream;
    private final int bufferLimit;
    private final Descriptor target;
    private final List<String> rules;
    private final List<CelMappingRule> celRules;
    private final MessageJoiner joiner = new MessageJoiner();
    private final ProtoFieldMapperImpl keys;

    /** Unmatched messages: FIFO in ZIP mode; key-indexed FIFO plus arrival order in KEYED. */
    private final Deque<DynamicMessage> leftPending = new ArrayDeque<>();
    private final Deque<DynamicMessage> rightPending = new ArrayDeque<>();
    private final Map<String, Deque<Buffered>> leftByKey = new LinkedHashMap<>();
    private final Map<String, Deque<Buffered>> rightByKey = new LinkedHashMap<>();
    private final Deque<Buffered> leftArrival = new ArrayDeque<>();
    private final Deque<Buffered> rightArrival = new ArrayDeque<>();
    private int leftBuffered;
    private int rightBuffered;

    /** Completed joins awaiting a {@link #take}; matching never depends on the caller. */
    private final Deque<DynamicMessage> ready = new ArrayDeque<>();

    public StreamJoiner(Mode mode, Side left, Side right, int bufferLimit,
                        Descriptor target, List<String> rules, List<CelMappingRule> celRules) {
        if (bufferLimit <= 0) {
            throw new IllegalArgumentException("bufferLimit must be positive");
        }
        requireServerStreaming(left);
        requireServerStreaming(right);
        if (mode == Mode.KEYED) {
            FieldDescriptor leftKey = validateKeyPath(left);
            FieldDescriptor rightKey = validateKeyPath(right);
            if (leftKey.getJavaType() != rightKey.getJavaType()) {
                throw new IllegalArgumentException("key types differ: " + left.name() + "."
                        + left.keyPath() + " is " + leftKey.getJavaType() + ", " + right.name()
                        + "." + right.keyPath() + " is " + rightKey.getJavaType());
            }
        }
        this.mode = mode;
        this.left = left;
        this.right = right;
        this.bufferLimit = bufferLimit;
        this.target = Objects.requireNonNull(target, "target");
        this.rules = List.copyOf(rules);
        this.celRules = List.copyOf(celRules);
        this.keys = new ProtoFieldMapperImpl(DescriptorRegistry.create());
        this.leftStream = DynamicGrpcCalls.openServerStream(left.channel(), left.method(),
                left.request(), CallOptions.DEFAULT, new Metadata());
        try {
            this.rightStream = DynamicGrpcCalls.openServerStream(right.channel(), right.method(),
                    right.request(), CallOptions.DEFAULT, new Metadata());
        } catch (RuntimeException e) {
            // Nothing will ever call close() on a joiner whose constructor threw, so the
            // half-opened join must hang up on the left server itself.
            this.leftStream.close();
            throw e;
        }
    }

    private static void requireServerStreaming(Side side) {
        if (side.method().isClientStreaming() || !side.method().isServerStreaming()) {
            throw new IllegalArgumentException(side.name() + ": "
                    + side.method().getFullName() + " is not server-streaming");
        }
    }

    /** The key path must resolve to a singular scalar through singular message fields. */
    private static FieldDescriptor validateKeyPath(Side side) {
        if (side.keyPath() == null || side.keyPath().isBlank()) {
            throw new IllegalArgumentException("KEYED joins need a keyPath on both sides");
        }
        Descriptor current = side.method().getOutputType();
        FieldDescriptor field = null;
        for (String segment : side.keyPath().split("\\.")) {
            if (current == null) {
                throw new IllegalArgumentException(side.name() + " keyPath '" + side.keyPath()
                        + "' descends through a non-message field");
            }
            field = current.findFieldByName(segment);
            if (field == null) {
                throw new IllegalArgumentException(side.name() + " keyPath '" + side.keyPath()
                        + "': no field '" + segment + "' on " + current.getFullName());
            }
            if (field.isRepeated()) {
                throw new IllegalArgumentException(side.name() + " keyPath '" + side.keyPath()
                        + "': '" + segment + "' is repeated; keys are singular");
            }
            current = field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
                    ? field.getMessageType() : null;
        }
        if (field == null) {
            // A path of nothing but separators, such as "." — split drops the empty segments.
            throw new IllegalArgumentException(side.name() + " keyPath '" + side.keyPath()
                    + "' names no fields");
        }
        if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
            throw new IllegalArgumentException(side.name() + " keyPath '" + side.keyPath()
                    + "' must end at a scalar field");
        }
        return field;
    }

    /**
     * Takes up to {@code max} joined messages, waiting at most {@code timeout}. Returns
     * fewer (possibly none) on a quiet interval or when both streams have ended; joins
     * completed beyond {@code max} are held for the next call, never lost.
     *
     * @throws io.grpc.StatusRuntimeException once either stream has failed
     * @throws MappingException when a matched pair does not map into the target
     */
    public List<DynamicMessage> take(int max, Duration timeout) throws MappingException {
        if (max <= 0) {
            throw new IllegalArgumentException("max must be positive");
        }
        Objects.requireNonNull(timeout, "timeout");
        List<DynamicMessage> joined = new ArrayList<>();
        long deadline = System.nanoTime() + timeout.toNanos();
        drainReady(joined, max);
        while (joined.size() < max) {
            // Pull whatever both sides have within the remaining time; the per-stream
            // take is the flow-control valve. Each message matches IMMEDIATELY against
            // the other side's buffer (a symmetric hash join) and completed joins park
            // in `ready` - so neither a full output nor eviction can strand a pair.
            long remainingNanos = deadline - System.nanoTime();
            if (remainingNanos <= 0) {
                break;
            }
            // Both sides wait in turn, so each gets half of what is left: a round trip must
            // fit inside the caller's timeout rather than consuming it twice.
            Duration slice = Duration.ofNanos(
                    Math.min(remainingNanos / 2, PULL_SLICE.toNanos()));
            for (DynamicMessage message : leftStream.take(max, slice)) {
                offer(message, true);
            }
            for (DynamicMessage message : rightStream.take(max, slice)) {
                offer(message, false);
            }
            if (mode == Mode.ZIP) {
                zipMatches();
            }
            drainReady(joined, max);
            if (isClosed()) {
                break;
            }
        }
        drainReady(joined, max);
        return joined;
    }

    /** Both streams ended and nothing buffered can still be delivered. */
    public boolean isClosed() {
        return leftStream.isClosed() && rightStream.isClosed() && ready.isEmpty();
    }

    private void drainReady(List<DynamicMessage> out, int max) {
        while (out.size() < max && !ready.isEmpty()) {
            out.add(ready.removeFirst());
        }
    }

    private void offer(DynamicMessage message, boolean isLeft) throws MappingException {
        if (mode == Mode.ZIP) {
            Deque<DynamicMessage> pending = isLeft ? leftPending : rightPending;
            pending.addLast(message);
            if (pending.size() > bufferLimit) {
                pending.removeFirst(); // drop-oldest: memory stays bounded, by policy
            }
            return;
        }
        String key = keyOf(message, isLeft ? left : right);
        if (key == null) {
            return; // no readable key: it can never match, so it never buffers
        }
        Map<String, Deque<Buffered>> other = isLeft ? rightByKey : leftByKey;
        Deque<Buffered> partners = other.get(key);
        if (partners != null && !partners.isEmpty()) {
            Buffered partner = partners.removeFirst();
            partner.taken = true;
            if (partners.isEmpty()) {
                other.remove(key);
            }
            if (isLeft) {
                rightBuffered--;
            } else {
                leftBuffered--;
            }
            ready.addLast(isLeft
                    ? join(message, partner.message)
                    : join(partner.message, message));
            return;
        }
        Buffered entry = new Buffered(message, key);
        Map<String, Deque<Buffered>> index = isLeft ? leftByKey : rightByKey;
        index.computeIfAbsent(key, k -> new ArrayDeque<>()).addLast(entry);
        (isLeft ? leftArrival : rightArrival).addLast(entry);
        if (isLeft) {
            leftBuffered++;
        } else {
            rightBuffered++;
        }
        evictOldest(isLeft);
    }

    /** Drop-oldest by true arrival order; entries consumed by matches are skipped lazily. */
    private void evictOldest(boolean isLeft) {
        Map<String, Deque<Buffered>> index = isLeft ? leftByKey : rightByKey;
        Deque<Buffered> arrival = isLeft ? leftArrival : rightArrival;
        int buffered = isLeft ? leftBuffered : rightBuffered;
        while (buffered > bufferLimit) {
            Buffered oldest = arrival.pollFirst();
            if (oldest == null) {
                break;
            }
            if (oldest.taken) {
                continue;
            }
            // Per-key deques are FIFO of unmatched entries, so the globally oldest
            // unmatched entry is necessarily the head of its key's deque.
            Deque<Buffered> queue = index.get(oldest.key);
            queue.removeFirst();
            if (queue.isEmpty()) {
                index.remove(oldest.key);
            }
            buffered--;
        }
        if (isLeft) {
            leftBuffered = buffered;
        } else {
            rightBuffered = buffered;
        }
    }

    private void zipMatches() throws MappingException {
        while (!leftPending.isEmpty() && !rightPending.isEmpty()) {
            ready.addLast(join(leftPending.removeFirst(), rightPending.removeFirst()));
        }
    }

    private DynamicMessage join(DynamicMessage leftMessage, DynamicMessage rightMessage)
            throws MappingException {
        MessageScope scope = MessageScope.builder()
                .add(left.name(), leftMessage)
                .add(right.name(), rightMessage)
                .build();
        return joiner.join(target, scope, rules, celRules);
    }

    private String keyOf(DynamicMessage message, Side side) throws MappingException {
        Object value = keys.getValue(message, side.keyPath(), true);
        return value == null ? null : String.valueOf(value);
    }

    @Override
    public void close() {
        leftStream.close();
        rightStream.close();
    }
}
