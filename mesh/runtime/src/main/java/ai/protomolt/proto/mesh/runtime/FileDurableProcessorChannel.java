package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.descriptors.DescriptorIdentity;
import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mesh.runtime.v1.ChannelRecord;
import ai.protomolt.proto.mesh.runtime.v1.DeliveryClaim;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorCompletion;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorContract;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorFailure;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorWork;
import ai.protomolt.proto.mesh.runtime.v1.WorkClaimed;
import ai.protomolt.proto.mesh.runtime.v1.WorkCompleted;
import ai.protomolt.proto.mesh.runtime.v1.WorkEnqueued;
import ai.protomolt.proto.mesh.runtime.v1.WorkFailed;
import ai.protomolt.proto.mesh.runtime.v1.WorkReleased;
import ai.protomolt.proto.mesh.runtime.v1.WorkSettled;
import ai.protomolt.proto.mesh.runtime.v1.TypedPayload;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32C;

/**
 * File-backed protobuf processor channel.
 *
 * <p>The WAL starts with an eight-byte format marker. Each record is a big-endian length,
 * exact {@link ChannelRecord} bytes, and CRC32C. Every append is forced before in-memory state
 * advances. Recovery truncates only an incomplete final frame left by a torn append; a complete
 * frame with a bad checksum or an invalid transition is refused.</p>
 */
public final class FileDurableProcessorChannel implements DurableProcessorChannel {

    private static final byte[] MAGIC = {'P', 'M', 'C', 'H', '0', '0', '0', '1'};
    private static final int MAX_RECORD_BYTES = 64 * 1024 * 1024;

    private final Path path;
    private final DescriptorRegistry descriptors;
    private final Clock clock;
    private final FileChannel file;
    private final FileLock lock;
    private final Map<String, WorkState> states = new LinkedHashMap<>();
    private final List<ChannelRecord> log = new ArrayList<>();
    private boolean closed;

    public FileDurableProcessorChannel(Path path, DescriptorRegistry descriptors) {
        this(path, descriptors, Clock.systemUTC());
    }

    public FileDurableProcessorChannel(
            Path path, DescriptorRegistry descriptors, Clock clock) {
        this.path = Objects.requireNonNull(path, "path").toAbsolutePath().normalize();
        this.descriptors = Objects.requireNonNull(descriptors, "descriptors");
        this.clock = Objects.requireNonNull(clock, "clock");
        FileChannel opened = null;
        FileLock acquired = null;
        try {
            Path parent = this.path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            opened = FileChannel.open(this.path,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            try {
                acquired = opened.tryLock();
            } catch (OverlappingFileLockException e) {
                throw new IllegalStateException(
                        "processor channel is already open: " + this.path, e);
            }
            if (acquired == null) {
                throw new IllegalStateException(
                        "processor channel is locked by another process: " + this.path);
            }
            file = opened;
            lock = acquired;
            initializeAndReplay();
        } catch (IOException e) {
            closeAfterFailedOpen(acquired, opened);
            throw new IllegalStateException(
                    "cannot open processor channel " + this.path, e);
        } catch (RuntimeException e) {
            closeAfterFailedOpen(acquired, opened);
            throw e;
        }
    }

    @Override
    public synchronized DeliveryView enqueue(ProcessorWork work) {
        requireOpen();
        RemoteValidation.work(work, descriptors);
        if (!RemoteValidation.instant(work.getDeadline()).isAfter(clock.instant())) {
            throw new IllegalArgumentException(
                    "processor work deadline must be in the future");
        }
        WorkState existing = states.get(work.getDeliveryId());
        if (existing != null) {
            if (!existing.work.equals(work)) {
                throw new IllegalArgumentException("conflicting work already uses delivery_id "
                        + work.getDeliveryId());
            }
            return existing.view();
        }
        append(ChannelRecord.newBuilder()
                .setEnqueued(WorkEnqueued.newBuilder().setWork(work)));
        return states.get(work.getDeliveryId()).view();
    }

    @Override
    public synchronized List<DeliveryClaim> claim(
            String workerId,
            Collection<ProcessorContract> contracts,
            int permits,
            Duration leaseDuration,
            Instant now) {
        requireOpen();
        RemoteValidation.workerId(workerId);
        Objects.requireNonNull(contracts, "contracts");
        contracts.forEach(contract -> RemoteValidation.contract(contract, descriptors));
        if (permits < 1 || permits > 100_000) {
            throw new IllegalArgumentException("permits must be between 1 and 100000");
        }
        Objects.requireNonNull(leaseDuration, "leaseDuration");
        if (leaseDuration.isZero() || leaseDuration.isNegative()) {
            throw new IllegalArgumentException("leaseDuration must be positive");
        }
        Objects.requireNonNull(now, "now");
        expire(now);
        List<DeliveryClaim> claimed = new ArrayList<>();
        for (WorkState state : states.values()) {
            if (claimed.size() == permits) {
                break;
            }
            if (state.status != DeliveryState.PENDING
                    || !RemoteValidation.supports(contracts, state.work.getContract())) {
                continue;
            }
            if (state.attempt >= state.work.getMaxAttempts()) {
                appendFailed(state, "attempts-exhausted",
                        "delivery exhausted max_attempts before another claim", "");
                continue;
            }
            Instant workDeadline = RemoteValidation.instant(state.work.getDeadline());
            Instant leaseExpiry = now.plus(leaseDuration);
            if (workDeadline.isBefore(leaseExpiry)) {
                leaseExpiry = workDeadline;
            }
            DeliveryClaim claim = DeliveryClaim.newBuilder()
                    .setWork(state.work)
                    .setWorkerId(workerId)
                    .setLeaseToken(UUID.randomUUID().toString())
                    .setAttempt(state.attempt + 1)
                    .setLeaseExpiresAt(RemoteValidation.timestamp(leaseExpiry))
                    .build();
            append(ChannelRecord.newBuilder()
                    .setClaimed(WorkClaimed.newBuilder().setClaim(claim)));
            claimed.add(claim);
        }
        return List.copyOf(claimed);
    }

    @Override
    public synchronized void complete(
            String workerId, ProcessorCompletion completion, Instant now) {
        requireOpen();
        RemoteValidation.annotations(completion);
        WorkState state = requireClaim(workerId, completion.getDeliveryId(),
                completion.getLeaseToken(), now);
        validateCompletion(state.work.getContract(), completion);
        append(ChannelRecord.newBuilder()
                .setCompleted(WorkCompleted.newBuilder()
                        .setWorkerId(workerId)
                        .setCompletion(completion)));
    }

    @Override
    public synchronized void fail(
            String workerId, ProcessorFailure failure, Instant now) {
        requireOpen();
        RemoteValidation.annotations(failure);
        WorkState state = requireClaim(workerId, failure.getDeliveryId(),
                failure.getLeaseToken(), now);
        String code = bounded(failure.getCode(), 128, "remote-processor-failed");
        String message = bounded(failure.getMessage(), 8_192, "remote processor failed");
        boolean retry = failure.getRetryable()
                && state.attempt < state.work.getMaxAttempts()
                && RemoteValidation.instant(state.work.getDeadline()).isAfter(now);
        if (retry) {
            appendReleased(state, message, failure.getLeaseToken());
        } else {
            appendFailed(state, code, message, failure.getLeaseToken());
        }
    }

    @Override
    public synchronized Completion awaitCompletion(String deliveryId, Instant deadline)
            throws InterruptedException {
        requireOpen();
        Objects.requireNonNull(deadline, "deadline");
        Duration budget = Duration.between(clock.instant(), deadline);
        long budgetNanos;
        try {
            budgetNanos = budget.toNanos();
        } catch (ArithmeticException e) {
            budgetNanos = Long.MAX_VALUE;
        }
        long endNanos = saturatedAdd(System.nanoTime(), Math.max(0, budgetNanos));
        while (true) {
            expire(clock.instant());
            WorkState state = requireState(deliveryId);
            if (state.status == DeliveryState.COMPLETED
                    || state.status == DeliveryState.SETTLED) {
                return state.completion;
            }
            if (state.status == DeliveryState.FAILED) {
                throw new RemoteProcessorException(
                        state.failureCode, state.failureMessage);
            }
            long remaining = endNanos - System.nanoTime();
            if (remaining <= 0) {
                throw new RemoteProcessorException("remote-deadline-exceeded",
                        "delivery " + deliveryId + " did not complete before " + deadline);
            }
            long millis = TimeUnit.NANOSECONDS.toMillis(remaining);
            int nanos = (int) (remaining - TimeUnit.MILLISECONDS.toNanos(millis));
            wait(millis, nanos);
            requireOpen();
        }
    }

    @Override
    public synchronized void settle(String deliveryId, String leaseToken, Instant now) {
        requireOpen();
        Objects.requireNonNull(now, "now");
        expire(now);
        WorkState state = requireState(deliveryId);
        if (state.status == DeliveryState.SETTLED
                && state.lastLeaseToken.equals(leaseToken)) {
            return;
        }
        if (state.status != DeliveryState.COMPLETED) {
            throw new IllegalStateException("delivery " + deliveryId
                    + " is not completed; state=" + state.status);
        }
        requireToken(state, leaseToken);
        append(ChannelRecord.newBuilder()
                .setSettled(WorkSettled.newBuilder()
                        .setDeliveryId(deliveryId)
                        .setLeaseToken(leaseToken)));
    }

    @Override
    public synchronized void release(
            String deliveryId, String leaseToken, String reason, Instant now) {
        requireOpen();
        Objects.requireNonNull(now, "now");
        WorkState state = requireState(deliveryId);
        if (state.status == DeliveryState.PENDING
                && state.lastLeaseToken.equals(leaseToken)) {
            return;
        }
        if (state.status != DeliveryState.CLAIMED
                && state.status != DeliveryState.COMPLETED) {
            throw new IllegalStateException("delivery " + deliveryId
                    + " cannot be released from state " + state.status);
        }
        requireToken(state, leaseToken);
        if (state.attempt >= state.work.getMaxAttempts()
                || !RemoteValidation.instant(state.work.getDeadline()).isAfter(now)) {
            appendFailed(state, "downstream-failed",
                    bounded(reason, 8_192, "downstream processing failed"), leaseToken);
        } else {
            appendReleased(state, bounded(reason, 8_192, "delivery released"), leaseToken);
        }
    }

    @Override
    public synchronized void releaseWorker(String workerId, String reason, Instant now) {
        requireOpen();
        RemoteValidation.workerId(workerId);
        List<WorkState> owned = states.values().stream()
                .filter(state -> state.status == DeliveryState.CLAIMED)
                .filter(state -> state.claim.getWorkerId().equals(workerId))
                .toList();
        for (WorkState state : owned) {
            release(state.work.getDeliveryId(), state.lastLeaseToken, reason, now);
        }
    }

    @Override
    public synchronized int expire(Instant now) {
        requireOpen();
        Objects.requireNonNull(now, "now");
        int transitions = 0;
        for (WorkState state : List.copyOf(states.values())) {
            if (state.status == DeliveryState.FAILED
                    || state.status == DeliveryState.SETTLED) {
                continue;
            }
            if (!RemoteValidation.instant(state.work.getDeadline()).isAfter(now)) {
                appendFailed(state, "work-deadline-exceeded",
                        "delivery deadline elapsed before downstream settlement",
                        state.lastLeaseToken);
                transitions++;
                continue;
            }
            if (state.status == DeliveryState.CLAIMED
                    && !RemoteValidation.instant(state.claim.getLeaseExpiresAt()).isAfter(now)) {
                if (state.attempt >= state.work.getMaxAttempts()) {
                    appendFailed(state, "lease-expired",
                            "delivery lease expired and max_attempts is exhausted",
                            state.lastLeaseToken);
                } else {
                    appendReleased(state, "delivery lease expired", state.lastLeaseToken);
                }
                transitions++;
            }
        }
        return transitions;
    }

    @Override
    public synchronized Optional<DeliveryView> delivery(String deliveryId) {
        WorkState state = states.get(deliveryId);
        return state == null ? Optional.empty() : Optional.of(state.view());
    }

    @Override
    public synchronized List<DeliveryView> deliveries() {
        return states.values().stream().map(WorkState::view).toList();
    }

    @Override
    public synchronized List<ChannelRecord> records() {
        return List.copyOf(log);
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        notifyAll();
        try {
            lock.release();
            file.close();
        } catch (IOException e) {
            throw new IllegalStateException(
                    "failed to close processor channel " + path, e);
        }
    }

    private WorkState requireClaim(
            String workerId, String deliveryId, String leaseToken, Instant now) {
        RemoteValidation.workerId(workerId);
        Objects.requireNonNull(now, "now");
        RemoteValidation.uuid(deliveryId, "delivery_id");
        RemoteValidation.uuid(leaseToken, "lease_token");
        expire(now);
        WorkState state = requireState(deliveryId);
        if (state.status != DeliveryState.CLAIMED) {
            throw new IllegalStateException("delivery " + deliveryId
                    + " has no active claim; state=" + state.status);
        }
        requireToken(state, leaseToken);
        if (!state.claim.getWorkerId().equals(workerId)) {
            throw new IllegalArgumentException("delivery " + deliveryId
                    + " is claimed by another worker");
        }
        return state;
    }

    private void validateCompletion(
            ProcessorContract contract, ProcessorCompletion completion) {
        if (completion.getOutputsCount() > contract.getMaxOutputs()) {
            throw new IllegalArgumentException("processor " + contract.getProcessorId()
                    + " returned " + completion.getOutputsCount()
                    + " outputs, exceeding max_outputs " + contract.getMaxOutputs());
        }
        for (TypedPayload output : completion.getOutputsList()) {
            DescriptorIdentity actual = DescriptorIdentity.of(
                    RuntimeSchemas.unpack(descriptors, output).getDescriptorForType());
            boolean declared = contract.getOutputSchemasList().stream()
                    .map(RuntimeSchemas::identity)
                    .anyMatch(actual::equals);
            if (!declared) {
                throw new IllegalArgumentException("processor " + contract.getProcessorId()
                        + " returned undeclared schema " + actual);
            }
        }
    }

    private void appendReleased(WorkState state, String reason, String leaseToken) {
        append(ChannelRecord.newBuilder()
                .setReleased(WorkReleased.newBuilder()
                        .setDeliveryId(state.work.getDeliveryId())
                        .setLeaseToken(leaseToken)
                        .setReason(reason)));
    }

    private void appendFailed(
            WorkState state, String code, String message, String leaseToken) {
        append(ChannelRecord.newBuilder()
                .setFailed(WorkFailed.newBuilder()
                        .setDeliveryId(state.work.getDeliveryId())
                        .setLeaseToken(leaseToken)
                        .setCode(code)
                        .setMessage(message)));
    }

    private void append(ChannelRecord.Builder event) {
        ChannelRecord record = event
                .setSequence(log.size() + 1L)
                .setRecordedAt(RemoteValidation.timestamp(clock.instant()))
                .build();
        validate(record);
        byte[] bytes = record.toByteArray();
        if (bytes.length > MAX_RECORD_BYTES) {
            throw new IllegalArgumentException("channel record exceeds "
                    + MAX_RECORD_BYTES + " bytes");
        }
        CRC32C checksum = new CRC32C();
        checksum.update(bytes, 0, bytes.length);
        ByteBuffer frame = ByteBuffer.allocate(Integer.BYTES + bytes.length + Integer.BYTES)
                .putInt(bytes.length)
                .put(bytes)
                .putInt((int) checksum.getValue());
        frame.flip();
        try {
            file.position(file.size());
            while (frame.hasRemaining()) {
                file.write(frame);
            }
            file.force(true);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "failed to append processor channel record", e);
        }
        apply(record);
        log.add(record);
        notifyAll();
    }

    /** Validates the complete reducer transition before any bytes become durable. */
    private void validate(ChannelRecord record) {
        RemoteValidation.annotations(record);
        long expected = log.size() + 1L;
        if (record.getSequence() != expected) {
            throw new IllegalArgumentException("channel sequence gap: expected "
                    + expected + " but found " + record.getSequence());
        }
        if (!record.hasRecordedAt()) {
            throw new IllegalArgumentException("channel record requires recorded_at");
        }
        RemoteValidation.instant(record.getRecordedAt());
        switch (record.getEventCase()) {
            case ENQUEUED -> validateEnqueued(record.getEnqueued());
            case CLAIMED -> validateClaimed(record.getClaimed());
            case COMPLETED -> validateCompleted(record.getCompleted());
            case RELEASED -> validateReleased(record.getReleased());
            case FAILED -> validateFailed(record.getFailed());
            case SETTLED -> validateSettled(record.getSettled());
            case EVENT_NOT_SET -> throw new IllegalArgumentException(
                    "channel record " + record.getSequence() + " has no event");
        }
    }

    private void validateEnqueued(WorkEnqueued event) {
        ProcessorWork work = event.getWork();
        RemoteValidation.work(work, descriptors);
        if (states.containsKey(work.getDeliveryId())) {
            throw new IllegalArgumentException("channel re-enqueues delivery_id "
                    + work.getDeliveryId());
        }
    }

    private void validateClaimed(WorkClaimed event) {
        DeliveryClaim claim = event.getClaim();
        RemoteValidation.claim(claim, descriptors);
        WorkState state = requireState(claim.getWork().getDeliveryId());
        if (state.status != DeliveryState.PENDING || !state.work.equals(claim.getWork())) {
            throw new IllegalArgumentException("invalid claim transition for delivery "
                    + state.work.getDeliveryId());
        }
        if (claim.getAttempt() != state.attempt + 1) {
            throw new IllegalArgumentException("delivery " + state.work.getDeliveryId()
                    + " claim attempt must be " + (state.attempt + 1));
        }
    }

    private void validateCompleted(WorkCompleted event) {
        ProcessorCompletion completion = event.getCompletion();
        RemoteValidation.workerId(event.getWorkerId());
        RemoteValidation.uuid(completion.getDeliveryId(), "delivery_id");
        RemoteValidation.uuid(completion.getLeaseToken(), "lease_token");
        WorkState state = requireState(completion.getDeliveryId());
        if (state.status != DeliveryState.CLAIMED
                || !state.claim.getWorkerId().equals(event.getWorkerId())) {
            throw new IllegalArgumentException("invalid completion transition for delivery "
                    + completion.getDeliveryId());
        }
        requireToken(state, completion.getLeaseToken());
        validateCompletion(state.work.getContract(), completion);
    }

    private void validateReleased(WorkReleased event) {
        RemoteValidation.uuid(event.getDeliveryId(), "delivery_id");
        RemoteValidation.uuid(event.getLeaseToken(), "lease_token");
        WorkState state = requireState(event.getDeliveryId());
        if (state.status != DeliveryState.CLAIMED
                && state.status != DeliveryState.COMPLETED) {
            throw new IllegalArgumentException("invalid release transition for delivery "
                    + event.getDeliveryId() + " from " + state.status);
        }
        requireToken(state, event.getLeaseToken());
        if (event.getReason().isBlank() || event.getReason().length() > 8_192) {
            throw new IllegalArgumentException(
                    "release reason must contain between 1 and 8192 characters");
        }
    }

    private void validateFailed(WorkFailed event) {
        RemoteValidation.uuid(event.getDeliveryId(), "delivery_id");
        WorkState state = requireState(event.getDeliveryId());
        if (state.status == DeliveryState.SETTLED
                || state.status == DeliveryState.FAILED) {
            throw new IllegalArgumentException("invalid failure transition for delivery "
                    + event.getDeliveryId() + " from " + state.status);
        }
        if (!event.getLeaseToken().isEmpty()) {
            RemoteValidation.uuid(event.getLeaseToken(), "lease_token");
            requireToken(state, event.getLeaseToken());
        }
        if (event.getCode().isBlank() || event.getCode().length() > 128) {
            throw new IllegalArgumentException(
                    "failure code must contain between 1 and 128 characters");
        }
        if (event.getMessage().isBlank() || event.getMessage().length() > 8_192) {
            throw new IllegalArgumentException(
                    "failure message must contain between 1 and 8192 characters");
        }
    }

    private void validateSettled(WorkSettled event) {
        RemoteValidation.uuid(event.getDeliveryId(), "delivery_id");
        RemoteValidation.uuid(event.getLeaseToken(), "lease_token");
        WorkState state = requireState(event.getDeliveryId());
        if (state.status != DeliveryState.COMPLETED) {
            throw new IllegalArgumentException("invalid settlement transition for delivery "
                    + event.getDeliveryId() + " from " + state.status);
        }
        requireToken(state, event.getLeaseToken());
    }

    private void apply(ChannelRecord record) {
        long expected = log.size() + 1L;
        if (record.getSequence() != expected) {
            throw new IllegalArgumentException("channel sequence gap: expected "
                    + expected + " but found " + record.getSequence());
        }
        switch (record.getEventCase()) {
            case ENQUEUED -> applyEnqueued(record.getEnqueued());
            case CLAIMED -> applyClaimed(record.getClaimed());
            case COMPLETED -> applyCompleted(record.getCompleted());
            case RELEASED -> applyReleased(record.getReleased());
            case FAILED -> applyFailed(record.getFailed());
            case SETTLED -> applySettled(record.getSettled());
            case EVENT_NOT_SET -> throw new IllegalArgumentException(
                    "channel record " + record.getSequence() + " has no event");
        }
    }

    private void applyEnqueued(WorkEnqueued event) {
        ProcessorWork work = event.getWork();
        RemoteValidation.work(work, descriptors);
        if (states.putIfAbsent(work.getDeliveryId(), new WorkState(work)) != null) {
            throw new IllegalArgumentException("channel re-enqueues delivery_id "
                    + work.getDeliveryId());
        }
    }

    private void applyClaimed(WorkClaimed event) {
        DeliveryClaim claim = event.getClaim();
        WorkState state = requireState(claim.getWork().getDeliveryId());
        if (state.status != DeliveryState.PENDING || !state.work.equals(claim.getWork())) {
            throw new IllegalArgumentException("invalid claim transition for delivery "
                    + state.work.getDeliveryId());
        }
        RemoteValidation.workerId(claim.getWorkerId());
        RemoteValidation.uuid(claim.getLeaseToken(), "lease_token");
        RemoteValidation.instant(claim.getLeaseExpiresAt());
        if (claim.getAttempt() != state.attempt + 1) {
            throw new IllegalArgumentException("delivery " + state.work.getDeliveryId()
                    + " claim attempt must be " + (state.attempt + 1));
        }
        state.status = DeliveryState.CLAIMED;
        state.attempt = claim.getAttempt();
        state.claim = claim;
        state.lastLeaseToken = claim.getLeaseToken();
        state.completion = null;
        state.failureCode = "";
        state.failureMessage = "";
    }

    private void applyCompleted(WorkCompleted event) {
        ProcessorCompletion completion = event.getCompletion();
        WorkState state = requireState(completion.getDeliveryId());
        if (state.status != DeliveryState.CLAIMED
                || !state.claim.getWorkerId().equals(event.getWorkerId())) {
            throw new IllegalArgumentException("invalid completion transition for delivery "
                    + completion.getDeliveryId());
        }
        requireToken(state, completion.getLeaseToken());
        validateCompletion(state.work.getContract(), completion);
        state.status = DeliveryState.COMPLETED;
        state.completion = new Completion(event.getWorkerId(), completion);
    }

    private void applyReleased(WorkReleased event) {
        WorkState state = requireState(event.getDeliveryId());
        if (state.status != DeliveryState.CLAIMED
                && state.status != DeliveryState.COMPLETED) {
            throw new IllegalArgumentException("invalid release transition for delivery "
                    + event.getDeliveryId() + " from " + state.status);
        }
        requireToken(state, event.getLeaseToken());
        state.status = DeliveryState.PENDING;
        state.claim = null;
        state.completion = null;
    }

    private void applyFailed(WorkFailed event) {
        WorkState state = requireState(event.getDeliveryId());
        if (state.status == DeliveryState.SETTLED
                || state.status == DeliveryState.FAILED) {
            throw new IllegalArgumentException("invalid failure transition for delivery "
                    + event.getDeliveryId() + " from " + state.status);
        }
        if (!event.getLeaseToken().isEmpty()) {
            requireToken(state, event.getLeaseToken());
        }
        state.status = DeliveryState.FAILED;
        state.failureCode = event.getCode();
        state.failureMessage = event.getMessage();
        state.completion = null;
    }

    private void applySettled(WorkSettled event) {
        WorkState state = requireState(event.getDeliveryId());
        if (state.status != DeliveryState.COMPLETED) {
            throw new IllegalArgumentException("invalid settlement transition for delivery "
                    + event.getDeliveryId() + " from " + state.status);
        }
        requireToken(state, event.getLeaseToken());
        state.status = DeliveryState.SETTLED;
    }

    private void initializeAndReplay() throws IOException {
        if (file.size() == 0) {
            ByteBuffer header = ByteBuffer.wrap(MAGIC);
            while (header.hasRemaining()) {
                file.write(header);
            }
            file.force(true);
            return;
        }
        if (file.size() < MAGIC.length) {
            throw new IllegalArgumentException("processor channel has an incomplete header: "
                    + path);
        }
        ByteBuffer header = ByteBuffer.allocate(MAGIC.length);
        readFully(header, 0);
        if (!java.util.Arrays.equals(header.array(), MAGIC)) {
            throw new IllegalArgumentException("processor channel format marker mismatch: "
                    + path);
        }
        long position = MAGIC.length;
        while (position < file.size()) {
            long frameStart = position;
            if (file.size() - position < Integer.BYTES) {
                truncateTail(frameStart);
                break;
            }
            ByteBuffer lengthBytes = ByteBuffer.allocate(Integer.BYTES);
            readFully(lengthBytes, position);
            int length = ByteBuffer.wrap(lengthBytes.array()).getInt();
            if (length < 1 || length > MAX_RECORD_BYTES) {
                throw new IllegalArgumentException("invalid processor channel frame length "
                        + length + " at byte " + position);
            }
            position += Integer.BYTES;
            if (file.size() - position < (long) length + Integer.BYTES) {
                truncateTail(frameStart);
                break;
            }
            ByteBuffer recordBytes = ByteBuffer.allocate(length);
            readFully(recordBytes, position);
            position += length;
            ByteBuffer checksumBytes = ByteBuffer.allocate(Integer.BYTES);
            readFully(checksumBytes, position);
            position += Integer.BYTES;
            int storedChecksum = ByteBuffer.wrap(checksumBytes.array()).getInt();
            CRC32C checksum = new CRC32C();
            checksum.update(recordBytes.array(), 0, length);
            if ((int) checksum.getValue() != storedChecksum) {
                throw new IllegalArgumentException("processor channel CRC mismatch at byte "
                        + frameStart);
            }
            ChannelRecord record;
            try {
                record = ChannelRecord.parseFrom(recordBytes.array());
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalArgumentException(
                        "invalid processor channel protobuf at byte " + frameStart, e);
            }
            validate(record);
            apply(record);
            log.add(record);
        }
        file.position(file.size());
    }

    private void truncateTail(long position) throws IOException {
        file.truncate(position);
        file.force(true);
    }

    private void readFully(ByteBuffer destination, long position) throws IOException {
        while (destination.hasRemaining()) {
            int read = file.read(destination, position);
            if (read < 0) {
                throw new IOException("unexpected EOF in processor channel");
            }
            position += read;
        }
    }

    private WorkState requireState(String deliveryId) {
        WorkState state = states.get(deliveryId);
        if (state == null) {
            throw new IllegalArgumentException("unknown delivery_id " + deliveryId);
        }
        return state;
    }

    private static void requireToken(WorkState state, String leaseToken) {
        if (!state.lastLeaseToken.equals(leaseToken)) {
            throw new IllegalArgumentException("stale lease_token for delivery "
                    + state.work.getDeliveryId());
        }
    }

    private void requireOpen() {
        if (closed) {
            throw new IllegalStateException("processor channel is closed");
        }
    }

    private static String bounded(
            String value, int maximum, String fallback) {
        String result = value == null || value.isBlank() ? fallback : value;
        return result.length() <= maximum ? result : result.substring(0, maximum);
    }

    private static long saturatedAdd(long first, long second) {
        long result = first + second;
        if (((first ^ result) & (second ^ result)) < 0) {
            return Long.MAX_VALUE;
        }
        return result;
    }

    private static void closeAfterFailedOpen(FileLock lock, FileChannel file) {
        try {
            if (lock != null && lock.isValid()) {
                lock.release();
            }
        } catch (IOException ignored) {
            // The original open or recovery failure remains the useful refusal.
        }
        try {
            if (file != null && file.isOpen()) {
                file.close();
            }
        } catch (IOException ignored) {
            // The original open or recovery failure remains the useful refusal.
        }
    }

    private static final class WorkState {
        private final ProcessorWork work;
        private DeliveryState status = DeliveryState.PENDING;
        private int attempt;
        private DeliveryClaim claim;
        private Completion completion;
        private String lastLeaseToken = "";
        private String failureCode = "";
        private String failureMessage = "";

        private WorkState(ProcessorWork work) {
            this.work = work;
        }

        private DeliveryView view() {
            return new DeliveryView(work, status, attempt, claim, completion,
                    failureCode, failureMessage);
        }
    }
}
