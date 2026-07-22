package ai.pipestream.proto.connector;

import com.google.protobuf.Int32Value;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The bounded bridge between a push source and a synchronous consumer: a flooding
 * producer gets paused and resumed, ordering survives, failures surface after the
 * messages that beat them, and close frees a blocked producer.
 */
class SourcePumpTest {

    private static final SourcePlan NO_PLAN = new SourcePlan() {
    };

    /** A handle that records its flow-control calls; the flood test drives it. */
    private static final class RecordingHandle implements StreamSource.Handle {
        final AtomicInteger pauses = new AtomicInteger();
        final AtomicInteger resumes = new AtomicInteger();
        final AtomicInteger closes = new AtomicInteger();
        volatile boolean paused;

        @Override
        public void pause() {
            paused = true;
            pauses.incrementAndGet();
        }

        @Override
        public void resume() {
            paused = false;
            resumes.incrementAndGet();
        }

        @Override
        public void close() {
            closes.incrementAndGet();
        }
    }

    /** Pushes {@code count} messages from its own thread, honoring pause and close. */
    private static final class FloodHandle implements StreamSource.Handle {
        private final RecordingHandle flow = new RecordingHandle();
        private final Thread thread;
        private volatile boolean running = true;

        FloodHandle(int count, StreamSource.Listener listener) {
            this.thread = new Thread(() -> {
                for (int i = 0; i < count && running; i++) {
                    while (flow.paused && running) {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                    }
                    if (running) {
                        listener.onMessage(Int32Value.of(i));
                    }
                }
                if (running) {
                    listener.onComplete();
                }
            }, "flood-source");
            this.thread.setDaemon(true);
        }

        @Override
        public void pause() {
            flow.pause();
        }

        @Override
        public void resume() {
            flow.resume();
        }

        @Override
        public void close() {
            running = false;
            flow.close();
        }
    }

    private static int valueOf(Message message) {
        return ((Int32Value) message).getValue();
    }

    @Test
    void capacityMustBePositive() {
        assertThatThrownBy(() -> new SourcePump(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("capacity must be positive: 0");
        assertThatThrownBy(() -> new SourcePump(-4))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("capacity must be positive: -4");
    }

    /**
     * The source can push before {@code open} has returned its handle, so a queue that filled
     * during that unattached window has to be paused the moment the handle arrives — otherwise
     * the source stays unthrottled until the next take.
     */
    @Test
    void attachPausesASourceThatFilledTheQueueBeforeTheHandleArrived() {
        SourcePump pump = new SourcePump(2);
        pump.onMessage(Int32Value.of(0));
        pump.onMessage(Int32Value.of(1));
        assertThat(pump.depth()).isEqualTo(2);

        RecordingHandle handle = new RecordingHandle();
        pump.attach(handle);

        assertThat(handle.pauses.get()).isEqualTo(1);
        assertThat(handle.paused).isTrue();
        assertThat(handle.resumes.get()).isZero();
        pump.close();
    }

    @Test
    void attachDoesNotPauseASourceWhenTheQueueStillHasRoom() {
        SourcePump pump = new SourcePump(2);
        pump.onMessage(Int32Value.of(0));

        RecordingHandle handle = new RecordingHandle();
        pump.attach(handle);

        assertThat(handle.pauses.get()).isZero();
        assertThat(handle.paused).isFalse();
        pump.close();
    }

    @Test
    void floodingProducerIsPausedResumedAndFullyDrainedInOrder() throws Exception {
        SourcePump pump = new SourcePump(8);
        FloodHandle handle = new FloodHandle(100, pump);
        handle.thread.start();
        pump.attach(handle);

        // Hold back consumption until the queue fills and the pump pauses the source.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (handle.flow.pauses.get() == 0 && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(handle.flow.pauses.get()).as("queue filled and source paused").isGreaterThan(0);

        for (int i = 0; i < 100; i++) {
            Message next = pump.take(Duration.ofSeconds(5));
            assertThat(next).as("message %d arrives", i).isNotNull();
            assertThat(valueOf(next)).isEqualTo(i);
        }

        assertThat(handle.flow.resumes.get()).as("draining resumed the source").isGreaterThan(0);
        assertThat(pump.take(Duration.ofMillis(200))).as("stream ended and drained").isNull();
        deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!pump.isClosed() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(pump.isCompleted()).isTrue();
        assertThat(pump.isClosed()).isTrue();
        pump.close();
    }

    @Test
    void completionDrainsThenCloses() {
        SourcePump pump = new SourcePump(8);
        pump.attach(new RecordingHandle());
        pump.onMessage(Int32Value.of(7));
        pump.onComplete();

        assertThat(valueOf(pump.take(Duration.ofSeconds(1)))).isEqualTo(7);
        assertThat(pump.take(Duration.ofMillis(100))).isNull();
        assertThat(pump.isClosed()).isTrue();
    }

    @Test
    void failureSurfacesAfterBufferedMessagesDrain() {
        SourcePump pump = new SourcePump(8);
        pump.attach(new RecordingHandle());
        pump.onMessage(Int32Value.of(0));
        pump.onMessage(Int32Value.of(1));
        pump.onError(new IllegalStateException("boom"));

        assertThat(valueOf(pump.take(Duration.ofSeconds(1)))).isEqualTo(0);
        assertThat(valueOf(pump.take(Duration.ofSeconds(1)))).isEqualTo(1);
        assertThatThrownBy(() -> pump.take(Duration.ofMillis(100)))
                .isInstanceOf(SourceException.class)
                .hasCauseInstanceOf(IllegalStateException.class);
        assertThat(pump.isClosed()).isTrue();
    }

    @Test
    void closeFreesAProducerBlockedOnAFullQueue() throws Exception {
        SourcePump pump = new SourcePump(4);
        RecordingHandle recording = new RecordingHandle();
        pump.attach(recording);

        AtomicInteger offered = new AtomicInteger();
        Thread producer = new Thread(() -> {
            for (int i = 0; i < 100 && !Thread.currentThread().isInterrupted(); i++) {
                pump.onMessage(Int32Value.of(i));
                offered.incrementAndGet();
            }
        }, "blocked-producer");
        producer.setDaemon(true);
        producer.start();

        // Let the producer fill the queue and wedge on the fifth message.
        Thread.sleep(200);
        assertThat(producer.isAlive()).isTrue();
        assertThat(pump.depth()).isEqualTo(4);

        pump.close();
        producer.join(TimeUnit.SECONDS.toMillis(2));
        assertThat(producer.isAlive()).as("close unblocks the producer").isFalse();
        assertThat(recording.closes.get()).isEqualTo(1);
    }
}
