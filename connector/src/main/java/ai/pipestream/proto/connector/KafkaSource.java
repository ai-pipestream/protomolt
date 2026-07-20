package ai.pipestream.proto.connector;

import com.google.protobuf.Message;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A Kafka topic as a {@link StreamSource}. One poll thread subscribes and pushes each
 * record through the plan's {@link MessageParser}. Pausing pauses the assigned partitions
 * while polls continue, so group membership survives a stall; closing wakes the poll loop
 * and leaves offsets where the commit policy put them.
 *
 * <p>Defaults are a live tap: auto-commit on, newest offsets. Offset ownership is a
 * deployment concern carried in {@link KafkaSourcePlan#overrides()}, not part of this
 * interface; for managed offsets and rebalance-safe delivery, the Kafka Connect source
 * ({@code protomolt-connect}) is the production path.</p>
 *
 * <p>One caveat for bounded consumers: a full {@link SourcePump} blocks the poll thread,
 * and a stall longer than {@code max.poll.interval.ms} drops the consumer from its group.
 * It rejoins and resumes from the last committed offsets.</p>
 */
public final class KafkaSource implements StreamSource<KafkaSourcePlan> {

    private static final Duration POLL = Duration.ofMillis(100);

    @Override
    public String name() {
        return "kafka";
    }

    @Override
    public Handle open(KafkaSourcePlan plan, Listener listener) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, plan.bootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, plan.groupId());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        config.putAll(plan.overrides());
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(config);
        consumer.subscribe(List.of(plan.topic()));
        KafkaHandle handle = new KafkaHandle(consumer, plan, listener);
        handle.thread.start();
        return handle;
    }

    private static final class KafkaHandle implements Handle {
        private final KafkaConsumer<byte[], byte[]> consumer;
        private final MessageParser parser;
        private final Listener listener;
        private final AtomicBoolean terminated = new AtomicBoolean();
        private final Thread thread;
        private volatile boolean paused;
        private volatile boolean pauseApplied;
        private volatile boolean running = true;

        KafkaHandle(KafkaConsumer<byte[], byte[]> consumer, KafkaSourcePlan plan, Listener listener) {
            this.consumer = consumer;
            this.parser = plan.parser();
            this.listener = listener;
            this.thread = new Thread(this::pump, "kafka-source");
            this.thread.setDaemon(true);
        }

        private void pump() {
            try {
                while (running) {
                    applyPause();
                    ConsumerRecords<byte[], byte[]> records = consumer.poll(POLL);
                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        if (!running) {
                            return;
                        }
                        Message message;
                        try {
                            message = parser.parse(record.topic(), record.value());
                        } catch (RuntimeException e) {
                            fireError(new SourceException("could not parse a record from "
                                    + record.topic() + ":" + record.partition()
                                    + "@" + record.offset(), e));
                            return;
                        }
                        listener.onMessage(message);
                    }
                }
            } catch (WakeupException e) {
                // close() asked us to stop; fall through to the terminal signal.
            } catch (RuntimeException e) {
                fireError(e);
            } finally {
                try {
                    consumer.close();
                } catch (RuntimeException ignored) {
                    // Closing a woken consumer can throw; the stream is over either way.
                }
                fireComplete();
            }
        }

        private void applyPause() {
            if (paused && !pauseApplied && !consumer.assignment().isEmpty()) {
                consumer.pause(consumer.assignment());
                pauseApplied = true;
            } else if (!paused && pauseApplied) {
                consumer.resume(consumer.assignment());
                pauseApplied = false;
            }
        }

        // The consumer is not thread-safe, so these only set flags (close also wakes the
        // poll); the poll thread applies them within one poll interval.
        @Override
        public void pause() {
            paused = true;
        }

        @Override
        public void resume() {
            paused = false;
        }

        @Override
        public void close() {
            running = false;
            consumer.wakeup();
            try {
                thread.join(TimeUnit.SECONDS.toMillis(5));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        private void fireComplete() {
            if (terminated.compareAndSet(false, true)) {
                listener.onComplete();
            }
        }

        private void fireError(Throwable error) {
            if (terminated.compareAndSet(false, true)) {
                listener.onError(error);
            }
        }
    }
}
