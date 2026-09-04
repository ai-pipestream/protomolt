package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.v1.ChannelRecord;

import java.util.ArrayList;
import java.util.List;

/** Process-local journal for explicitly non-durable channel policies. */
final class InMemoryProcessorChannelJournal implements ProcessorChannelJournal {

    private final List<ChannelRecord> records = new ArrayList<>();
    private boolean closed;

    @Override
    public synchronized List<ChannelRecord> load() {
        requireOpen();
        return List.copyOf(records);
    }

    @Override
    public synchronized void append(ChannelRecord record) {
        requireOpen();
        records.add(record);
    }

    @Override
    public synchronized void close() {
        closed = true;
        records.clear();
    }

    private void requireOpen() {
        if (closed) {
            throw new IllegalStateException("processor channel journal is closed");
        }
    }
}
