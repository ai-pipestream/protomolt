package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.v1.ChannelRecord;

import java.util.List;

/** Atomic append source used by the shared processor-channel reducer. */
interface ProcessorChannelJournal extends AutoCloseable {

    List<ChannelRecord> load();

    void append(ChannelRecord record);

    @Override
    void close();
}
