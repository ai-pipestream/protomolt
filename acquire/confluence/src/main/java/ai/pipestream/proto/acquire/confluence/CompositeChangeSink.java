package ai.pipestream.proto.acquire.confluence;

import ai.pipestream.proto.acquire.confluence.v1.ConfluenceChange;
import ai.pipestream.proto.acquire.confluence.v1.ConfluenceSnapshot;

import java.util.List;

/**
 * Fans one emission out to several sinks, in list order. An exception from an
 * earlier sink skips the later ones and propagates to the caller: composing
 * sinks is wiring, not error policy, so each sink keeps its own failure
 * semantics (see {@link KafkaChangeSink} versus {@link RepoChangeSink}).
 */
public final class CompositeChangeSink implements ChangeSink {

    private final List<ChangeSink> sinks;

    public CompositeChangeSink(List<ChangeSink> sinks) {
        this.sinks = List.copyOf(sinks);
    }

    @Override
    public void emit(ConfluenceChange change) {
        for (ChangeSink sink : sinks) {
            sink.emit(change);
        }
    }

    @Override
    public void snapshot(ConfluenceSnapshot snapshot) {
        for (ChangeSink sink : sinks) {
            sink.snapshot(snapshot);
        }
    }
}
