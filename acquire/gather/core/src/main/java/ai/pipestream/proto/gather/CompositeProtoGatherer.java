package ai.pipestream.proto.gather;

import ai.pipestream.proto.sources.ProtoSourceSet;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Gathers from an ordered list of gatherers and merges the results into one
 * {@link ProtoSourceSet}.
 *
 * <p>The same import path may be produced by more than one gatherer only with identical
 * content (the first gatherer's origin wins); differing content is a {@link GatherException}
 * naming both origins. The composite {@link #isAvailable() is available} only when every child
 * is, since a gather requires all of them.</p>
 */
public final class CompositeProtoGatherer implements ProtoGatherer {

    private final List<ProtoGatherer> gatherers;

    public CompositeProtoGatherer(List<ProtoGatherer> gatherers) {
        Objects.requireNonNull(gatherers, "gatherers");
        if (gatherers.isEmpty()) {
            throw new IllegalArgumentException("At least one gatherer is required");
        }
        this.gatherers = List.copyOf(gatherers);
    }

    public static CompositeProtoGatherer of(ProtoGatherer... gatherers) {
        return new CompositeProtoGatherer(List.of(gatherers));
    }

    @Override
    public ProtoSourceSet gather() throws GatherException {
        ProtoSourceSet merged = ProtoSourceSet.empty();
        for (ProtoGatherer gatherer : gatherers) {
            try {
                merged = merged.merge(gatherer.gather());
            } catch (IllegalStateException e) {
                throw new GatherException("Conflicting proto sources across gatherers: "
                        + e.getMessage(), e);
            }
        }
        return merged;
    }

    @Override
    public String origin() {
        return gatherers.stream()
                .map(ProtoGatherer::origin)
                .collect(Collectors.joining(", ", "composite[", "]"));
    }

    @Override
    public boolean isAvailable() {
        return gatherers.stream().allMatch(ProtoGatherer::isAvailable);
    }
}
