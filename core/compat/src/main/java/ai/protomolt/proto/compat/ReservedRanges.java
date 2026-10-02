package ai.protomolt.proto.compat;

import com.google.protobuf.DescriptorProtos.DescriptorProto;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * The {@code reserved} declarations: whether a number is still fenced off, and what dropping a
 * fence means. Reservation is the only thing standing between a removed field number and a future
 * reuse that would silently reinterpret old payloads, so losing one is reported even though
 * nothing breaks at the moment it happens.
 */
final class ReservedRanges {

    private ReservedRanges() {
    }

    static void diff(String fqn, DescriptorProto oldMsg, DescriptorProto newMsg,
                     List<SchemaChange> changes) {
        for (DescriptorProto.ReservedRange range : oldMsg.getReservedRangeList()) {
            if (!isReservedRange(newMsg, range.getStart(), range.getEnd())) {
                changes.add(new SchemaChange(ChangeRules.RESERVED_RANGE_REMOVED, fqn,
                        text(range), "",
                        "Message " + fqn + " no longer reserves " + text(range)
                                + "; a future reuse would corrupt old payloads.",
                        DiffImpacts.INFO));
            }
        }
    }

    /**
     * Whether {@code [start, end)} is entirely covered by {@code message}'s reserved ranges.
     * The comparison is by interval: {@code reserved 2 to max} spans over half a billion
     * numbers, so walking a range one number at a time is not viable.
     */
    private static boolean isReservedRange(DescriptorProto message, int start, int end) {
        List<DescriptorProto.ReservedRange> ranges =
                new ArrayList<>(message.getReservedRangeList());
        ranges.sort(Comparator.comparingInt(DescriptorProto.ReservedRange::getStart));
        int covered = start;
        for (DescriptorProto.ReservedRange range : ranges) {
            if (covered >= end) {
                return true;
            }
            if (range.getStart() > covered) {
                return false; // a gap opens before this range begins
            }
            covered = Math.max(covered, range.getEnd());
        }
        return covered >= end;
    }

    static boolean isReservedNumber(DescriptorProto message, int number) {
        for (DescriptorProto.ReservedRange range : message.getReservedRangeList()) {
            if (number >= range.getStart() && number < range.getEnd()) {
                return true;
            }
        }
        return false;
    }

    static String snippet(DescriptorProto message, int number) {
        for (DescriptorProto.ReservedRange range : message.getReservedRangeList()) {
            if (number >= range.getStart() && number < range.getEnd()) {
                return text(range);
            }
        }
        return "";
    }

    static String text(DescriptorProto.ReservedRange range) {
        int lastInclusive = range.getEnd() - 1;
        return range.getStart() == lastInclusive
                ? "reserved " + range.getStart()
                : "reserved " + range.getStart() + " to " + lastInclusive;
    }
}
