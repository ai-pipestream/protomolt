package ai.protomolt.proto.asset.characterize.container;

import ai.protomolt.proto.asset.characterize.ByteWindows;
import ai.protomolt.proto.asset.characterize.container.ContainerSignatures.MemberRule;
import ai.protomolt.proto.asset.characterize.container.ContainerSignatures.Rule;
import ai.protomolt.proto.asset.characterize.signature.SignatureSequence.Outcome;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Identifying a container by what is inside it.
 *
 * <p>Leading magic gets as far as "this is a ZIP" or "this is a compound
 * file", and stops. The formats people actually care about live one level
 * in: a word processor document and a spreadsheet are the same four bytes
 * followed by different members. This lists the members and runs the
 * published rules over them.
 *
 * <p>The result separates what was proved from what could not be
 * examined. A container whose index or members lie outside the byte
 * windows yields no rules and says so, rather than reporting the formats
 * as absent.
 */
public final class ContainerIdentification {

    /** A rule that held. */
    public record Hit(String ruleId, String formatId, String description) {
    }

    /**
     * What examining a container established.
     *
     * @param kind the container structure, or null when the content is not
     *        a container this reads
     * @param hits the rules that held, in published order
     * @param memberCount how many members were listed
     * @param complete whether every rule could be decided
     * @param detail why examination stopped short, blank when it did not
     */
    public record Result(ContainerKind kind, List<Hit> hits, int memberCount,
                         boolean complete, String detail) {

        /** Validates and copies. */
        public Result {
            hits = List.copyOf(hits);
        }

        /** Whether any rule held. */
        public boolean identified() {
            return !hits.isEmpty();
        }

        /** The most specific format identifier found, or blank. */
        public String formatId() {
            return hits.isEmpty() ? "" : hits.getFirst().formatId();
        }
    }

    /** Nothing was examined, because the content is not a container. */
    private static final Result NOT_A_CONTAINER =
            new Result(null, List.of(), 0, true, "");

    private ContainerIdentification() {
    }

    /**
     * Examines a container with the bundled rules.
     *
     * @param bytes the content's windows
     * @return what was established
     */
    public static Result identify(ByteWindows bytes) {
        return identify(bytes, ContainerSignatures.bundled());
    }

    /**
     * Examines a container.
     *
     * @param bytes the content's windows
     * @param signatures the rules to apply
     * @return what was established
     */
    public static Result identify(ByteWindows bytes, ContainerSignatures signatures) {
        if (bytes == null || bytes.isEmpty()) {
            return NOT_A_CONTAINER;
        }
        if (bytes.matchesAt(0, Ole2Members.MAGIC)) {
            return compound(bytes, signatures);
        }
        if (bytes.matchesAt(0, new byte[] {'P', 'K', 0x03, 0x04})) {
            return archive(bytes, signatures);
        }
        return NOT_A_CONTAINER;
    }

    // ------------------------------------------------------------------
    // ZIP
    // ------------------------------------------------------------------

    private static Result archive(ByteWindows bytes, ContainerSignatures signatures) {
        List<ZipMembers.Member> members = ZipMembers.list(bytes);
        if (members == null) {
            return new Result(ContainerKind.ZIP, List.of(), 0, false,
                    "the archive's index is outside the captured window,"
                            + " so its members were not listed");
        }
        Map<String, ZipMembers.Member> byPath = new LinkedHashMap<>();
        for (ZipMembers.Member member : members) {
            byPath.putIfAbsent(member.name(), member);
        }
        Map<String, byte[]> contents = new LinkedHashMap<>();
        Reading reading = new Reading();
        List<Hit> hits = new ArrayList<>();
        for (Rule rule : signatures.rulesFor(ContainerKind.ZIP)) {
            if (holds(rule, path -> byPath.containsKey(path),
                    path -> contents.computeIfAbsent(path,
                            key -> ZipMembers.read(bytes, byPath.get(key))), reading)) {
                hits.add(new Hit(rule.id(), rule.formatId(), rule.description()));
            }
        }
        return new Result(ContainerKind.ZIP, hits, members.size(), !reading.blind,
                reading.blind ? "some members were outside the captured window" : "");
    }

    // ------------------------------------------------------------------
    // Compound files
    // ------------------------------------------------------------------

    private static Result compound(ByteWindows bytes, ContainerSignatures signatures) {
        Ole2Members members = Ole2Members.read(bytes);
        if (members == null) {
            return new Result(ContainerKind.OLE2, List.of(), 0, false,
                    "the compound file's header is outside the captured window");
        }
        Map<String, byte[]> contents = new LinkedHashMap<>();
        Reading reading = new Reading();
        reading.blind = !members.complete();
        List<Hit> hits = new ArrayList<>();
        for (Rule rule : signatures.rulesFor(ContainerKind.OLE2)) {
            if (holds(rule, path -> members.entry(path) != null,
                    path -> contents.computeIfAbsent(path,
                            key -> members.content(members.entry(key))), reading)) {
                hits.add(new Hit(rule.id(), rule.formatId(), rule.description()));
            }
        }
        return new Result(ContainerKind.OLE2, hits, members.entries().size(),
                !reading.blind,
                reading.blind ? "part of the compound file was outside the captured window" : "");
    }

    // ------------------------------------------------------------------
    // Applying a rule
    // ------------------------------------------------------------------

    /** Tracks whether anything a rule needed went unread. */
    private static final class Reading {
        private boolean blind;
    }

    /** Whether every member a rule names holds. */
    private static boolean holds(Rule rule, MemberPresence present, MemberContent content,
                                 Reading reading) {
        for (MemberRule member : rule.members()) {
            if (!present.has(member.path())) {
                return false;
            }
            if (member.pattern() == null) {
                continue;
            }
            byte[] bytes = content.of(member.path());
            if (bytes == null) {
                // The member is there and its bytes are not. This rule
                // cannot be decided, and neither can the ones after it
                // that read the same member.
                reading.blind = true;
                return false;
            }
            if (member.pattern().matchIn(ByteWindows.ofWhole(bytes)) != Outcome.MATCHED) {
                return false;
            }
        }
        return true;
    }

    /** Whether a container lists a member. */
    private interface MemberPresence {
        boolean has(String path);
    }

    /** A member's bytes, or null when they could not be read. */
    private interface MemberContent {
        byte[] of(String path);
    }
}
