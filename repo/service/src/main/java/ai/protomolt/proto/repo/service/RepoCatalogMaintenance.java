package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.accession.CatalogEntryRebuild;
import ai.protomolt.proto.repo.accession.DocumentReads;
import ai.protomolt.proto.repo.v1.NodeAddress;
import java.io.PrintStream;
import java.time.Clock;
import java.util.Objects;
import java.util.function.Supplier;

/** Local operator commands over the configured repository; never mounted on a network listener. */
final class RepoCatalogMaintenance {
    private RepoCatalogMaintenance() {}

    static void run(String[] args, Supplier<RepoServices> open, PrintStream output) {
        // Refuse malformed commands before opening a database or contacting storage.
        var command = parse(args);
        CatalogEntryRebuild.RevisionResult result;
        try (var services = open.get()) {
            var composition = services.v2Composition();
            var reads = new DocumentReads(composition.catalog(), composition.drives(), composition.limits(),
                    services.readBudget(), Clock.systemUTC());
            result = new CatalogEntryRebuild(composition.catalog(), reads)
                    .revision(command.address(), command.revision(), command.repair());
        }
        output.println(report(command.revision(), command.repair(), result));
    }

    /** The one line an operator reads; {@code PURGED} means the document vanished between the two repair transactions. */
    static String report(long revision, boolean repair, CatalogEntryRebuild.RevisionResult result) {
        return "revision=" + revision
                + " core=" + (result.tombstone() ? "TOMBSTONE_NO_CORE" : "VERIFIED_WITH_RETAINED_SCHEMAS")
                + " revision_type=" + (result.revisionTypeRewritten() ? "REBUILT" : "NOT_REQUESTED")
                + " current_entry=" + (!repair ? "NOT_REQUESTED"
                        : result.currentEntryRewritten() ? "REBUILT"
                        : result.purgedBeforeEntryRepair() ? "PURGED" : "NOT_CURRENT");
    }

    private static Command parse(String[] args) {
        Objects.requireNonNull(args);
        if (args.length != 6 || !(args[0].equals("verify-core") || args[0].equals("rebuild-entry")))
            throw new IllegalArgumentException("Usage: protomolt-repo-service "
                    + "{verify-core|rebuild-entry} ACCOUNT GRAPH DATASOURCE_OR_NODE DOCUMENT REVISION");
        for (int i = 1; i < 5; i++) {
            if (args[i].isBlank()) throw new IllegalArgumentException("All four address components are required");
        }
        long revision;
        try {
            revision = Long.parseLong(args[5]);
        } catch (NumberFormatException invalid) {
            throw new IllegalArgumentException("REVISION must be a positive integer", invalid);
        }
        if (revision < 1) throw new IllegalArgumentException("REVISION must be a positive integer");
        var address = NodeAddress.newBuilder().setAccountId(args[1]).setGraphId(args[2])
                .setGraphAddressId(args[3]).setDocId(args[4]).build();
        return new Command(address, revision, args[0].equals("rebuild-entry"));
    }

    private record Command(NodeAddress address, long revision, boolean repair) {}
}
