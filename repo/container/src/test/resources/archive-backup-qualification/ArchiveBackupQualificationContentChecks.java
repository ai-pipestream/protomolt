package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.archive.v1.ArchiveMutationReceipt;
import ai.protomolt.proto.repo.archive.v1.ArchiveMutationRequest;
import ai.protomolt.proto.repo.archive.v1.ArchiveMutationServiceGrpc;
import ai.protomolt.proto.repo.archive.v1.ArchiveServiceGrpc;
import ai.protomolt.proto.repo.archive.v1.DeleteRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.GetArchiveMutationRequest;
import ai.protomolt.proto.repo.archive.v1.GetArchiveStatsRequest;
import ai.protomolt.proto.repo.archive.v1.GetArchiveStatsResponse;
import ai.protomolt.proto.repo.archive.v1.GetEntryRequest;
import ai.protomolt.proto.repo.archive.v1.GetEntryResponse;
import ai.protomolt.proto.repo.archive.v1.ListVersionsRequest;
import ai.protomolt.proto.repo.archive.v1.ListVersionsResponse;
import ai.protomolt.proto.repo.archive.v1.PutEntryRequest;
import ai.protomolt.proto.repo.archive.v1.RenditionManifestEntry;
import ai.protomolt.proto.repo.archive.v1.RenditionState;
import ai.protomolt.proto.repo.archive.v1.VersionManifest;
import ai.protomolt.proto.repo.blob.spi.BlobStoreException;
import ai.protomolt.proto.repo.container.archive.ArchiveManifests;
import ai.protomolt.proto.repo.service.RepoServices;
import ai.protomolt.proto.repo.spi.ArchiveMutationRepository;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.stub.MetadataUtils;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * The same archive observations before capture and after restore. Every comparison is against
 * responses and ledger rows the seed host recorded, never against a value this class computes
 * on its own. Reads go through the archive repository (library path) and, for the transport
 * check, the authenticated in-process gRPC services.
 */
final class ArchiveBackupQualificationContentChecks {
    static final String API_TOKEN_HEADER = "api_token";
    private final RepoServices host;
    private final RepositoryBackupRehearsalChecks checks;
    private final Path identities;
    private final Map<String, Object> record;

    ArchiveBackupQualificationContentChecks(RepoServices host, RepositoryBackupRehearsalChecks checks, Path identities, Map<String, Object> record) {
        this.host = host; this.checks = checks; this.identities = identities; this.record = record;
    }

    List<Map<String, Object>> entries() { return RepositoryBackupRehearsalJson.list(record, "entries"); }

    Map<String, Object> entry(String tag) {
        return entries().stream().filter(entry -> RepositoryBackupRehearsalJson.string(entry, "tag").equals(tag)).findFirst()
                .orElseThrow(() -> new RepositoryBackupRehearsalFailure("No recorded entry " + tag));
    }

    static byte[] read(Path directory, String name) {
        try { return Files.readAllBytes(directory.resolve(name)); }
        catch (IOException failure) { throw new RepositoryBackupRehearsalFailure("Cannot read " + name, failure); }
    }

    static EntryAddress address(Map<String, Object> entry) {
        return ArchiveBackupQualificationFixture.address(RepositoryBackupRehearsalJson.string(entry, "account"),
                RepositoryBackupRehearsalJson.string(entry, "archive"), RepositoryBackupRehearsalJson.string(entry, "entryId"));
    }

    static List<Long> versions(Map<String, Object> entry) {
        var versions = new ArrayList<Long>();
        for (Object value : (List<?>) entry.get("versions")) versions.add(((Number) value).longValue());
        return versions;
    }

    static RenditionManifestEntry rendition(VersionManifest manifest, String name) {
        return manifest.getRenditionsList().stream().filter(item -> item.getRendition().getName().equals(name)).findFirst()
                .orElseThrow(() -> new RepositoryBackupRehearsalFailure("Manifest has no rendition " + name));
    }

    /** The recorded manifest of one version from the recorded listing, which the seed writes before its replay check. */
    VersionManifest recordedManifest(String tag, long version) {
        try {
            return ListVersionsResponse.parseFrom(read(identities, "versions-" + tag + ".pb")).getVersionsList().stream()
                    .filter(manifest -> manifest.getVersion() == version).findFirst()
                    .orElseThrow(() -> new RepositoryBackupRehearsalFailure("No recorded manifest for " + tag + " v" + version));
        } catch (InvalidProtocolBufferException failure) { throw new RepositoryBackupRehearsalFailure("Corrupt recorded listing", failure); }
    }

    GetEntryResponse recorded(String tag, long version) {
        try { return GetEntryResponse.parseFrom(read(identities, "entry-" + tag + "-v" + version + ".pb")); }
        catch (InvalidProtocolBufferException failure) { throw new RepositoryBackupRehearsalFailure("Corrupt recorded entry", failure); }
    }

    /**
     * Every recorded version of every entry reads back equal to the recorded response: the bytes
     * of each PRESENT rendition (which the read path verifies against the manifest digest), the
     * manifest with its frozen metadata snapshot and, while nothing has changed since the record,
     * the current entry metadata.
     */
    void verifyEntries(String phase, boolean currentMetadata) { verifyEntries(phase, currentMetadata, Set.of()); }

    /** The same, skipping entries whose retained manifests a later step changed on purpose. */
    void verifyEntries(String phase, boolean currentMetadata, Set<String> skip) {
        var operator = ArchiveBackupQualificationFixture.operator();
        for (var entry : entries()) {
            String tag = RepositoryBackupRehearsalJson.string(entry, "tag");
            if (skip.contains(tag)) continue;
            var address = address(entry);
            for (long version : versions(entry)) {
                var expected = recorded(tag, version);
                var actual = host.archiveRepository().getEntry(operator, GetEntryRequest.newBuilder().setAddress(address).setVersion(version).build());
                checks.require(actual.getRenditionsList().equals(expected.getRenditionsList()), phase + ".entry." + tag + ".v" + version + ".bytes",
                        "renditions=" + actual.getRenditionsCount() + " byte-equal to the recorded read");
                checks.require(actual.getManifest().equals(expected.getManifest()) && actual.getManifest().getVersion() == version
                        && actual.getManifest().getMetadataSnapshot().getCurrentVersion() == version
                        && actual.getManifest().getMetadataSnapshot().getAddress().equals(address),
                        phase + ".entry." + tag + ".v" + version + ".manifest", "manifest equal; frozen snapshot title=" + actual.getManifest().getMetadataSnapshot().getTitle());
                long present = actual.getManifest().getRenditionsList().stream().filter(item -> item.getState() == RenditionState.RENDITION_STATE_PRESENT).count();
                checks.require(present == actual.getRenditionsCount(), phase + ".entry." + tag + ".v" + version + ".present",
                        "every PRESENT manifest rendition delivered bytes: " + present);
                if (currentMetadata) checks.require(actual.getInfo().equals(expected.getInfo()), phase + ".entry." + tag + ".v" + version + ".info",
                        "current metadata equal: title=" + actual.getInfo().getTitle() + " currentVersion=" + actual.getInfo().getCurrentVersion()
                        + " classification=" + actual.getInfo().getClassification().getState());
            }
        }
    }

    /** The retained version listing, newest first; exact, or the recorded manifests inside a longer listing. */
    void verifyVersions(String phase, boolean exact) {
        var operator = ArchiveBackupQualificationFixture.operator();
        for (var entry : entries()) {
            String tag = RepositoryBackupRehearsalJson.string(entry, "tag");
            ListVersionsResponse expected;
            try { expected = ListVersionsResponse.parseFrom(read(identities, "versions-" + tag + ".pb")); }
            catch (InvalidProtocolBufferException failure) { throw new RepositoryBackupRehearsalFailure("Corrupt recorded listing", failure); }
            var actual = host.archiveRepository().listVersions(operator, ListVersionsRequest.newBuilder().setAddress(address(entry)).build());
            if (exact) checks.require(actual.equals(expected), phase + ".versions." + tag, "listing equal: " + actual.getVersionsList().stream().map(VersionManifest::getVersion).toList());
            else {
                var byVersion = new LinkedHashMap<Long, VersionManifest>();
                actual.getVersionsList().forEach(manifest -> byVersion.put(manifest.getVersion(), manifest));
                boolean retained = expected.getVersionsList().stream().allMatch(manifest -> manifest.equals(byVersion.get(manifest.getVersion())));
                checks.require(retained, phase + ".versions." + tag, "recorded manifests retained inside listing " + byVersion.keySet());
            }
        }
    }

    /** Exact ledger counters per archive, as recorded. */
    void verifyStats(String phase) {
        var operator = ArchiveBackupQualificationFixture.operator();
        for (var archive : RepositoryBackupRehearsalJson.list(record, "archives")) {
            String account = RepositoryBackupRehearsalJson.string(archive, "account"), name = RepositoryBackupRehearsalJson.string(archive, "name");
            GetArchiveStatsResponse expected;
            try { expected = GetArchiveStatsResponse.parseFrom(read(identities, "stats-" + account + "-" + name + ".pb")); }
            catch (InvalidProtocolBufferException failure) { throw new RepositoryBackupRehearsalFailure("Corrupt recorded stats", failure); }
            var actual = host.archiveRepository().stats(operator, GetArchiveStatsRequest.newBuilder().setAccountId(account).setArchive(name).build());
            checks.require(actual.equals(expected), phase + ".stats." + name + "." + account.substring(account.lastIndexOf('-') + 1),
                    "entries=" + actual.getStats().getEntries() + " versions=" + actual.getStats().getVersions() + " retainedBytes=" + actual.getStats().getRetainedBytes()
                    + " currentBytes=" + actual.getStats().getCurrentBytes());
        }
    }

    /**
     * Process authority decides archive access: the operator reads, every caller without
     * process authority is PERMISSION_DENIED on reads, writes and mutations regardless of
     * account membership, and the operator addressing an entry under the other account is
     * NOT_FOUND because identity is derived from the address.
     */
    Map<String, Object> verifyAccess(String phase) {
        var matrix = new LinkedHashMap<String, Object>();
        var accounts = RepositoryBackupRehearsalJson.list(record, "accountsDetail");
        var operator = ArchiveBackupQualificationFixture.operator();
        var pending = RepositoryBackupRehearsalJson.object(record, "pending");
        for (var entry : entries()) {
            String tag = RepositoryBackupRehearsalJson.string(entry, "tag");
            String account = RepositoryBackupRehearsalJson.string(entry, "account");
            var address = address(entry);
            long version = versions(entry).getLast();
            matrix.put("operator." + tag, attempt(operator, address, version));
            matrix.put("member." + tag, attempt(ArchiveBackupQualificationFixture.member(account), address, version));
            matrix.put("stranger." + tag, attempt(ArchiveBackupQualificationFixture.stranger(account), address, version));
            matrix.put("outsider." + tag, attempt(ArchiveBackupQualificationFixture.outsider(), address, version));
            matrix.put("member-write." + tag, outcome(() -> host.archiveRepository().putEntry(ArchiveBackupQualificationFixture.member(account),
                    PutEntryRequest.newBuilder().setAddress(address).setTitle("denied write").build())));
            matrix.put("member-mutation." + tag, outcome(() -> host.archiveMutationRepository().mutateArchive(ArchiveBackupQualificationFixture.member(account),
                    ArchiveMutationRequest.newBuilder().setOperationId(UUID.randomUUID().toString())
                            .setDeleteRendition(DeleteRenditionRequest.newBuilder().setAddress(address).setRendition("original").setReason("denied")).build())));
            for (var other : accounts) {
                String otherAccount = RepositoryBackupRehearsalJson.string(other, "account");
                if (otherAccount.equals(account)) continue;
                matrix.put("cross-account." + tag, attempt(operator, address.toBuilder().setAccountId(otherAccount).build(), version));
            }
        }
        for (var entry : entries()) {
            String tag = RepositoryBackupRehearsalJson.string(entry, "tag");
            checks.require("OK".equals(matrix.get("operator." + tag)), phase + ".access." + tag + ".operator", "process authority reads " + tag);
            for (String caller : List.of("member", "stranger", "outsider"))
                checks.require("PERMISSION_DENIED".equals(matrix.get(caller + "." + tag)), phase + ".access." + tag + "." + caller,
                        caller + " without process authority: " + matrix.get(caller + "." + tag));
            checks.require("PERMISSION_DENIED".equals(matrix.get("member-write." + tag)) && "PERMISSION_DENIED".equals(matrix.get("member-mutation." + tag)),
                    phase + ".access." + tag + ".member_writes", "put=" + matrix.get("member-write." + tag) + " mutation=" + matrix.get("member-mutation." + tag));
            checks.require("NOT_FOUND".equals(matrix.get("cross-account." + tag)), phase + ".access." + tag + ".cross_account",
                    "the same entry id under the other account: " + matrix.get("cross-account." + tag));
        }
        String pendingAccount = RepositoryBackupRehearsalJson.string(pending, "account");
        String operation = RepositoryBackupRehearsalJson.string(pending, "operationId");
        matrix.put("member-mutation-lookup", outcome(() -> host.archiveMutationRepository().getArchiveMutation(ArchiveBackupQualificationFixture.member(pendingAccount),
                GetArchiveMutationRequest.newBuilder().setAccountId(pendingAccount).setOperationId(operation).build())));
        for (var other : accounts) {
            String otherAccount = RepositoryBackupRehearsalJson.string(other, "account");
            if (otherAccount.equals(pendingAccount)) continue;
            matrix.put("cross-account-mutation-lookup", outcome(() -> host.archiveMutationRepository().getArchiveMutation(operator,
                    GetArchiveMutationRequest.newBuilder().setAccountId(otherAccount).setOperationId(operation).build())));
        }
        checks.require("PERMISSION_DENIED".equals(matrix.get("member-mutation-lookup")) && "NOT_FOUND".equals(matrix.get("cross-account-mutation-lookup")),
                phase + ".access.mutation_lookup", "member=" + matrix.get("member-mutation-lookup") + " other account=" + matrix.get("cross-account-mutation-lookup"));
        return matrix;
    }

    private String attempt(RepositoryCaller caller, EntryAddress address, long version) {
        return outcome(() -> host.archiveRepository().getEntry(caller, GetEntryRequest.newBuilder().setAddress(address).setVersion(version).build()));
    }

    /** The authenticated transport refuses callers without the operator token and serves the recorded entry and receipt with it. */
    void verifyTransport(String phase) throws Exception {
        String token = ArchiveBackupQualificationFixture.env(ArchiveBackupQualificationFixture.API_TOKEN_ENV);
        String name = "qualification-archive-" + UUID.randomUUID();
        var server = host.startInProcess(name, token, null);
        var channel = InProcessChannelBuilder.forName(name).build();
        try {
            var first = entries().getFirst();
            String tag = RepositoryBackupRehearsalJson.string(first, "tag");
            long version = versions(first).getFirst();
            var request = GetEntryRequest.newBuilder().setAddress(address(first)).setVersion(version).build();
            var stub = ArchiveServiceGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS);
            String unauthenticated;
            try { stub.getEntry(request); unauthenticated = "OK"; }
            catch (StatusRuntimeException failure) { unauthenticated = failure.getStatus().getCode().name(); }
            checks.require("UNAUTHENTICATED".equals(unauthenticated), phase + ".transport.unauthenticated", "status=" + unauthenticated);
            var headers = new Metadata();
            headers.put(Metadata.Key.of(API_TOKEN_HEADER, Metadata.ASCII_STRING_MARSHALLER), token);
            var response = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers)).getEntry(request);
            checks.require(response.equals(recorded(tag, version)), phase + ".transport.entry", "authenticated read of " + tag + " v" + version + " equals the recorded response");
            var pending = RepositoryBackupRehearsalJson.object(record, "pending");
            var recordedReceipt = ArchiveMutationReceipt.parseFrom(read(identities, "mutation-receipt-" + RepositoryBackupRehearsalJson.string(pending, "tag") + ".pb"));
            var lookup = ArchiveMutationServiceGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers)).getArchiveMutation(GetArchiveMutationRequest.newBuilder()
                            .setAccountId(RepositoryBackupRehearsalJson.string(pending, "account")).setOperationId(RepositoryBackupRehearsalJson.string(pending, "operationId")).build());
            checks.require(ArchiveBackupQualificationFixture.logical(lookup.getReceipt()).equals(ArchiveBackupQualificationFixture.logical(recordedReceipt)),
                    phase + ".transport.mutation_lookup", "authenticated receipt lookup carries the recorded logical admission; state=" + lookup.getReceipt().getState());
        } finally {
            channel.shutdownNow(); channel.awaitTermination(10, TimeUnit.SECONDS);
            server.shutdownNow(); server.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    /** The authenticated transport refuses the first recorded entry with exactly the library's code and message as status and description. */
    void verifyTransportRefusal(String phase, String expectedStatus, String expectedDescription) throws Exception {
        String token = ArchiveBackupQualificationFixture.env(ArchiveBackupQualificationFixture.API_TOKEN_ENV);
        String name = "qualification-archive-" + UUID.randomUUID();
        var server = host.startInProcess(name, token, null);
        var channel = InProcessChannelBuilder.forName(name).build();
        try {
            var first = entries().getFirst();
            String tag = RepositoryBackupRehearsalJson.string(first, "tag");
            long version = versions(first).getLast();
            var request = GetEntryRequest.newBuilder().setAddress(address(first)).setVersion(version).build();
            var headers = new Metadata();
            headers.put(Metadata.Key.of(API_TOKEN_HEADER, Metadata.ASCII_STRING_MARSHALLER), token);
            var stub = ArchiveServiceGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
            String status, description;
            try { stub.getEntry(request); status = "OK"; description = ""; }
            catch (StatusRuntimeException failure) { status = failure.getStatus().getCode().name(); description = String.valueOf(failure.getStatus().getDescription()); }
            checks.require(status.equals(expectedStatus) && description.equals(expectedDescription), phase + ".transport.read_refused",
                    "authenticated gRPC read of " + tag + " v" + version + " -> " + status + " \"" + description + "\" equals the library classification");
        } finally {
            channel.shutdownNow(); channel.awaitTermination(10, TimeUnit.SECONDS);
            server.shutdownNow(); server.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    /**
     * Replay through the production paths: an identical save is elided with the retained
     * manifest (no version lands), and the recorded mutation request returns its original
     * logical admission with the current physical observation.
     */
    void verifyReplay(String phase) throws Exception {
        var operator = ArchiveBackupQualificationFixture.operator();
        var replay = RepositoryBackupRehearsalJson.object(record, "replay");
        String tag = RepositoryBackupRehearsalJson.string(replay, "tag");
        long version = RepositoryBackupRehearsalJson.number(replay, "version");
        var request = PutEntryRequest.parseFrom(read(identities, "put-replay-" + tag + ".pb"));
        var response = host.archiveRepository().putEntry(operator, request);
        checks.require(response.getDeduplicated() && response.getVersion() == version && response.getManifest().equals(recordedManifest(tag, version)),
                phase + ".replay.put." + tag, "deduplicated=" + response.getDeduplicated() + " version=" + response.getVersion() + "; retained manifest returned, no version landed");
        var pending = RepositoryBackupRehearsalJson.object(record, "pending");
        String pendingTag = RepositoryBackupRehearsalJson.string(pending, "tag");
        var mutation = ArchiveMutationRequest.parseFrom(read(identities, "mutation-request-" + pendingTag + ".pb"));
        var recordedReceipt = ArchiveMutationReceipt.parseFrom(read(identities, "mutation-receipt-" + pendingTag + ".pb"));
        var replayed = host.archiveMutationRepository().mutateArchive(operator, mutation);
        checks.require(ArchiveBackupQualificationFixture.logical(replayed).equals(ArchiveBackupQualificationFixture.logical(recordedReceipt))
                && replayed.getCommandSha256().equals(RepositoryBackupRehearsalJson.string(pending, "commandSha256")),
                phase + ".replay.mutation." + pendingTag, "same operation id and command: logical admission equal, state=" + replayed.getState()
                        + " pending=" + replayed.getObjectsPending() + " statusRevision=" + replayed.getStatusRevision());
    }

    /** Poll the production observation until the predicate holds or the deadline passes; a timeout fails the check. */
    ArchiveMutationReceipt awaitObservation(ArchiveMutationRepository mutations, String account, String operation, Predicate<ArchiveMutationReceipt> done,
            Duration deadline, String check, String detail) throws InterruptedException {
        var operator = ArchiveBackupQualificationFixture.operator();
        var lookup = GetArchiveMutationRequest.newBuilder().setAccountId(account).setOperationId(operation).build();
        long until = System.nanoTime() + deadline.toNanos();
        ArchiveMutationReceipt observed = mutations.getArchiveMutation(operator, lookup);
        while (!done.test(observed) && System.nanoTime() < until) {
            Thread.sleep(200);
            observed = mutations.getArchiveMutation(operator, lookup);
        }
        checks.require(done.test(observed), check, detail + ": state=" + observed.getState() + " pending=" + observed.getObjectsPending()
                + " confirmedAbsent=" + observed.getObjectsConfirmedAbsent() + (observed.hasErrorCode() ? " error=" + observed.getErrorCode() : "")
                + " statusRevision=" + observed.getStatusRevision());
        return observed;
    }

    /**
     * Provider identity by exact version id for every recorded binding: LIVE objects read with
     * the recorded SHA-256, ETag and size; DELETED objects (reclaimed cleanup targets) have no
     * version left under their key and the recorded version id no longer resolves.
     */
    static void verifyProviderObjects(RepositoryBackupRehearsalChecks checks, String phase, String endpoint, List<Map<String, Object>> rows) {
        try (var client = ArchiveBackupQualificationFixture.s3(endpoint)) {
            int live = 0, absent = 0;
            for (var row : rows) {
                String namespace = RepositoryBackupRehearsalJson.string(row, "namespace"), key = RepositoryBackupRehearsalJson.string(row, "key");
                String version = RepositoryBackupRehearsalJson.string(row, "providerVersion"), state = RepositoryBackupRehearsalJson.string(row, "state");
                String name = phase + ".provider." + RepositoryBackupRehearsalJson.sha256((namespace + "/" + key + "@" + version).getBytes()).substring(0, 12);
                if (state.equals("LIVE")) {
                    var response = client.getObjectAsBytes(builder -> builder.bucket(namespace).key(key).versionId(version));
                    String sha = ArchiveManifests.sha256Hex(response.asByteArray());
                    boolean match = sha.equals(RepositoryBackupRehearsalJson.string(row, "sha256")) && response.response().eTag().equals(RepositoryBackupRehearsalJson.string(row, "etag"))
                            && version.equals(response.response().versionId()) && response.asByteArray().length == RepositoryBackupRehearsalJson.number(row, "size");
                    checks.require(match, name, "LIVE bucket=" + namespace + " key=" + key + " version=" + version + " sha256=" + sha + " etag=" + response.response().eTag());
                    live++;
                } else if (state.equals("DELETED")) {
                    String outcome = outcome(() -> client.getObjectAsBytes(builder -> builder.bucket(namespace).key(key).versionId(version)));
                    long remaining = client.listObjectVersions(builder -> builder.bucket(namespace).prefix(key)).versions().stream()
                            .filter(candidate -> candidate.key().equals(key)).count();
                    checks.require((outcome.startsWith("NoSuchKeyException") || outcome.startsWith("NoSuchVersionException")) && remaining == 0, name,
                            "DELETED bucket=" + namespace + " key=" + key + ": recorded version " + version + " -> " + outcome + "; versions left under the key=" + remaining);
                    absent++;
                } else throw new RepositoryBackupRehearsalFailure("Unexpected upload state " + state + " for " + key);
            }
            checks.require(live + absent == rows.size() && live > 0, phase + ".provider.count", "objects=" + rows.size() + " live=" + live + " reclaimed=" + absent);
        }
    }

    /** The outcome class of an action: a RepositoryException code, or the exception type for anything else. */
    interface Action { void run() throws Exception; }
    static String outcome(Action action) {
        try { action.run(); return "OK"; }
        catch (RepositoryException failure) { return failure.code().name(); }
        catch (NoSuchKeyException failure) { return "NoSuchKeyException: " + failure.getMessage(); }
        catch (S3Exception failure) {
            String code = failure.awsErrorDetails() == null ? "" : failure.awsErrorDetails().errorCode();
            return ("NoSuchVersion".equals(code) ? "NoSuchVersionException" : failure.getClass().getSimpleName()) + ": " + failure.statusCode() + " " + code;
        }
        catch (Exception failure) { return failure.getClass().getSimpleName() + ": " + failure.getMessage(); }
    }

    /** The message of a domain failure, which the transport serves as its status description; "OK" when the action succeeds. */
    static String message(Action action) {
        try { action.run(); return "OK"; }
        catch (Exception failure) { return String.valueOf(failure.getMessage()); }
    }

    /** The message of the provider-boundary failure on the cause chain, or "none" when no BlobStoreException is present. */
    static String providerMessage(Action action) {
        try { action.run(); return "OK"; }
        catch (Exception failure) {
            for (Throwable cause = failure; cause != null; cause = cause.getCause())
                if (cause instanceof BlobStoreException provider) return String.valueOf(provider.getMessage());
            return "none";
        }
    }

    /** The provider-boundary cause chain of a domain failure, for recording how a refusal was classified. */
    static String causes(Action action) {
        try { action.run(); return "OK"; }
        catch (Exception failure) {
            var text = new StringBuilder();
            for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
                if (!text.isEmpty()) text.append(" <- ");
                text.append(cause.getClass().getSimpleName());
                if (cause instanceof RepositoryException domain) text.append('(').append(domain.code()).append(')');
                if (cause instanceof BlobStoreException provider) text.append('(').append(provider.code()).append(')');
            }
            return text.toString();
        }
    }
}
