package ai.protomolt.proto.repo.recovery;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.repo.archive.v1.*;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.service.RepoServices;
import ai.protomolt.proto.repo.spi.HistoricalMaterializationRepository;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The same observations before shutdown and after restore. Every comparison is against the
 * identities the seed host recorded from real responses and the ledger, never against a
 * value this class computes on its own.
 */
final class ContentChecks {
    private final RepoServices host;
    private final Checks checks;
    private final Path identities;
    private final Map<String, Object> record;
    private final String account;
    private final NodeAddress address;

    ContentChecks(RepoServices host, Checks checks, Path identities, Map<String, Object> record) {
        this.host = host; this.checks = checks; this.identities = identities; this.record = record;
        this.account = Json.string(record, "account");
        this.address = RehearsalFixture.address(account);
    }

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> revisions() { return (List<Map<String, Object>>) record.get("revisions"); }

    static byte[] read(Path directory, String name) {
        try { return Files.readAllBytes(directory.resolve(name)); }
        catch (IOException failure) { throw new RehearsalFailure("Cannot read " + name, failure); }
    }

    /** Raw fragments and validated documents of every recorded revision, with command, policy and metadata bindings. */
    void verifyHistory(String phase) throws Exception {
        var member = RehearsalFixture.member(account);
        for (var revision : revisions()) {
            String tag = Json.string(revision, "tag");
            var id = UUID.fromString(Json.string(revision, "revisionId"));
            var expectedDocument = Document.parseFrom(read(identities, "document-" + tag + ".pb"));
            var fragments = (List<Map<String, Object>>) revision.get("rawFragments");
            try (var raw = host.historicalRepository().readRaw(member, address, id, RepositoryReadControl.NONE)) {
                checks.require(raw.fragments().size() == fragments.size(), phase + ".raw." + tag + ".fragment_count",
                        "fragments=" + raw.fragments().size() + " expected=" + fragments.size());
                for (int i = 0; i < fragments.size(); i++) {
                    var fragment = raw.fragments().get(i);
                    var bytes = new byte[fragment.bytes().remaining()]; fragment.bytes().get(bytes);
                    String sha = DocumentPartCodec.sha256Hex(bytes);
                    checks.require(fragment.revisionOrdinal() == Json.number(fragments.get(i), "revisionOrdinal")
                            && sha.equals(Json.string(fragments.get(i), "sha256")) && bytes.length == Json.number(fragments.get(i), "size"),
                            phase + ".raw." + tag + ".fragment" + i, "ordinal=" + fragment.revisionOrdinal() + " sha256=" + sha + " size=" + bytes.length);
                }
                checks.require(raw.publicationRevision() == Json.number(revision, "mutationRevision"), phase + ".raw." + tag + ".publication_revision",
                        "publicationRevision=" + raw.publicationRevision());
                raw.authorizeDelivery(RepositoryReadControl.NONE);
            }
            try (var validated = host.historicalRepository().readValidated(member, address, id, RepositoryReadControl.NONE)) {
                checks.require(validated.document().equals(expectedDocument), phase + ".validated." + tag + ".document",
                        "document bytes equal recorded, sha256=" + DocumentPartCodec.sha256Hex(validated.document().toByteArray()));
                String command = DocumentPartCodec.sha256Hex(validated.commandSha256().toByteArray());
                checks.require(command.equals(Json.string(revision, "commandSha256Sha256")) && validated.policySha256().equals(Json.string(revision, "policySha256"))
                        && validated.validationProfile().equals(Json.string(revision, "validationProfile")),
                        phase + ".validated." + tag + ".bindings", "policy=" + validated.policySha256() + " profile=" + validated.validationProfile());
                checks.require(DocumentPartCodec.sha256Hex(validated.metadata().toByteArray()).equals(Json.string(revision, "metadataSha256"))
                        && DocumentPartCodec.sha256Hex(validated.manifest().toByteArray()).equals(Json.string(revision, "manifestSha256"))
                        && validated.metadata().hasKnown(), phase + ".validated." + tag + ".metadata", "recorded metadata and manifest snapshot");
                validated.authorizeDelivery(RepositoryReadControl.NONE);
            }
        }
    }

    /** Typed occurrence decoded only from the retained descriptor artifact; the artifact digest is the recorded one. */
    void verifyMaterialization(String phase) throws Exception {
        var member = RehearsalFixture.member(account);
        var selection = Json.object(record, "materialization");
        var first = revisions().getFirst();
        var id = UUID.fromString(Json.string(first, "revisionId"));
        var selector = new HistoricalMaterializationRepository.Selection((int) Json.number(selection, "revisionOrdinal"),
                Json.string(selection, "rootSha256"), Json.string(selection, "pathSha256"));
        var limits = new HistoricalMaterializationRepository.Limits(4_000_000, 4_000_000, 16_000_000, 64, 8_000_000, 64);
        try (var result = host.historicalMaterializationRepository().readMaterialized(member, address, id, selector, limits, RepositoryReadControl.NONE)) {
            var view = result.view(RepositoryReadControl.NONE);
            String artifact = Json.string(selection, "artifactSha256");
            checks.require(view.schema().getArtifactSha256().equals(artifact)
                    && DocumentPartCodec.sha256Hex(view.definition().descriptorArtifact().toByteArray()).equals(artifact)
                    && view.definition().reference().getDescriptorSha256().equals(artifact),
                    phase + ".materialized.descriptor_identity", "retained descriptor artifact sha256=" + artifact);
            var tool = view.definition().metadata().getCompilation().getAdmissionRuntime();
            checks.require(tool.getName().equals(Json.string(Json.object(record, "toolIdentity"), "name"))
                    && tool.getVersion().equals(Json.string(Json.object(record, "toolIdentity"), "version"))
                    && DocumentPartCodec.sha256Hex(view.definition().metadataArtifact().toByteArray()).equals(view.definition().reference().getMetadataSha256()),
                    phase + ".materialized.tool_identity", "admission runtime " + tool.getName() + "/" + tool.getVersion());
            var files = ClosedDescriptorSet.load(view.definition().descriptorArtifact(), new ClosedDescriptorSet.Limits(16_000_000, 256, 4096, 64));
            var type = files.stream().flatMap(file -> file.getMessageTypes().stream())
                    .filter(candidate -> candidate.getFullName().equals(RehearsalFixture.TYPE_NAME)).findFirst().orElseThrow();
            boolean imports = files.stream().anyMatch(file -> file.getName().equals("google/protobuf/timestamp.proto"))
                    && type.getFile().getDependencies().size() >= 2;
            var offline = DynamicMessage.parseFrom(type, view.original().getValue());
            String label = (String) offline.getField(type.findFieldByName("label"));
            checks.require(imports && label.equals(Json.string(selection, "label")) && view.value().getDescriptorForType().getFullName().equals(RehearsalFixture.TYPE_NAME),
                    phase + ".materialized.offline_decode", "label=" + label + " files=" + files.size() + " imports retained");
            checks.require(view.selection().equals(selector) && view.occurrence().revisionOrdinal() == selector.revisionOrdinal(),
                    phase + ".materialized.selection_identity", "ordinal=" + selector.revisionOrdinal());
        }
    }

    /** Archive versions, frozen metadata snapshots and the shared rendition object. */
    void verifyArchive(String phase) throws Exception {
        var archive = Json.object(record, "archive");
        var entry = EntryAddress.newBuilder().setAccountId(account).setArchive(Json.string(archive, "name")).setEntryId(Json.string(archive, "entryId")).build();
        var operator = RehearsalFixture.operator();
        for (long version : List.of(1L, 2L)) {
            var expected = GetEntryResponse.parseFrom(read(identities, "archive-entry-v" + version + ".pb"));
            var actual = host.archiveRepository().getEntry(operator, GetEntryRequest.newBuilder().setAddress(entry).setVersion(version).build());
            checks.require(actual.getRenditionsList().equals(expected.getRenditionsList()), phase + ".archive.v" + version + ".bytes",
                    "renditions=" + actual.getRenditionsCount() + " byte-equal to recorded");
            checks.require(actual.getManifest().equals(expected.getManifest()) && actual.getManifest().getMetadataSnapshot().getCurrentVersion() == version,
                    phase + ".archive.v" + version + ".manifest", "frozen metadata snapshot title=" + actual.getManifest().getMetadataSnapshot().getTitle());
        }
        var versions = host.archiveRepository().listVersions(operator, ListVersionsRequest.newBuilder().setAddress(entry).build());
        var listed = versions.getVersionsList().stream().map(VersionManifest::getVersion).toList();
        checks.require(listed.equals(List.of(2L, 1L)) || listed.equals(List.of(3L, 2L, 1L)), phase + ".archive.versions", "versions=" + listed);
        var manifest1 = VersionManifest.parseFrom(read(identities, "archive-manifest-v1.pb"));
        var manifest2 = VersionManifest.parseFrom(read(identities, "archive-manifest-v2.pb"));
        checks.require(rendition(manifest1, "original").getObjectKey().equals(rendition(manifest2, "original").getObjectKey())
                && rendition(manifest1, "original").getObjectKey().equals(Json.string(archive, "sharedObjectKey"))
                && !rendition(manifest1, "markdown").getObjectKey().equals(rendition(manifest2, "markdown").getObjectKey()),
                phase + ".archive.shared_object", "shared key=" + Json.string(archive, "sharedObjectKey"));
        var stats = host.archiveRepository().stats(operator, GetArchiveStatsRequest.newBuilder().setAccountId(account).setArchive(Json.string(archive, "name")).build()).getStats();
        checks.require(stats.getEntries() == 1 && stats.getVersions() >= 2 && stats.getRetainedBytes() == Json.number(archive, "retainedBytes") + (stats.getVersions() - 2) * Json.number(archive, "v3Bytes"),
                phase + ".archive.stats", "entries=" + stats.getEntries() + " versions=" + stats.getVersions() + " retainedBytes=" + stats.getRetainedBytes());
    }

    static RenditionManifestEntry rendition(VersionManifest manifest, String name) {
        return manifest.getRenditionsList().stream().filter(entry -> entry.getRendition().getName().equals(name)).findFirst().orElseThrow();
    }

    /** Exact receipt replay of the recorded publication requests. */
    void verifyReplay(String phase) throws Exception {
        for (var revision : revisions()) {
            String tag = Json.string(revision, "tag");
            var request = PublishDocumentRequest.parseFrom(read(identities, "request-" + tag + ".pb"));
            var receipt = PublishDocumentResponse.parseFrom(read(identities, "receipt-" + tag + ".pb"));
            var replay = host.publicationRepository().publishDocument(RehearsalFixture.operator(), request, RepositoryReadControl.NONE);
            checks.require(replay.equals(receipt), phase + ".replay." + tag, "receipt byte-equal, revision=" + Json.string(revision, "revisionId"));
        }
        var archive = Json.object(record, "archive");
        var entry = EntryAddress.newBuilder().setAccountId(account).setArchive(Json.string(archive, "name")).setEntryId(Json.string(archive, "entryId")).build();
        var manifest2 = VersionManifest.parseFrom(read(identities, "archive-manifest-v2.pb"));
        var again = host.archiveRepository().putEntry(RehearsalFixture.operator(), PutEntryRequest.newBuilder().setAddress(entry)
                .setTitle("Current label").addRenditions(RenditionContent.newBuilder().setRendition(RenditionDescriptor.newBuilder()
                        .setName("markdown").setMediaType("text/markdown")).setData(ByteString.copyFromUtf8(Json.string(archive, "markdownV2")))).build());
        checks.require(again.getDeduplicated() && again.getVersion() == 2 && again.getManifest().equals(manifest2), phase + ".replay.archive_dedup",
                "deduplicated=" + again.getDeduplicated() + " version=" + again.getVersion());
    }

    /** Stored ownership and the revoked public grant decide access; no default process authority for clients. */
    Map<String, Object> verifyAccess(String phase) {
        var matrix = new LinkedHashMap<String, Object>();
        var first = UUID.fromString(Json.string(revisions().get(0), "revisionId"));
        var second = UUID.fromString(Json.string(revisions().get(1), "revisionId"));
        matrix.put("member.v1", attempt(RehearsalFixture.member(account), first));
        matrix.put("member.v2", attempt(RehearsalFixture.member(account), second));
        matrix.put("stranger.v1", attempt(RehearsalFixture.stranger(account), first));
        matrix.put("stranger.v2", attempt(RehearsalFixture.stranger(account), second));
        matrix.put("outsider.v1", attempt(RehearsalFixture.outsider(), first));
        matrix.put("outsider.v2", attempt(RehearsalFixture.outsider(), second));
        checks.require("OK".equals(matrix.get("member.v1")) && "OK".equals(matrix.get("member.v2")), phase + ".access.member", "member reads both revisions");
        checks.require("NOT_FOUND".equals(matrix.get("stranger.v1")) && "NOT_FOUND".equals(matrix.get("stranger.v2")), phase + ".access.revoked_public",
                "public grant revoked by revision 2 applies to revision 1: " + matrix.get("stranger.v1"));
        checks.require("NOT_FOUND".equals(matrix.get("outsider.v1")) && "NOT_FOUND".equals(matrix.get("outsider.v2")), phase + ".access.outsider", "foreign account denied");
        return matrix;
    }

    private String attempt(RepositoryCaller caller, UUID revision) {
        try (var read = host.historicalRepository().readValidated(caller, address, revision, RepositoryReadControl.NONE)) {
            read.authorizeDelivery(RepositoryReadControl.NONE);
            return "OK";
        } catch (RepositoryException denied) { return denied.code().name(); }
    }

    /** The authenticated transport refuses callers without the operator token and serves the recorded document with it. */
    void verifyTransport(String phase) throws Exception {
        String token = RehearsalFixture.env(RehearsalFixture.API_TOKEN_ENV);
        String name = "recovery-history-" + UUID.randomUUID();
        var server = host.startInProcess(name, token, null);
        var channel = io.grpc.inprocess.InProcessChannelBuilder.forName(name).build();
        try {
            var first = revisions().getFirst();
            var request = ReadRevisionRequest.newBuilder().setAddress(address).setRevisionId(Json.string(first, "revisionId"))
                    .setMode(HistoricalDocumentReadMode.HISTORICAL_DOCUMENT_READ_MODE_VALIDATED).build();
            var stub = DocumentHistoryServiceGrpc.newBlockingStub(channel).withDeadlineAfter(20, java.util.concurrent.TimeUnit.SECONDS);
            String unauthenticated;
            try { stub.readRevision(request); unauthenticated = "OK"; }
            catch (io.grpc.StatusRuntimeException failure) { unauthenticated = failure.getStatus().getCode().name(); }
            checks.require("UNAUTHENTICATED".equals(unauthenticated), phase + ".transport.unauthenticated", "status=" + unauthenticated);
            var headers = new io.grpc.Metadata();
            headers.put(io.grpc.Metadata.Key.of("api_token", io.grpc.Metadata.ASCII_STRING_MARSHALLER), token);
            var response = stub.withInterceptors(io.grpc.stub.MetadataUtils.newAttachHeadersInterceptor(headers)).readRevision(request);
            var expected = Document.parseFrom(read(identities, "document-" + Json.string(first, "tag") + ".pb"));
            checks.require(response.hasValidated() && response.getValidated().getDocument().equals(expected), phase + ".transport.validated",
                    "authenticated validated read equals recorded document");
        } finally {
            channel.shutdownNow(); channel.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS);
            server.shutdownNow(); server.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    /** Provider bytes and version identities of every recorded object, read directly with the recorded version id. */
    static void verifyProviderObjects(Checks checks, String phase, String endpoint, List<Map<String, Object>> objects) {
        try (var client = software.amazon.awssdk.services.s3.S3Client.builder().endpointOverride(java.net.URI.create(endpoint))
                .region(software.amazon.awssdk.regions.Region.of(RehearsalFixture.env("PROTOMOLT_RECOVERY_S3_REGION"))).forcePathStyle(true)
                .httpClientBuilder(software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient.builder())
                .credentialsProvider(software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                        software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(
                                RehearsalFixture.env("PROTOMOLT_RECOVERY_S3_ACCESS"), RehearsalFixture.env("PROTOMOLT_RECOVERY_S3_SECRET")))).build()) {
            var seen = new ArrayList<String>();
            for (var object : objects) {
                String namespace = Json.string(object, "namespace"), key = Json.string(object, "key"), version = Json.string(object, "providerVersion");
                var response = client.getObjectAsBytes(builder -> builder.bucket(namespace).key(key).versionId(version));
                String sha = DocumentPartCodec.sha256Hex(response.asByteArray());
                boolean match = sha.equals(Json.string(object, "sha256")) && response.response().eTag().equals(Json.string(object, "etag"))
                        && version.equals(response.response().versionId()) && response.asByteArray().length == Json.number(object, "size");
                checks.require(match, phase + ".provider." + Digests.sha256((namespace + "/" + key + "@" + version).getBytes()).substring(0, 12),
                        "bucket=" + namespace + " key=" + key + " version=" + version + " sha256=" + sha + " etag=" + response.response().eTag());
                seen.add(namespace + "/" + key + "@" + version);
            }
            checks.require(seen.size() == objects.size() && !seen.isEmpty(), phase + ".provider.count", "objects=" + seen.size());
        }
    }
}
