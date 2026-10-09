package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
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
 * The same observations before capture and after restore. Every comparison is against the
 * identities the seed host recorded from real responses and the ledger, never against a
 * value this class computes on its own. Adapted from GitHub PR #413 (4862f35f9) and extended
 * with a second account, nested Any occurrences and cross-account denial.
 */
final class RepositoryBackupRehearsalContentChecks {
    private final RepoServices host;
    private final RepositoryBackupRehearsalChecks checks;
    private final Path identities;
    private final Map<String, Object> record;

    RepositoryBackupRehearsalContentChecks(RepoServices host, RepositoryBackupRehearsalChecks checks, Path identities, Map<String, Object> record) {
        this.host = host; this.checks = checks; this.identities = identities; this.record = record;
    }

    List<Map<String, Object>> revisions() { return RepositoryBackupRehearsalJson.list(record, "revisions"); }

    static byte[] read(Path directory, String name) {
        try { return Files.readAllBytes(directory.resolve(name)); }
        catch (IOException failure) { throw new RepositoryBackupRehearsalFailure("Cannot read " + name, failure); }
    }

    private static NodeAddress address(Map<String, Object> revision) {
        return RepositoryBackupRehearsalFixture.address(RepositoryBackupRehearsalJson.string(revision, "account"));
    }

    /** Raw fragments and validated documents of every recorded revision, with command, policy and metadata bindings. */
    void verifyHistory(String phase) throws Exception {
        for (var revision : revisions()) {
            String tag = RepositoryBackupRehearsalJson.string(revision, "tag");
            String account = RepositoryBackupRehearsalJson.string(revision, "account");
            var member = RepositoryBackupRehearsalFixture.member(account);
            var id = UUID.fromString(RepositoryBackupRehearsalJson.string(revision, "revisionId"));
            var expectedDocument = Document.parseFrom(read(identities, "document-" + tag + ".pb"));
            var fragments = RepositoryBackupRehearsalJson.list(revision, "rawFragments");
            try (var raw = host.historicalRepository().readRaw(member, address(revision), id, RepositoryReadControl.NONE)) {
                checks.require(raw.fragments().size() == fragments.size(), phase + ".raw." + tag + ".fragment_count",
                        "fragments=" + raw.fragments().size() + " expected=" + fragments.size());
                for (int i = 0; i < fragments.size(); i++) {
                    var fragment = raw.fragments().get(i);
                    var bytes = new byte[fragment.bytes().remaining()]; fragment.bytes().get(bytes);
                    String sha = DocumentPartCodec.sha256Hex(bytes);
                    checks.require(fragment.revisionOrdinal() == RepositoryBackupRehearsalJson.number(fragments.get(i), "revisionOrdinal")
                            && sha.equals(RepositoryBackupRehearsalJson.string(fragments.get(i), "sha256")) && bytes.length == RepositoryBackupRehearsalJson.number(fragments.get(i), "size"),
                            phase + ".raw." + tag + ".fragment" + i, "ordinal=" + fragment.revisionOrdinal() + " sha256=" + sha + " size=" + bytes.length);
                }
                checks.require(raw.publicationRevision() == RepositoryBackupRehearsalJson.number(revision, "mutationRevision"), phase + ".raw." + tag + ".publication_revision",
                        "publicationRevision=" + raw.publicationRevision());
                raw.authorizeDelivery(RepositoryReadControl.NONE);
            }
            try (var validated = host.historicalRepository().readValidated(member, address(revision), id, RepositoryReadControl.NONE)) {
                checks.require(validated.document().equals(expectedDocument), phase + ".validated." + tag + ".document",
                        "document bytes equal recorded, sha256=" + DocumentPartCodec.sha256Hex(validated.document().toByteArray()));
                String command = DocumentPartCodec.sha256Hex(validated.commandSha256().toByteArray());
                checks.require(command.equals(RepositoryBackupRehearsalJson.string(revision, "commandSha256Sha256"))
                        && validated.policySha256().equals(RepositoryBackupRehearsalJson.string(revision, "policySha256"))
                        && validated.validationProfile().equals(RepositoryBackupRehearsalJson.string(revision, "validationProfile")),
                        phase + ".validated." + tag + ".bindings", "policy=" + validated.policySha256() + " profile=" + validated.validationProfile());
                checks.require(DocumentPartCodec.sha256Hex(validated.metadata().toByteArray()).equals(RepositoryBackupRehearsalJson.string(revision, "metadataSha256"))
                        && DocumentPartCodec.sha256Hex(validated.manifest().toByteArray()).equals(RepositoryBackupRehearsalJson.string(revision, "manifestSha256")),
                        phase + ".validated." + tag + ".metadata", "recorded metadata and manifest snapshot");
                validated.authorizeDelivery(RepositoryReadControl.NONE);
            }
        }
    }

    /** Every recorded occurrence (root and nested Any) decoded only from its retained descriptor artifact. */
    void verifyMaterialization(String phase) throws Exception {
        for (var revision : revisions()) {
            String tag = RepositoryBackupRehearsalJson.string(revision, "tag");
            String account = RepositoryBackupRehearsalJson.string(revision, "account");
            var member = RepositoryBackupRehearsalFixture.member(account);
            var id = UUID.fromString(RepositoryBackupRehearsalJson.string(revision, "revisionId"));
            for (var selection : RepositoryBackupRehearsalJson.list(revision, "selections")) {
                String typeUrl = RepositoryBackupRehearsalJson.string(selection, "typeUrl");
                String kind = typeUrl.endsWith(RepositoryBackupRehearsalFixture.ATTACHMENT_TYPE) ? "nested" : "root";
                var selector = new HistoricalMaterializationRepository.Selection((int) RepositoryBackupRehearsalJson.number(selection, "revisionOrdinal"),
                        RepositoryBackupRehearsalJson.string(selection, "rootSha256"), RepositoryBackupRehearsalJson.string(selection, "pathSha256"));
                try (var result = host.historicalMaterializationRepository().readMaterialized(member, address(revision), id, selector,
                        RepositoryBackupRehearsalFixture.LIMITS, RepositoryReadControl.NONE)) {
                    var view = result.view(RepositoryReadControl.NONE);
                    String artifact = RepositoryBackupRehearsalJson.string(selection, "artifactSha256");
                    checks.require(view.schema().getArtifactSha256().equals(artifact)
                            && DocumentPartCodec.sha256Hex(view.definition().descriptorArtifact().toByteArray()).equals(artifact)
                            && view.definition().reference().getDescriptorSha256().equals(artifact)
                            && view.definition().metadata().getTypeUrl().equals(typeUrl),
                            phase + ".materialized." + tag + "." + kind + ".descriptor_identity", "retained descriptor artifact sha256=" + artifact + " type=" + typeUrl);
                    var tool = view.definition().metadata().getCompilation().getAdmissionRuntime();
                    checks.require(tool.getName().equals("repository-backup-rehearsal") && tool.getVersion().equals("1")
                            && DocumentPartCodec.sha256Hex(view.definition().metadataArtifact().toByteArray()).equals(view.definition().reference().getMetadataSha256()),
                            phase + ".materialized." + tag + "." + kind + ".tool_identity", "admission runtime " + tool.getName() + "/" + tool.getVersion());
                    var files = ClosedDescriptorSet.load(view.definition().descriptorArtifact(), new ClosedDescriptorSet.Limits(16_000_000, 256, 4096, 64));
                    String typeName = typeUrl.substring(typeUrl.lastIndexOf('/') + 1);
                    var type = files.stream().flatMap(file -> file.getMessageTypes().stream())
                            .filter(candidate -> candidate.getFullName().equals(typeName)).findFirst().orElseThrow();
                    var offline = DynamicMessage.parseFrom(type, view.original().getValue());
                    String expected = RepositoryBackupRehearsalJson.string(selection, "expectedField");
                    String decoded = kind.equals("nested") ? (String) offline.getField(type.findFieldByName("name"))
                            : (String) offline.getField(type.findFieldByName("label"));
                    boolean imports = !kind.equals("root") || files.stream().anyMatch(file -> file.getName().equals("google/protobuf/timestamp.proto"));
                    checks.require(imports && decoded.equals(expected) && view.value().getDescriptorForType().getFullName().equals(typeName)
                            && view.path().getStepsCount() == RepositoryBackupRehearsalJson.number(selection, "steps"),
                            phase + ".materialized." + tag + "." + kind + ".offline_decode", "decoded=" + decoded + " files=" + files.size()
                                    + " steps=" + view.path().getStepsCount());
                    checks.require(view.selection().equals(selector) && view.occurrence().revisionOrdinal() == selector.revisionOrdinal(),
                            phase + ".materialized." + tag + "." + kind + ".selection_identity", "ordinal=" + selector.revisionOrdinal());
                }
            }
        }
    }

    /** Exact receipt replay of the recorded publication requests, without provider or registry access. */
    void verifyReplay(String phase) throws Exception {
        for (var revision : revisions()) {
            String tag = RepositoryBackupRehearsalJson.string(revision, "tag");
            var request = PublishDocumentRequest.parseFrom(read(identities, "request-" + tag + ".pb"));
            var receipt = PublishDocumentResponse.parseFrom(read(identities, "receipt-" + tag + ".pb"));
            var replay = host.publicationRepository().publishDocument(RepositoryBackupRehearsalFixture.operator(), request, RepositoryReadControl.NONE);
            checks.require(replay.equals(receipt), phase + ".replay." + tag, "receipt byte-equal, revision=" + RepositoryBackupRehearsalJson.string(revision, "revisionId"));
        }
    }

    /** Stored ownership decides access: the revoked public grant, a foreign account and a cross-account member are all denied. */
    Map<String, Object> verifyAccess(String phase) {
        var matrix = new LinkedHashMap<String, Object>();
        var accounts = new ArrayList<String>();
        for (var revision : revisions()) {
            String account = RepositoryBackupRehearsalJson.string(revision, "account");
            if (!accounts.contains(account)) accounts.add(account);
        }
        for (var revision : revisions()) {
            String tag = RepositoryBackupRehearsalJson.string(revision, "tag");
            String account = RepositoryBackupRehearsalJson.string(revision, "account");
            var id = UUID.fromString(RepositoryBackupRehearsalJson.string(revision, "revisionId"));
            matrix.put("member." + tag, attempt(RepositoryBackupRehearsalFixture.member(account), address(revision), id));
            matrix.put("stranger." + tag, attempt(RepositoryBackupRehearsalFixture.stranger(account), address(revision), id));
            matrix.put("outsider." + tag, attempt(RepositoryBackupRehearsalFixture.outsider(), address(revision), id));
            for (String other : accounts) if (!other.equals(account))
                matrix.put("cross-account-member." + tag, attempt(RepositoryBackupRehearsalFixture.member(other), address(revision), id));
        }
        for (var revision : revisions()) {
            String tag = RepositoryBackupRehearsalJson.string(revision, "tag");
            checks.require("OK".equals(matrix.get("member." + tag)), phase + ".access." + tag + ".member", "granted member reads " + tag);
            checks.require("NOT_FOUND".equals(matrix.get("stranger." + tag)), phase + ".access." + tag + ".stranger",
                    "same-account caller without the granted identity: " + matrix.get("stranger." + tag));
            checks.require("NOT_FOUND".equals(matrix.get("outsider." + tag)), phase + ".access." + tag + ".outsider", "foreign account denied");
            checks.require("NOT_FOUND".equals(matrix.get("cross-account-member." + tag)), phase + ".access." + tag + ".cross_account",
                    "the other account's member denied: " + matrix.get("cross-account-member." + tag));
        }
        return matrix;
    }

    private String attempt(RepositoryCaller caller, NodeAddress address, UUID revision) {
        try (var read = host.historicalRepository().readValidated(caller, address, revision, RepositoryReadControl.NONE)) {
            read.authorizeDelivery(RepositoryReadControl.NONE);
            return "OK";
        } catch (RepositoryException denied) { return denied.code().name(); }
    }

    /** The authenticated transport refuses callers without the operator token and serves the recorded document with it. */
    void verifyTransport(String phase) throws Exception {
        String token = RepositoryBackupRehearsalFixture.env(RepositoryBackupRehearsalFixture.API_TOKEN_ENV);
        String name = "rehearsal-history-" + UUID.randomUUID();
        var server = host.startInProcess(name, token, null);
        var channel = io.grpc.inprocess.InProcessChannelBuilder.forName(name).build();
        try {
            var first = revisions().getFirst();
            var request = ReadRevisionRequest.newBuilder().setAddress(address(first)).setRevisionId(RepositoryBackupRehearsalJson.string(first, "revisionId"))
                    .setMode(HistoricalDocumentReadMode.HISTORICAL_DOCUMENT_READ_MODE_VALIDATED).build();
            var stub = DocumentHistoryServiceGrpc.newBlockingStub(channel).withDeadlineAfter(20, java.util.concurrent.TimeUnit.SECONDS);
            String unauthenticated;
            try { stub.readRevision(request); unauthenticated = "OK"; }
            catch (io.grpc.StatusRuntimeException failure) { unauthenticated = failure.getStatus().getCode().name(); }
            checks.require("UNAUTHENTICATED".equals(unauthenticated), phase + ".transport.unauthenticated", "status=" + unauthenticated);
            var headers = new io.grpc.Metadata();
            headers.put(io.grpc.Metadata.Key.of("api_token", io.grpc.Metadata.ASCII_STRING_MARSHALLER), token);
            var response = stub.withInterceptors(io.grpc.stub.MetadataUtils.newAttachHeadersInterceptor(headers)).readRevision(request);
            var expected = Document.parseFrom(read(identities, "document-" + RepositoryBackupRehearsalJson.string(first, "tag") + ".pb"));
            checks.require(response.hasValidated() && response.getValidated().getDocument().equals(expected), phase + ".transport.validated",
                    "authenticated validated read equals recorded document");
        } finally {
            channel.shutdownNow(); channel.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS);
            server.shutdownNow(); server.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    /** Provider bytes and version identities of every recorded object, read directly with the recorded version id. */
    static void verifyProviderObjects(RepositoryBackupRehearsalChecks checks, String phase, String endpoint, List<Map<String, Object>> objects) {
        try (var client = RepositoryBackupRehearsalFixture.s3(endpoint)) {
            int seen = 0;
            for (var object : objects) {
                String namespace = RepositoryBackupRehearsalJson.string(object, "namespace"), key = RepositoryBackupRehearsalJson.string(object, "key"),
                        version = RepositoryBackupRehearsalJson.string(object, "providerVersion");
                var response = client.getObjectAsBytes(builder -> builder.bucket(namespace).key(key).versionId(version));
                String sha = DocumentPartCodec.sha256Hex(response.asByteArray());
                boolean match = sha.equals(RepositoryBackupRehearsalJson.string(object, "sha256")) && response.response().eTag().equals(RepositoryBackupRehearsalJson.string(object, "etag"))
                        && version.equals(response.response().versionId()) && response.asByteArray().length == RepositoryBackupRehearsalJson.number(object, "size");
                checks.require(match, phase + ".provider." + RepositoryBackupRehearsalJson.sha256((namespace + "/" + key + "@" + version).getBytes()).substring(0, 12),
                        "bucket=" + namespace + " key=" + key + " version=" + version + " sha256=" + sha + " etag=" + response.response().eTag());
                seen++;
            }
            checks.require(seen == objects.size() && seen > 0, phase + ".provider.count", "objects=" + seen);
        }
    }

    /** The outcome class of an action: a RepositoryException code, or the exception type for anything else. */
    interface Action { void run() throws Exception; }
    static String outcome(Action action) {
        try { action.run(); return "OK"; }
        catch (RepositoryException failure) { return failure.code().name(); }
        catch (Exception failure) { return failure.getClass().getSimpleName() + ": " + failure.getMessage(); }
    }

    static ByteString bytes(byte[] value) { return ByteString.copyFrom(value); }
}
