package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.admission.DocumentRetainedDefinitions;
import ai.protomolt.proto.repo.catalog.Catalog;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.Principal;
import ai.protomolt.proto.repo.v1.RepositorySchemaAssetReference;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Int64Value;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * GetSchemaDescriptor (#534, requirements §8 and §9): a revision's retained
 * descriptor set read by its digest. The digest grants nothing; access follows the
 * referring document, and every unreachable digest answers one indistinguishable
 * NOT_FOUND. The artifact length is reserved before its bytes load, and the bytes
 * are hashed again before they leave the handler.
 */
class SchemaDescriptorReadIT {
    private static final String NOT_FOUND = "Schema descriptor not found";

    private AccessionFixture f;
    private String account;
    private NodeAddress address;
    private StoredManifest first;

    @BeforeEach void open() throws Exception {
        f = new AccessionFixture();
        account = "acct-" + UUID.randomUUID();
        address = address(account, "doc-schemas");
        f.saves.save(processAuthority(), create("op-1", address, document(account, "doc-schemas", "t", "reader")).build());
        first = f.catalog.manifest(address, 1).orElseThrow();
    }

    @AfterEach void close() { f.close(); }

    @Test void everyRetainedDescriptorIsServedVerbatimToAReader() throws Exception {
        var digests = DocumentReads.retainedDescriptors(first.getValidation());
        // Each part's fragment schema and the structured_data Any's schema.
        assertThat(digests).contains(coreFragment(first).getDescriptorSha256(), payloadSchema(first).getDescriptorSha256());
        for (var caller : List.of(processAuthority(), reader(account))) {
            for (var digest : digests) {
                var response = f.reads.getSchemaDescriptor(caller, request(address, digest).build());
                assertThat(response.getRevision()).isEqualTo(1);
                assertThat(response.getDescriptorSha256()).isEqualTo(digest);
                assertThat(response.getDescriptorSet()).isEqualTo(f.catalog.artifact(digest).orElseThrow());
                assertThat(DocumentPartCodec.sha256Hex(response.getDescriptorSet())).isEqualTo(digest);
                assertThat(FileDescriptorSet.parseFrom(response.getDescriptorSet()).getFileCount()).isPositive();
            }
        }
        var payload = f.reads.getSchemaDescriptor(reader(account),
                request(address, payloadSchema(first).getDescriptorSha256()).build());
        assertThat(FileDescriptorSet.parseFrom(payload.getDescriptorSet()).getFileList())
                .extracting(file -> file.getName()).contains(Int64Value.getDescriptor().getFile().getName());
    }

    @Test void grantSnapshotSchemasAreRetainedDescriptorsToo() {
        var snapshot = RepositorySchemaAssetReference.newBuilder().setDescriptorSha256("c".repeat(64)).build();
        var validation = first.getValidation().toBuilder().clearProcessAuthority()
                .setWriterGrant(WriterGrantEvidence.newBuilder().setSnapshotSchema(snapshot)).build();
        assertThat(DocumentReads.retainedDescriptors(validation)).contains("c".repeat(64))
                .containsAll(DocumentReads.retainedDescriptors(first.getValidation()));
    }

    @Test void unreachableAndNonexistentDigestsAnswerTheSameNotFound() {
        var digest = payloadSchema(first).getDescriptorSha256();
        var other = "other-" + UUID.randomUUID();
        var refusals = new ArrayList<AccessionException>();
        // A digest no artifact has.
        refusals.add(refused(reader(account), request(address, "f".repeat(64))));
        // A retained artifact that is metadata, not a descriptor.
        refusals.add(refused(reader(account), request(address, payloadSchema(first).getMetadataSha256())));
        // A real descriptor, asked for by a caller the document's ACL does not name.
        refusals.add(refused(caller("outsider", account), request(address, digest)));
        // A real descriptor, asked for by a caller bound to another account.
        refusals.add(refused(caller("reader", other), request(address, digest)));
        // A real descriptor, under a document that does not exist.
        refusals.add(refused(reader(account), request(address(account, "doc-absent"), digest)));
        // A real descriptor, under a revision that does not exist.
        refusals.add(refused(reader(account), request(address, digest)
                .setRevision(RevisionSelector.newBuilder().setRevision(9))));
        assertThat(refusals).allSatisfy(refusal -> {
            assertThat(refusal.code()).isEqualTo(Status.Code.NOT_FOUND);
            assertThat(refusal.getMessage()).isEqualTo(NOT_FOUND);
        });
    }

    @Test void aDigestIsServedOnlyForTheRevisionsThatRetainIt() {
        // Revision 2 rewrites CORE without structured_data, so it retains no Int64Value schema.
        var core = Document.newBuilder().setDocId("doc-schemas").setOwnership(ownership(account, "reader"))
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle("second").setBody("no payload")).build();
        f.saves.save(processAuthority(), create("op-2", address, core).clearWrites()
                .addWrites(put(DocumentPart.DOCUMENT_PART_CORE)).setExpected(at(1)).build());
        var second = f.catalog.manifest(address, 2).orElseThrow();
        var payload = payloadSchema(first).getDescriptorSha256();
        assertThat(DocumentReads.retainedDescriptors(second.getValidation())).doesNotContain(payload);

        var current = refused(reader(account), request(address, payload));
        assertThat(current.code()).isEqualTo(Status.Code.NOT_FOUND);
        assertThat(current.getMessage()).isEqualTo(NOT_FOUND);
        var pinned = f.reads.getSchemaDescriptor(reader(account), request(address, payload)
                .setRevision(RevisionSelector.newBuilder().setRevision(1)).build());
        assertThat(pinned.getRevision()).isEqualTo(1);
        assertThat(pinned.getDescriptorSet()).isEqualTo(f.catalog.artifact(payload).orElseThrow());
        var latest = f.reads.getSchemaDescriptor(reader(account),
                request(address, coreFragment(second).getDescriptorSha256()).build());
        assertThat(latest.getRevision()).isEqualTo(2);
    }

    @Test void aDeletedDocumentServesItsDescriptorsOnlyAtAnExactRevision() {
        new DocumentDeletes(f.catalog, Clock.systemUTC()).delete(processAuthority(), DeleteDocumentRequest.newBuilder()
                .setAddress(address).setExpected(at(1)).setOperationId("delete").build());
        var digest = payloadSchema(first).getDescriptorSha256();
        assertThat(refused(reader(account), request(address, digest)).getMessage()).isEqualTo(NOT_FOUND);
        assertThat(f.reads.getSchemaDescriptor(reader(account), request(address, digest)
                .setRevision(RevisionSelector.newBuilder().setRevision(1)).build()).getRevision()).isEqualTo(1);
    }

    @Test void theCatalogLengthIsReservedBeforeAnyByteIsLoaded() {
        var digest = payloadSchema(first).getDescriptorSha256();
        long length = f.catalog.artifact(digest).orElseThrow().size();
        var loads = new AtomicInteger();
        var counting = catalog(loads, bytes -> bytes, OptionalLong.empty());
        var requested = new ArrayList<Long>();
        var released = new AtomicInteger();
        var budgeted = new DocumentReads(counting, f.drives, limits(), bytes -> {
            requested.add(bytes);
            return released::incrementAndGet;
        }, Clock.systemUTC());
        assertThat(budgeted.getSchemaDescriptor(reader(account), request(address, digest).build()).getDescriptorSet().size())
                .isEqualTo(length);
        assertThat(requested).containsExactly(length);
        assertThat(released).hasValue(1);
        assertThat(loads).hasValue(1);

        var exhausted = new DocumentReads(counting, f.drives, limits(), bytes -> {
            throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED, "Read buffer budget exhausted");
        }, Clock.systemUTC());
        assertThatThrownBy(() -> exhausted.getSchemaDescriptor(reader(account), request(address, digest).build()))
                .isInstanceOfSatisfying(AccessionException.class,
                        failure -> assertThat(failure.code()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED));
        assertThat(loads).as("a refused reservation loads nothing").hasValue(1);
    }

    @Test void aLengthAboveTheDescriptorCeilingIsRefusedBeforeReserving() {
        var loads = new AtomicInteger();
        var oversized = catalog(loads, bytes -> bytes, OptionalLong.of(DocumentRetainedDefinitions.MAX_DESCRIPTOR_BYTES + 1L));
        var reserved = new AtomicInteger();
        var reads = new DocumentReads(oversized, f.drives, limits(), bytes -> {
            reserved.incrementAndGet();
            return () -> {};
        }, Clock.systemUTC());
        assertThatThrownBy(() -> reads.getSchemaDescriptor(reader(account),
                request(address, payloadSchema(first).getDescriptorSha256()).build()))
                .isInstanceOfSatisfying(AccessionException.class,
                        failure -> assertThat(failure.code()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED));
        assertThat(reserved).hasValue(0);
        assertThat(loads).hasValue(0);
    }

    @Test void bytesThatDifferFromTheDigestOrTheLengthAreDataLoss() {
        var digest = payloadSchema(first).getDescriptorSha256();
        // The catalog checks its own rows; these stand in for a catalog that did not.
        UnaryOperator<ByteString> flipped = bytes -> {
            var damaged = bytes.toByteArray();
            damaged[damaged.length / 2] ^= 1;
            return ByteString.copyFrom(damaged);
        };
        UnaryOperator<ByteString> longer = bytes -> bytes.concat(ByteString.copyFromUtf8("x"));
        for (var damage : List.of(flipped, longer)) {
            var reads = new DocumentReads(catalog(new AtomicInteger(), damage, OptionalLong.empty()), f.drives, limits(),
                    bytes -> () -> {}, Clock.systemUTC());
            assertThatThrownBy(() -> reads.getSchemaDescriptor(reader(account), request(address, digest).build()))
                    .isInstanceOfSatisfying(AccessionException.class,
                            failure -> assertThat(failure.code()).isEqualTo(Status.Code.DATA_LOSS));
        }
    }

    @Test void aReferencedDescriptorMissingFromTheCatalogIsDataLoss() {
        var digest = payloadSchema(first).getDescriptorSha256();
        var missing = (Catalog) Proxy.newProxyInstance(Catalog.class.getClassLoader(), new Class<?>[] {Catalog.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "artifactLength" -> OptionalLong.empty();
                    case "artifact" -> Optional.empty();
                    default -> forward(method, arguments);
                });
        var reads = new DocumentReads(missing, f.drives, limits(), bytes -> () -> {}, Clock.systemUTC());
        assertThatThrownBy(() -> reads.getSchemaDescriptor(reader(account), request(address, digest).build()))
                .isInstanceOfSatisfying(AccessionException.class,
                        failure -> assertThat(failure.code()).isEqualTo(Status.Code.DATA_LOSS));
    }

    @Test void theGrpcSurfaceServesTheBytesAndOneNotFoundStatus() throws Exception {
        var name = "schemas-" + UUID.randomUUID();
        var server = InProcessServerBuilder.forName(name).directExecutor()
                .addService(new AccessionGrpcService(f.saves, f.reads, authentication -> new RepositoryCaller(
                        authentication.caller().name(), authentication.caller().unrestricted()))).build().start();
        var channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        try {
            var stub = DocumentServiceGrpc.newBlockingStub(channel);
            var digest = payloadSchema(first).getDescriptorSha256();
            assertThat(stub.getSchemaDescriptor(request(address, digest).build()).getDescriptorSet())
                    .isEqualTo(f.catalog.artifact(digest).orElseThrow());
            var statuses = new ArrayList<Status>();
            for (var refused : List.of(request(address, "f".repeat(64)), request(address(account, "doc-absent"), digest)))
                statuses.add(catchThrowableOfType(StatusRuntimeException.class,
                        () -> stub.getSchemaDescriptor(refused.build())).getStatus());
            assertThat(statuses).allSatisfy(status -> {
                assertThat(status.getCode()).isEqualTo(Status.Code.NOT_FOUND);
                assertThat(status.getDescription()).isEqualTo(NOT_FOUND);
            });
            var invalid = catchThrowableOfType(StatusRuntimeException.class,
                    () -> stub.getSchemaDescriptor(request(address, "F".repeat(64)).build()));
            assertThat(invalid.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        } finally {
            channel.shutdownNow();
            server.shutdownNow();
        }
    }

    // ------------------------------------------------------------------ helpers

    private static GetSchemaDescriptorRequest.Builder request(NodeAddress address, String digest) {
        return GetSchemaDescriptorRequest.newBuilder().setAddress(address).setDescriptorSha256(digest);
    }

    private AccessionException refused(RepositoryCaller caller, GetSchemaDescriptorRequest.Builder request) {
        return catchThrowableOfType(AccessionException.class, () -> f.reads.getSchemaDescriptor(caller, request.build()));
    }

    private static RepositoryCaller reader(String account) { return caller("reader", account); }

    private static RepositoryCaller caller(String name, String account) {
        return new RepositoryCaller(name, false, Set.of(account),
                Set.of(Principal.newBuilder().setIdentityType("service").setIdentity(name).build()));
    }

    private static RepositorySchemaAssetReference coreFragment(StoredManifest stored) {
        return stored.getValidation().getPartSchemasList().stream()
                .filter(schema -> schema.getPart().getPart() == DocumentPart.DOCUMENT_PART_CORE).findFirst()
                .orElseThrow().getFragmentSchema();
    }

    private static RepositorySchemaAssetReference payloadSchema(StoredManifest stored) {
        return stored.getValidation().getResolvedOccurrencesList().stream()
                .filter(occurrence -> occurrence.getSchema().getTypeUrl().equals(INT64_URL)).findFirst()
                .orElseThrow().getSchema();
    }

    /** The fixture catalog with artifact loads counted, their bytes rewritten and, when given, a fixed length. */
    private Catalog catalog(AtomicInteger loads, UnaryOperator<ByteString> bytes, OptionalLong length) {
        return (Catalog) Proxy.newProxyInstance(Catalog.class.getClassLoader(), new Class<?>[] {Catalog.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("artifactLength") && length.isPresent()) return length;
                    if (method.getName().equals("artifact")) {
                        loads.incrementAndGet();
                        return ((Optional<?>) forward(method, arguments)).map(value -> bytes.apply((ByteString) value));
                    }
                    return forward(method, arguments);
                });
    }

    private Object forward(Method method, Object[] arguments) throws Throwable {
        try {
            return method.invoke(f.catalog, arguments);
        } catch (InvocationTargetException failure) {
            throw failure.getCause();
        }
    }
}
