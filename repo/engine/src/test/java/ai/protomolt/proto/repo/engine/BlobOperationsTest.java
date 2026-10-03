package ai.protomolt.proto.repo.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.repo.v1.DeleteBlobRequest;
import ai.protomolt.proto.repo.v1.CompareAndPutBlobRequest;
import ai.protomolt.proto.repo.v1.ConditionalBlobKey;
import ai.protomolt.proto.repo.v1.FileStorageReference;
import ai.protomolt.proto.repo.v1.GetBlobRequest;
import ai.protomolt.proto.repo.v1.GetBlobForUpdateRequest;
import ai.protomolt.proto.repo.v1.PutBlobRequest;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;

/**
 * The blob surface's argument contract. Every case here is built with no object store and
 * no database behind it, which is the assertion as much as the message is: a request that
 * does not name what it wants has to be refused before anything is opened, and a test that
 * would touch a collaborator fails with a {@link NullPointerException} instead of passing
 * quietly.
 *
 * <p>What happens after the arguments are good needs a bucket and a schema, and lives in
 * {@code RepoServiceIT}.
 */
class BlobOperationsTest {

    private final BlobOperations blobs = new BlobOperations(null, null);

    @Test
    void reservedMutationsAreRejectedBeforeDriveLookupForEverySegmentPosition() {
        var caller = new RepositoryCaller("operator", true);
        for (String key : java.util.List.of(".protomolt-managed", ".protomolt-managed/v1/a",
                "/.protomolt-managed/", "prefix//.protomolt-managed//v1/a", "prefix/.protomolt-managed",
                "archive", "archive/a", "/archive/", "prefix//archive//a", "prefix/archive")) {
            for (Runnable call : java.util.List.<Runnable>of(
                    () -> blobs.put(caller, PutBlobRequest.newBuilder().setDriveName("unknown")
                            .setObjectKey(key).build()),
                    () -> blobs.compareAndPut(caller, CompareAndPutBlobRequest.newBuilder()
                            .setKey(ConditionalBlobKey.newBuilder().setDriveName("unknown").setObjectKey(key))
                            .setIfAbsent(true).build()),
                    () -> blobs.delete(caller, DeleteBlobRequest.newBuilder().setStorageRef(
                            FileStorageReference.newBuilder().setDriveName("unknown").setObjectKey(key)).build()))) {
                assertThatThrownBy(call::run).isInstanceOfSatisfying(RepositoryException.class,
                        error -> assertThat(error.code()).isEqualTo(RepositoryException.Code.PERMISSION_DENIED));
            }
        }
    }

    @Test
    void rawOperationsRejectNonAdministrativeCallersBeforeStorageAccess() {
        var caller = new RepositoryCaller("reader", false);
        for (Runnable call : java.util.List.<Runnable>of(
                () -> blobs.get(caller, GetBlobRequest.getDefaultInstance()),
                () -> blobs.put(caller, PutBlobRequest.getDefaultInstance()),
                () -> blobs.delete(caller, DeleteBlobRequest.getDefaultInstance()),
                () -> blobs.getForUpdate(caller, GetBlobForUpdateRequest.getDefaultInstance()),
                () -> blobs.compareAndPut(caller, CompareAndPutBlobRequest.getDefaultInstance()))) {
            assertThatThrownBy(call::run).isInstanceOfSatisfying(RepositoryException.class,
                    failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.PERMISSION_DENIED));
        }
    }

    @Test
    void conditionalRequestsRejectInvalidContractsBeforeAccessingStorage() {
        var key = ConditionalBlobKey.newBuilder().setDriveName("primary")
                .setObjectKey("state/current").build();
        assertRefusesNaming(() -> blobs.getForUpdate(new RepositoryCaller("test", true), GetBlobForUpdateRequest.getDefaultInstance()),
                "invalid conditional blob request");
        assertRefusesNaming(() -> blobs.getForUpdate(new RepositoryCaller("test", true), GetBlobForUpdateRequest.newBuilder()
                .setKey(key.toBuilder().setObjectKey(" ")).build()), "invalid conditional blob request");
        assertRefusesNaming(() -> blobs.compareAndPut(new RepositoryCaller("test", true), CompareAndPutBlobRequest.newBuilder()
                .setKey(key).build()), "invalid conditional blob request");
        assertRefusesNaming(() -> blobs.compareAndPut(new RepositoryCaller("test", true), CompareAndPutBlobRequest.newBuilder()
                .setKey(key).setIfAbsent(false).build()), "invalid conditional blob request");
        for (String unsafeTag : new String[] {"*", "W/\"tag\"", "\"one\",\"two\""}) {
            assertRefusesNaming(() -> blobs.compareAndPut(new RepositoryCaller("test", true), CompareAndPutBlobRequest.newBuilder()
                    .setKey(key).setExpectedEtag(unsafeTag).build()),
                    "invalid conditional blob request");
        }
        var unknown = key.toBuilder().setUnknownFields(com.google.protobuf.UnknownFieldSet.newBuilder()
                .addField(99, com.google.protobuf.UnknownFieldSet.Field.newBuilder()
                        .addVarint(1).build()).build()).build();
        assertRefusesNaming(() -> blobs.compareAndPut(new RepositoryCaller("test", true), CompareAndPutBlobRequest.newBuilder()
                .setKey(unknown).setIfAbsent(true).build()), "invalid conditional blob request");
    }

    private static void assertRefusesNaming(ThrowingCallable call, String... fragments) {
        assertThatThrownBy(call)
                .isInstanceOf(RepositoryException.class)
                .satisfies(t -> assertThat(((RepositoryException) t).code())
                        .isEqualTo(RepositoryException.Code.INVALID_ARGUMENT))
                .hasMessageContainingAll(fragments);
    }

    private static FileStorageReference.Builder ref() {
        return FileStorageReference.newBuilder().setDriveName("primary").setObjectKey("k");
    }

    // --- get --------------------------------------------------------------------

    @Test
    void getWithoutAStorageRefIsRefused() {
        assertRefusesNaming(() -> blobs.get(new RepositoryCaller("test", true), GetBlobRequest.getDefaultInstance()), "storage_ref");
    }

    @Test
    void getWithoutADriveIsRefused() {
        assertRefusesNaming(() -> blobs.get(new RepositoryCaller("test", true), GetBlobRequest.newBuilder()
                .setStorageRef(ref().setDriveName("")).build()), "storage_ref.drive_name");
    }

    @Test
    void getWithoutAnObjectKeyIsRefused() {
        assertRefusesNaming(() -> blobs.get(new RepositoryCaller("test", true), GetBlobRequest.newBuilder()
                .setStorageRef(ref().setObjectKey("")).build()), "storage_ref.object_key");
    }

    // --- delete -----------------------------------------------------------------

    @Test
    void deleteWithoutAStorageRefIsRefused() {
        assertRefusesNaming(() -> blobs.delete(new RepositoryCaller("test", true), DeleteBlobRequest.getDefaultInstance()),
                "storage_ref");
    }

    @Test
    void deleteWithoutADriveIsRefused() {
        assertRefusesNaming(() -> blobs.delete(new RepositoryCaller("test", true), DeleteBlobRequest.newBuilder()
                .setStorageRef(ref().setDriveName("")).build()), "storage_ref.drive_name");
    }

    @Test
    void deleteWithoutAnObjectKeyIsRefused() {
        assertRefusesNaming(() -> blobs.delete(new RepositoryCaller("test", true), DeleteBlobRequest.newBuilder()
                .setStorageRef(ref().setObjectKey("")).build()), "storage_ref.object_key");
    }

    /**
     * Get and delete read the same reference, and the two used to check it with two copies
     * of the same block. They answer identically because there is now one block.
     */
    @Test
    void getAndDeleteRefuseTheSameReferencesTheSameWay() {
        for (FileStorageReference.Builder bad : new FileStorageReference.Builder[] {
                ref().setDriveName(""), ref().setObjectKey("") }) {
            FileStorageReference reference = bad.build();
            String fromGet = messageOf(() -> blobs.get(new RepositoryCaller("test", true),
                    GetBlobRequest.newBuilder().setStorageRef(reference).build()));
            String fromDelete = messageOf(() -> blobs.delete(new RepositoryCaller("test", true),
                    DeleteBlobRequest.newBuilder().setStorageRef(reference).build()));
            assertThat(fromGet).isEqualTo(fromDelete);
        }
    }

    private static String messageOf(Runnable call) {
        try {
            call.run();
            throw new AssertionError("expected a refusal");
        } catch (RepositoryException e) {
            return e.getMessage();
        }
    }

    // --- put --------------------------------------------------------------------

    @Test
    void putWithoutADriveIsRefused() {
        assertRefusesNaming(() -> blobs.put(new RepositoryCaller("test", true), PutBlobRequest.getDefaultInstance()), "drive_name");
    }

    @Test
    void putWithABlankDriveIsRefused() {
        assertRefusesNaming(() -> blobs.put(new RepositoryCaller("test", true), PutBlobRequest.newBuilder()
                .setDriveName("   ").build()), "drive_name");
    }
}
