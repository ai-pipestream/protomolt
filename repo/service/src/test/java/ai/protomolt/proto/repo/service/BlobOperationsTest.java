package ai.protomolt.proto.repo.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.repo.v1.DeleteBlobRequest;
import ai.protomolt.proto.repo.v1.FileStorageReference;
import ai.protomolt.proto.repo.v1.GetBlobRequest;
import ai.protomolt.proto.repo.v1.PutBlobRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;

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

    private static void assertRefusesNaming(ThrowingCallable call, String... fragments) {
        assertThatThrownBy(call)
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(t -> assertThat(Status.fromThrowable(t).getCode())
                        .isEqualTo(Status.Code.INVALID_ARGUMENT))
                .hasMessageContainingAll(fragments);
    }

    private static FileStorageReference.Builder ref() {
        return FileStorageReference.newBuilder().setDriveName("primary").setObjectKey("k");
    }

    // --- get --------------------------------------------------------------------

    @Test
    void getWithoutAStorageRefIsRefused() {
        assertRefusesNaming(() -> blobs.get(GetBlobRequest.getDefaultInstance()), "storage_ref");
    }

    @Test
    void getWithoutADriveIsRefused() {
        assertRefusesNaming(() -> blobs.get(GetBlobRequest.newBuilder()
                .setStorageRef(ref().setDriveName("")).build()), "storage_ref.drive_name");
    }

    @Test
    void getWithoutAnObjectKeyIsRefused() {
        assertRefusesNaming(() -> blobs.get(GetBlobRequest.newBuilder()
                .setStorageRef(ref().setObjectKey("")).build()), "storage_ref.object_key");
    }

    // --- delete -----------------------------------------------------------------

    @Test
    void deleteWithoutAStorageRefIsRefused() {
        assertRefusesNaming(() -> blobs.delete(DeleteBlobRequest.getDefaultInstance()),
                "storage_ref");
    }

    @Test
    void deleteWithoutADriveIsRefused() {
        assertRefusesNaming(() -> blobs.delete(DeleteBlobRequest.newBuilder()
                .setStorageRef(ref().setDriveName("")).build()), "storage_ref.drive_name");
    }

    @Test
    void deleteWithoutAnObjectKeyIsRefused() {
        assertRefusesNaming(() -> blobs.delete(DeleteBlobRequest.newBuilder()
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
            String fromGet = messageOf(() -> blobs.get(
                    GetBlobRequest.newBuilder().setStorageRef(reference).build()));
            String fromDelete = messageOf(() -> blobs.delete(
                    DeleteBlobRequest.newBuilder().setStorageRef(reference).build()));
            assertThat(fromGet).isEqualTo(fromDelete);
        }
    }

    private static String messageOf(Runnable call) {
        try {
            call.run();
            throw new AssertionError("expected a refusal");
        } catch (StatusRuntimeException e) {
            return e.getMessage();
        }
    }

    // --- put --------------------------------------------------------------------

    @Test
    void putWithoutADriveIsRefused() {
        assertRefusesNaming(() -> blobs.put(PutBlobRequest.getDefaultInstance()), "drive_name");
    }

    @Test
    void putWithABlankDriveIsRefused() {
        assertRefusesNaming(() -> blobs.put(PutBlobRequest.newBuilder()
                .setDriveName("   ").build()), "drive_name");
    }
}
