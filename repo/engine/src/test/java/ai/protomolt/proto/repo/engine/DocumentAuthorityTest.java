package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.v1.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DocumentAuthorityTest {
    @Test void scopedPrincipalCannotReachStorageBeforeAccountPolicyExists() {
        var operations = new DocumentOperations(null, null, null, null, null, null);
        var caller = new RepositoryCaller("scoped", false);
        for (Runnable operation : java.util.List.<Runnable>of(
                () -> operations.saveDocument(caller, SaveDocumentRequest.getDefaultInstance()),
                () -> operations.getDocument(caller, GetDocumentRequest.getDefaultInstance()),
                () -> operations.getDocumentByReference(caller, GetDocumentByReferenceRequest.getDefaultInstance()),
                () -> operations.getDocumentManifest(caller, GetDocumentManifestRequest.getDefaultInstance()),
                () -> operations.listDocuments(caller, ListDocumentsRequest.getDefaultInstance()),
                () -> operations.deleteDocument(caller, DeleteDocumentRequest.getDefaultInstance()))) {
            assertThatThrownBy(operation::run).isInstanceOfSatisfying(RepositoryException.class,
                    failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.PERMISSION_DENIED));
        }
    }
}
