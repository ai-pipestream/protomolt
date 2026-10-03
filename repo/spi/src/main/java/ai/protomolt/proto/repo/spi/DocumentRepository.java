package ai.protomolt.proto.repo.spi;

import ai.protomolt.proto.repo.v1.*;

/** Shared document operations using existing wire contracts and trusted caller identity. */
public interface DocumentRepository {
    default SaveDocumentResponse saveDocument(RepositoryCaller caller, SaveDocumentRequest request) {
        return saveDocument(caller, request, RepositoryOperationControl.NONE);
    }
    SaveDocumentResponse saveDocument(RepositoryCaller caller, SaveDocumentRequest request, RepositoryOperationControl control);
    default GetDocumentResponse getDocument(RepositoryCaller caller, GetDocumentRequest request) {
        return getDocument(caller, request, RepositoryReadControl.NONE);
    }
    GetDocumentResponse getDocument(RepositoryCaller caller, GetDocumentRequest request, RepositoryReadControl control);
    default GetDocumentResponse getDocumentByReference(RepositoryCaller caller, GetDocumentByReferenceRequest request) {
        return getDocumentByReference(caller, request, RepositoryReadControl.NONE);
    }
    GetDocumentResponse getDocumentByReference(RepositoryCaller caller, GetDocumentByReferenceRequest request, RepositoryReadControl control);
    GetDocumentManifestResponse getDocumentManifest(RepositoryCaller caller, GetDocumentManifestRequest request);
    DeleteDocumentResponse deleteDocument(RepositoryCaller caller, DeleteDocumentRequest request);
    ListDocumentsResponse listDocuments(RepositoryCaller caller, ListDocumentsRequest request);
}
