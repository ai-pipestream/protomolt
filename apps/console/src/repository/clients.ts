/**
 * The generated service clients the repository screens call, provided once at
 * the application root so tests can provide clients over an in-memory router.
 */
import { createClient, type Client, type Transport } from '@connectrpc/connect'
import { inject, type InjectionKey } from 'vue'
import { ArchiveService } from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_service_pb'
import { DriveService } from '@/gen/ai/protomolt/proto/repo/v1/drive_pb'
import { DocumentService } from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import { SessionApi } from '@/services/session'
import type { UploadRequest } from './uploadCore'

/** One client per service the screens call. */
export interface RepositoryClients {
  documents: Client<typeof DocumentService>
  drives: Client<typeof DriveService>
  archives: Client<typeof ArchiveService>
}

/** Clients for every repository service over one transport. */
export function repositoryClients(transport: Transport): RepositoryClients {
  return {
    documents: createClient(DocumentService, transport),
    drives: createClient(DriveService, transport),
    archives: createClient(ArchiveService, transport),
  }
}

export const REPOSITORY_CLIENTS: InjectionKey<RepositoryClients> = Symbol('repository-clients')

/** The provided clients; a screen mounted without them fails by name. */
export function useRepositoryClients(): RepositoryClients {
  const clients = inject(REPOSITORY_CLIENTS)
  if (clients === undefined) {
    throw new Error('Repository clients were not provided: provide REPOSITORY_CLIENTS at the app root.')
  }
  return clients
}

/** Starts sign-in for a call answered UNAUTHENTICATED outside the Connect transport. */
export const REPOSITORY_SIGN_IN: InjectionKey<() => void> = Symbol('repository-sign-in')

export function useSignIn(): () => void {
  const signIn = inject(REPOSITORY_SIGN_IN)
  if (signIn === undefined) {
    throw new Error('Repository sign-in was not provided: provide REPOSITORY_SIGN_IN at the app root.')
  }
  return signIn
}

/** Makes the HTTP request an upload sends; the browser's XMLHttpRequest unless a test provides one. */
export const REPOSITORY_UPLOAD_REQUEST: InjectionKey<() => UploadRequest> = Symbol('repository-upload-request')

/** The session status read the account picker uses; the host's SessionApi unless a test provides one. */
export const REPOSITORY_SESSION: InjectionKey<Pick<SessionApi, 'status'>> = Symbol('repository-session')

export function useRepositorySession(): Pick<SessionApi, 'status'> {
  return inject(REPOSITORY_SESSION, () => new SessionApi(), true)
}
