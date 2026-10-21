/**
 * The generated service clients the repository screens call, provided once at
 * the application root so tests can provide clients over an in-memory router.
 */
import { createClient, type Client, type Transport } from '@connectrpc/connect'
import { inject, type InjectionKey } from 'vue'
import { AccountService } from '@/gen/ai/protomolt/proto/account/v1/account_service_pb'
import { DriveService } from '@/gen/ai/protomolt/proto/repo/v1/drive_pb'
import { DocumentService } from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'

/** One client per service the screens call. */
export interface RepositoryClients {
  documents: Client<typeof DocumentService>
  drives: Client<typeof DriveService>
  accounts: Client<typeof AccountService>
}

/** Clients for every repository service over one transport. */
export function repositoryClients(transport: Transport): RepositoryClients {
  return {
    documents: createClient(DocumentService, transport),
    drives: createClient(DriveService, transport),
    accounts: createClient(AccountService, transport),
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
