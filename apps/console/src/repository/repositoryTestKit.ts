/**
 * Test plumbing for the repository screens: generated service clients over
 * Connect-ES's in-memory router, which runs the real Connect protocol with
 * binary protobuf. Handlers answer with messages built from the generated
 * types; there are no JSON fixtures and no fetch mocks.
 */
import { create, toBinary, type DescFile } from '@bufbuild/protobuf'
import { FileDescriptorSetSchema, timestampFromDate } from '@bufbuild/protobuf/wkt'
import { createRouterTransport, type ConnectRouter } from '@connectrpc/connect'
import { NodeAddressSchema, type NodeAddress } from '@/gen/ai/protomolt/proto/repo/v1/address_pb'
import { repositoryClients, type RepositoryClients } from './clients'

/** Clients whose calls the given routes answer in memory. */
export function routerClients(routes: (router: ConnectRouter) => void): RepositoryClients {
  return repositoryClients(createRouterTransport(routes))
}

/** A fixed call number for tests. */
export function testAddress(overrides: Partial<NodeAddress> = {}): NodeAddress {
  return create(NodeAddressSchema, {
    accountId: 'acct-a',
    graphId: 'intake:acct-a',
    graphAddressId: 'ds-files',
    docId: 'doc-1',
    ...overrides,
  })
}

/** A Timestamp for a fixed instant. */
export function at(iso: string) {
  return timestampFromDate(new Date(iso))
}

/**
 * The serialized FileDescriptorSet of a generated file and its whole import
 * closure, as the repository retains one at save time.
 */
export function descriptorSetOf(file: DescFile): Uint8Array {
  const seen = new Map<string, DescFile>()
  const visit = (f: DescFile) => {
    if (seen.has(f.name)) return
    for (const dep of f.dependencies) visit(dep)
    seen.set(f.name, f)
  }
  visit(file)
  return toBinary(FileDescriptorSetSchema, create(FileDescriptorSetSchema, { file: [...seen.values()].map((f) => f.proto) }))
}
