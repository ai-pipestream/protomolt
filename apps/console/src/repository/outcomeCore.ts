/**
 * Operation outcome display: lookupCore answers the wire states (committed with
 * an accession or a recorded rejection, no recorded outcome, unavailable) and
 * this turns one answer into what the inspector shows, including the router
 * location of a committed revision. The wire carries no unchanged flag: a save
 * that replaced nothing is a new document (previous revision 0), anything else
 * names the revision it replaced.
 */
import type { Client } from '@connectrpc/connect'
import { RejectionReasonSchema } from '@/gen/ai/protomolt/proto/repo/v2/errors_pb'
import type { DocumentService } from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import { enumLabel } from './formatCore'
import { lookupOutcome, type OutcomeAnswer } from './lookupCore'
import type { RpcFailure } from './rpcErrorCore'

/** One rule violation of a recorded VALIDATION_FAILED rejection. */
export interface OutcomeViolation {
  ruleId: string
  fieldPath: string
  message: string
}

/** The router location of a committed revision, resolvable by the console router. */
export interface DocumentRoute {
  name: 'repository-document'
  query: Record<string, string>
}

/** What the inspector shows for one answer; `kind` picks the panel. */
export type OutcomeView =
  | {
      kind: 'committed'
      docId: string
      datasourceId: string
      revision: string
      previousRevision: string
      newDocument: boolean
      tombstone: boolean
      recordedAt: string | null
      route: DocumentRoute | null
    }
  | { kind: 'rejected'; reason: string; message: string; violations: OutcomeViolation[]; recordedAt: string | null }
  | { kind: 'none'; explanation: string }
  | { kind: 'unavailable'; failure: RpcFailure }

/** What the none panel says; "no outcome" never means the save failed. */
export const NO_OUTCOME_EXPLANATION =
  'No terminal outcome is recorded for this account and operation ID. '
  + 'An outstanding request may still commit later; a mistyped ID looks the same.'

/** One wire answer as the inspector shows it. */
export function describeOutcome(answer: OutcomeAnswer): OutcomeView {
  switch (answer.state) {
    case 'committed': {
      const accession = answer.accession
      const address = accession.address
      return {
        kind: 'committed',
        docId: address?.docId ?? '',
        datasourceId: address?.graphAddressId ?? '',
        revision: accession.revision.toString(),
        previousRevision: accession.previousRevision.toString(),
        newDocument: accession.previousRevision === 0n,
        tombstone: accession.tombstone,
        recordedAt: answer.recordedAt,
        route: address
          ? {
              name: 'repository-document',
              query: {
                account: address.accountId,
                graph: address.graphId,
                node: address.graphAddressId,
                doc: address.docId,
                revision: accession.revision.toString(),
              },
            }
          : null,
      }
    }
    case 'rejected':
      return {
        kind: 'rejected',
        reason: enumLabel(RejectionReasonSchema, answer.rejection.reason),
        message: answer.rejection.message,
        violations: answer.rejection.violations.map((violation) => ({
          ruleId: violation.ruleId,
          fieldPath: violation.fieldPath,
          message: violation.message,
        })),
        recordedAt: answer.recordedAt,
      }
    case 'none':
      return { kind: 'none', explanation: NO_OUTCOME_EXPLANATION }
    case 'unavailable':
      return { kind: 'unavailable', failure: answer.failure }
  }
}

/** Looks up one operation and answers its display form; input errors throw, like lookupOutcome. */
export async function inspectOutcome(
  client: Client<typeof DocumentService>,
  accountId: string,
  operationId: string,
  signal: AbortSignal,
): Promise<OutcomeView> {
  return describeOutcome(await lookupOutcome(client, accountId, operationId, signal))
}
