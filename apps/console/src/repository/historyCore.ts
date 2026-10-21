/**
 * Revision history: ListRevisions pages, newest first, with each revision's
 * part changes.
 */
import { ChangeKindSchema } from '@/gen/ai/protomolt/proto/repo/v2/events_pb'
import type { RevisionSummary } from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import { ValidationStatusSchema } from '@/gen/ai/protomolt/proto/repo/v2/manifest_pb'
import { enumLabel, instant } from './formatCore'
import { ownerLabel, partLabel } from './documentCore'

/** One revision of the history table. */
export interface RevisionRow {
  revision: bigint
  previousRevision: string
  committedAt: string | null
  operationId: string
  owner: string
  tombstone: boolean
  validation: string
  changes: { part: string; kind: string; sha256: string }[]
}

export function revisionRow(summary: RevisionSummary): RevisionRow {
  return {
    revision: summary.revision,
    previousRevision: summary.previousRevision === 0n ? 'none' : summary.previousRevision.toString(),
    committedAt: instant(summary.committedAt),
    operationId: summary.operationId,
    owner: summary.tombstone && summary.owner === undefined ? 'tombstone' : ownerLabel(summary.owner),
    tombstone: summary.tombstone,
    validation: enumLabel(ValidationStatusSchema, summary.validationStatus),
    changes: summary.changes.map((c) => ({
      part: partLabel(c.selector),
      kind: enumLabel(ChangeKindSchema, c.kind),
      sha256: c.sha256,
    })),
  }
}

/** The history page size; the server accepts 1 to 1000. */
export const HISTORY_PAGE_SIZE = 25
