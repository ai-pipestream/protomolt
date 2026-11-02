/** Revision-scoped Any resolution shared by inspection and comparison. */
import { ScalarType, equals, type DescMessage } from '@bufbuild/protobuf'
import type { Any } from '@bufbuild/protobuf/wkt'
import type { Client } from '@connectrpc/connect'
import type { NodeAddress } from '@/gen/ai/protomolt/proto/repo/v1/address_pb'
import { RepositoryOccurrenceKeyType, type RepositorySchemaOccurrenceStep } from '@/gen/ai/protomolt/proto/repo/v1/schema_occurrence_pb'
import type { DocumentService } from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import { PartSelectorSchema } from '@/gen/ai/protomolt/proto/repo/v2/manifest_pb'
import type { OccurrenceLocator, PartSelector, ResolvedOccurrence, RevisionValidation } from '@/gen/ai/protomolt/proto/repo/v2/manifest_pb'
import { retainedRegistry, typeNameOf } from './partReadCore'
import { Sha256 } from './sha256'

/**
 * Schemas for the typed Any occurrences of one revision, keyed by part and
 * complete locator, from the occurrences its validation record resolved. Each
 * distinct descriptor set is fetched once and checked against its digest.
 */
export class AnySchemas {
  private constructor(
    private readonly byOccurrence: Map<string, { desc: DescMessage; retained: ResolvedOccurrence; boundary: string }>,
    private readonly opaque: Set<string> = new Set(),
  ) {}

  static none(): AnySchemas {
    return new AnySchemas(new Map())
  }

  static async load(
    client: Client<typeof DocumentService>,
    address: NodeAddress,
    revision: bigint,
    validation: RevisionValidation,
    signal: AbortSignal,
    part?: PartSelector,
  ): Promise<AnySchemas> {
    const registries = new Map<string, Awaited<ReturnType<typeof retainedRegistry>>>()
    const byOccurrence = new Map<string, { desc: DescMessage; retained: ResolvedOccurrence; boundary: string }>()
    const selected = (locator: OccurrenceLocator | undefined) => part === undefined || locator?.part === undefined || equals(PartSelectorSchema, locator.part, part)
    const opaque = new Set(validation.opaqueOccurrences.filter(o => selected(o.occurrence)).map((occurrence) => retainedLocationKey(occurrence.occurrence)))
    for (const occurrence of validation.resolvedOccurrences) {
      if (!selected(occurrence.occurrence)) continue
      const schema = occurrence.schema
      if (schema === undefined) throw new Error(`Revision ${revision} resolved an Any occurrence without a schema reference.`)
      const locator = occurrence.occurrence
      const terminal = locator?.steps.at(-1)
      const boundary = terminal?.step.case === 'anyBoundary' ? terminal.step.value : undefined
      if (boundary !== undefined && (boundary.outcome.case !== 'resolved'
        || boundaryKey(boundary.typeUrl, boundary.valueSha256, boundary.valueSizeBytes, boundary.outcome.value.artifactSha256)
          !== boundaryKey(schema.typeUrl, occurrence.payloadSha256, occurrence.payloadLength, schema.descriptorSha256))) {
        throw new Error(`Revision ${revision} has an Any boundary that differs from its retained binding.`)
      }
      const key = retainedLocationKey(terminal?.step.case === 'anyBoundary' ? { ...locator!, steps: locator!.steps.slice(0, -1) } : locator)
      if (byOccurrence.has(key) || opaque.has(key)) throw new Error(`Revision ${revision} has conflicting Any occurrence bindings.`)
      let registry = registries.get(schema.descriptorSha256)
      if (registry === undefined) {
        registry = await retainedRegistry(client, address, revision, schema.descriptorSha256, signal)
        registries.set(schema.descriptorSha256, registry)
      }
      const typeName = typeNameOf(schema.typeUrl)
      const desc = registry.getMessage(typeName)
      if (desc === undefined) {
        throw new Error(`The retained descriptor set ${schema.descriptorSha256} does not define ${typeName}.`)
      }
      const nextBoundary = terminal?.step.case === 'anyBoundary'
        ? stepKey(terminal)
        : boundaryKey(schema.typeUrl, occurrence.payloadSha256, occurrence.payloadLength, schema.descriptorSha256)
      byOccurrence.set(key, { desc, retained: occurrence, boundary: nextBoundary })
    }
    return new AnySchemas(byOccurrence, opaque)
  }

  /** Opaque/missing/identity mismatch are distinct from a decoded value. */
  statusOf(any: Any, location: AnyLocation): 'typed' | 'opaque' | 'missing' | 'mismatch' {
    const key = locationKey(location)
    if (this.opaque.has(key)) return 'opaque'
    if (!this.byOccurrence.has(key)) return 'missing'
    return this.schemaOf(any, location) === null ? 'mismatch' : 'typed'
  }

  /** The schema retained for this location and exact payload, or null. */
  schemaOf(any: Any, location: AnyLocation): { desc: DescMessage; boundary: string } | null {
    const binding = this.byOccurrence.get(locationKey(location))
    if (binding === undefined) return null
    const { retained, desc, boundary } = binding
    const schema = retained.schema!
    if (schema.typeUrl !== any.typeUrl || retained.payloadLength !== BigInt(any.value.length)
      || retained.payloadSha256 !== new Sha256().update(any.value).hex()) return null
    return { desc, boundary }
  }
}

export interface AnyLocation {
  part: PartSelector
  steps: string[]
}

function keyOf(...values: string[]): string {
  return values.map((value) => `${value.length}:${value}`).join('')
}

function locationKey(location: AnyLocation): string {
  const selector = location.part.selector
  if (selector.case === undefined) throw new Error('An Any occurrence has no part selector.')
  return keyOf(selector.case, String(selector.value), ...location.steps)
}

function retainedLocationKey(locator: OccurrenceLocator | undefined): string {
  if (locator?.part === undefined || locator.steps.length === 0) throw new Error('An Any occurrence has no complete locator.')
  return locationKey({ part: locator.part, steps: locator.steps.map(stepKey) })
}

function boundaryKey(
  typeUrl: string,
  sha256: string,
  length: bigint,
  descriptorSha256: string,
  typeName = '',
  descriptorFingerprint = '',
): string {
  return keyOf('anyBoundary', typeUrl, sha256, String(length), descriptorSha256, typeName, descriptorFingerprint)
}

function stepKey(value: RepositorySchemaOccurrenceStep): string {
  const step = value.step
  switch (step.case) {
    case 'fieldNumber':
    case 'repeatedIndex':
      return `${step.case}:${step.value}`
    case 'mapKey': {
      const key = step.value
      if (key.value.case === undefined) throw new Error('An Any occurrence has a map key without a value.')
      return keyOf('mapKey', String(key.type), key.value.case, String(key.value.value))
    }
    case 'anyBoundary': {
      const boundary = step.value
      if (boundary.outcome.case !== 'resolved') throw new Error('An Any occurrence crosses an unresolved Any boundary.')
      const resolved = boundary.outcome.value
      return boundaryKey(
        boundary.typeUrl,
        boundary.valueSha256,
        boundary.valueSizeBytes,
        resolved.artifactSha256,
        resolved.schema?.typeName ?? '',
        resolved.schema?.descriptorFingerprint ?? '',
      )
    }
    default:
      throw new Error('An Any occurrence has an empty locator step.')
  }
}

export function mapStepKey(scalar: ScalarType, value: unknown): string {
  const type = RepositoryOccurrenceKeyType[ScalarType[scalar] as keyof typeof RepositoryOccurrenceKeyType]
  const kind = scalar === ScalarType.STRING ? 'stringValue' : scalar === ScalarType.BOOL ? 'boolValue'
    : [ScalarType.UINT32, ScalarType.FIXED32, ScalarType.UINT64, ScalarType.FIXED64].includes(scalar) ? 'unsignedValue' : 'signedValue'
  return keyOf('mapKey', String(type), kind, String(value))
}

export function withStep(location: AnyLocation, step: string, fn: () => void): void {
  location.steps.push(step)
  try {
    fn()
  } finally {
    location.steps.pop()
  }
}

