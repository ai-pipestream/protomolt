/**
 * The merge workbench's pure logic: building merge-schemas requests, interpreting the
 * clash report, tracking resolutions, and deriving the registration payload from the
 * emitted proto source. The view stays thin; everything decidable lives here.
 */

import type { SchemaReference } from './api'

export interface MergeSourceInput {
  /** Scope name (becomes field prefixes on rename). */
  name: string
  /** Fully-qualified message type. */
  type: string
  /** The subject's compiled descriptor set, base64. */
  descriptorSetBase64: string
}

export interface ClashOrigin {
  source: string
  type: string
}

export interface MergeClash {
  field: string
  kind: 'coalesced' | 'type-clash' | 'cardinality-clash'
  origins: ClashOrigin[]
  suggested: { action: string; names?: Record<string, string> }
}

export interface ResolutionChoice {
  action: 'rename' | 'prefer' | 'coalesce'
  source?: string
  names?: Record<string, string>
}

export interface MergeReport {
  resolved: boolean
  clashes: MergeClash[]
  type?: string
  file?: string
  protoSource?: string
  descriptorSetBase64?: string
  joinRules?: string[]
  unionRules?: Record<string, { rules?: string[] }>
}

/** The merge-schemas envelope (proto3 JSON of MergeSchemasRequest). */
export function mergeRequest(
  name: string,
  sources: MergeSourceInput[],
  resolutions: Record<string, ResolutionChoice>,
  reportOnly: boolean,
): unknown {
  return {
    name,
    sources: sources.map((s) => ({
      name: s.name,
      schema: { descriptorSetBase64: s.descriptorSetBase64 },
      type: s.type,
    })),
    ...(Object.keys(resolutions).length ? { resolutions } : {}),
    ...(reportOnly ? { reportOnly: true } : {}),
  }
}

/** A hard clash needs a decision; coalesced entries are informational. */
export function isHardClash(clash: MergeClash): boolean {
  return clash.kind !== 'coalesced'
}

/** Pre-fills every hard clash with its suggested resolution (rename, prefixed names). */
export function defaultResolutions(clashes: MergeClash[]): Record<string, ResolutionChoice> {
  const out: Record<string, ResolutionChoice> = {}
  for (const clash of clashes) {
    if (!isHardClash(clash)) continue
    out[clash.field] = {
      action: (clash.suggested.action as ResolutionChoice['action']) ?? 'rename',
      ...(clash.suggested.names ? { names: { ...clash.suggested.names } } : {}),
    }
  }
  return out
}

/** Hard clashes still missing a resolution (blocks the merge). */
export function unresolvedFields(
  clashes: MergeClash[],
  resolutions: Record<string, ResolutionChoice>,
): string[] {
  return clashes
    .filter((c) => isHardClash(c) && !resolutions[c.field])
    .map((c) => c.field)
}

/** The import paths of an emitted proto source, in order. */
export function importsOf(protoSource: string): string[] {
  const imports: string[] = []
  for (const line of protoSource.split('\n')) {
    const match = /^import\s+"([^"]+)"\s*;/.exec(line.trim())
    if (match) imports.push(match[1])
  }
  return imports
}

/**
 * References for registering the merged schema: each import is a subject registered
 * under its own import path (the register-by-path convention), pinned to the version
 * the caller resolved.
 */
export function mergedReferences(
  protoSource: string,
  versionOf: (subject: string) => number,
): SchemaReference[] {
  return importsOf(protoSource).map((path) => ({
    name: path,
    subject: path,
    version: versionOf(path),
  }))
}

/** A default scope name from a type's last segment, lowercased ('shop.v1.Order' → 'order'). */
export function scopeNameFor(type: string, taken: string[]): string {
  const base = (type.split('.').pop() ?? 'source')
    .replace(/[^A-Za-z0-9_]/g, '_')
    .toLowerCase()
  let name = base
  let n = 2
  while (taken.includes(name)) name = `${base}_${n++}`
  return name
}

/** A default merged type name from the two sources ('derived.v1.OrderTicket'). */
export function mergedNameFor(sources: MergeSourceInput[]): string {
  const parts = sources.map((s) => {
    const last = s.type.split('.').pop() ?? 'X'
    return last.charAt(0).toUpperCase() + last.slice(1)
  })
  return `derived.v1.${parts.join('')}`
}
