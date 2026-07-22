/**
 * Check-then-register flow for the compat panel, as a pure fold over the
 * API so the state machine is unit-testable with a mocked client.
 *
 * The server has no dry-run endpoint. The closest safe probe is the
 * content-identity lookup (POST /subjects/{s}): a hit means registering is a
 * no-op (idempotent re-register returns the same id), a 40403 miss means a
 * real registration WOULD happen — which is exactly when the UI must ask
 * for confirmation. Compatibility itself is only enforced by the server at
 * registration time; a 409 there is parsed into violation rows.
 */
import {
  RegistryError,
  parseViolations,
  type CompatViolation,
  type RegistryApi,
  type SchemaReference,
} from './api'

export type CheckOutcome =
  /** Identical schema already registered — registering changes nothing. */
  | { kind: 'unchanged'; version: number; id: number }
  /** Subject does not exist yet — registering creates it at version 1. */
  | { kind: 'new-subject' }
  /** Schema differs from every stored version — registering creates a new version. */
  | { kind: 'will-register' }
  /** The candidate (or its references) is invalid — 42201. */
  | { kind: 'invalid'; message: string }

export async function checkCandidate(
  api: RegistryApi,
  subject: string,
  schema: string,
  references: SchemaReference[],
): Promise<CheckOutcome> {
  try {
    const match = await api.lookup(subject, schema, references)
    return { kind: 'unchanged', version: match.version, id: match.id }
  } catch (e) {
    if (e instanceof RegistryError) {
      if (e.isUnknownSubject) return { kind: 'new-subject' }
      if (e.isSchemaNotFound) return { kind: 'will-register' }
      if (e.status === 422) return { kind: 'invalid', message: e.message }
    }
    throw e
  }
}

export type RegisterOutcome =
  | { kind: 'registered'; id: number }
  | { kind: 'incompatible'; violations: CompatViolation[]; message: string }
  | { kind: 'invalid'; message: string }

export async function registerCandidate(
  api: RegistryApi,
  subject: string,
  schema: string,
  references: SchemaReference[],
): Promise<RegisterOutcome> {
  try {
    const { id } = await api.register(subject, schema, references)
    return { kind: 'registered', id }
  } catch (e) {
    if (e instanceof RegistryError) {
      if (e.isIncompatible) {
        return { kind: 'incompatible', violations: parseViolations(e.message), message: e.message }
      }
      if (e.status === 422) return { kind: 'invalid', message: e.message }
    }
    throw e
  }
}
