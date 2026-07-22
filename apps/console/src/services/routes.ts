/**
 * Route helpers for the schema registry console. Subjects may contain
 * slashes (import-path subjects like "example/person.proto"), so subject
 * links are built as percent-encoded PATH STRINGS — encodeURIComponent keeps
 * the whole subject a single path segment (`%2F` inside), which the
 * catch-all `:subject(.*)` route param decodes back on match.
 */
import type { RouteLocationRaw } from 'vue-router'

export const SCHEMA_REGISTRY_BASE = '/schema-registry'

export function subjectRoute(subject: string, version?: number): RouteLocationRaw {
  return {
    path: `${SCHEMA_REGISTRY_BASE}/subjects/${encodeURIComponent(subject)}`,
    ...(version !== undefined ? { query: { v: String(version) } } : {}),
  }
}
