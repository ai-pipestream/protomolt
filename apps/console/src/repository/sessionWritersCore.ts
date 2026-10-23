/**
 * What a signed-in session may write: the intake writer scopes its principal's
 * account binding lists, as the session status names them. The host checks
 * writer authority on every save; this only tells the person beforehand, with
 * the same coverage rule the host applies to an intake writer grant.
 */
import type { IntakeWriterScope } from '@/gen/ai/protomolt/proto/authz/v1/account_bindings_pb'

/** One scope as the screens list it: `ds-files` or `ds-files via crawler`, and whether it creates. */
export function scopeLabel(scope: IntakeWriterScope): string {
  const owner = scope.connectorId === undefined ? scope.datasourceId : `${scope.datasourceId} via ${scope.connectorId}`
  return scope.allowCreate ? `${owner} (create and update)` : `${owner} (update only)`
}

/** The scopes in one account. */
export function scopesIn(scopes: IntakeWriterScope[], accountId: string): IntakeWriterScope[] {
  return scopes.filter((s) => s.accountId === accountId)
}

/**
 * The scope that covers an intake save, or null. A scope covers its datasource
 * through any connector when it names none, and only its own connector when it
 * names one; creating a document also needs allow_create.
 */
export function coveringScope(
  scopes: IntakeWriterScope[],
  target: { accountId: string; datasourceId: string; connectorId: string; creating: boolean },
): IntakeWriterScope | null {
  for (const scope of scopes) {
    if (scope.accountId !== target.accountId || scope.datasourceId !== target.datasourceId) continue
    if (scope.connectorId !== undefined && scope.connectorId !== target.connectorId) continue
    if (target.creating && !scope.allowCreate) continue
    return scope
  }
  return null
}
