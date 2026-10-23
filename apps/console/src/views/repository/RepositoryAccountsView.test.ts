// @vitest-environment jsdom
import { create } from '@bufbuild/protobuf'
import { describe, expect, it, vi } from 'vitest'
import { IntakeWriterScopeSchema } from '@/gen/ai/protomolt/proto/authz/v1/account_bindings_pb'
import { ConsoleSessionStatusSchema, type ConsoleSessionStatus } from '@/gen/ai/protomolt/proto/authz/v1/console_session_pb'
import { REPOSITORY_SESSION, REPOSITORY_SIGN_IN } from '@/repository/clients'
import { mountRepositoryView, settle } from '@/repository/repositoryViewKit'
import { SessionApiError } from '@/services/session'
import RepositoryAccountsView from './RepositoryAccountsView.vue'

async function mountWith(status: () => Promise<ConsoleSessionStatus>, signIn = vi.fn()) {
  const mounted = await mountRepositoryView(RepositoryAccountsView, '/repository', () => {}, {
    [REPOSITORY_SESSION as symbol]: { status },
    [REPOSITORY_SIGN_IN as symbol]: signIn,
  })
  return { ...mounted, signIn }
}

describe('RepositoryAccountsView', () => {
  it("lists the session's bound accounts and links each to its documents", async () => {
    const { wrapper } = await mountWith(async () =>
      create(ConsoleSessionStatusSchema, { authenticated: true, principal: 'ada', accountIds: ['acct-a', 'acct-b'] }),
    )
    expect(wrapper.get('[data-testid="session-principal"]').text()).toContain('ada')
    const list = wrapper.get('[data-testid="account-list"]')
    expect(list.text()).toContain('acct-a')
    expect(list.text()).toContain('acct-b')
    expect(list.find('a[href="/repository/accounts/acct-b"]').exists()).toBe(true)
    wrapper.unmount()
  })

  it('says so when the session is bound to no account', async () => {
    const { wrapper } = await mountWith(async () => create(ConsoleSessionStatusSchema, { authenticated: true, principal: 'ada' }))
    expect(wrapper.get('[data-testid="no-accounts"]').text()).toContain('bound to no accounts')
    wrapper.unmount()
  })

  it('offers sign-in when there is no session, and still opens an account by ID', async () => {
    const { wrapper, router, signIn } = await mountWith(async () => create(ConsoleSessionStatusSchema, { authenticated: false }))
    expect(wrapper.find('[data-testid="signed-out"]').exists()).toBe(true)
    await wrapper.get('[data-testid="sign-in"]').trigger('click')
    expect(signIn).toHaveBeenCalledTimes(1)
    await wrapper.get('[data-testid="account-id-input"] input').setValue('acct-z')
    await wrapper.get('[data-testid="open-account"]').trigger('click')
    await settle()
    expect(router.currentRoute.value.fullPath).toBe('/repository/accounts/acct-z')
    wrapper.unmount()
  })

  it('shows a session read the host did not answer, by its HTTP status', async () => {
    const { wrapper } = await mountWith(async () => {
      throw new SessionApiError(503, '/console/session answered 503: identity provider unreachable')
    })
    const failure = wrapper.get('[data-testid="rpc-failure"]').text()
    expect(failure).toContain('HTTP 503')
    expect(failure).toContain('identity provider unreachable')
    expect(wrapper.find('[data-testid="signed-out"]').exists()).toBe(false)
    wrapper.unmount()
  })

  it('says per account whether the session reads only or writes as an intake owner', async () => {
    const { wrapper } = await mountWith(async () =>
      create(ConsoleSessionStatusSchema, {
        authenticated: true,
        principal: 'ada',
        accountIds: ['acct-a', 'acct-b'],
        intakeWriters: [create(IntakeWriterScopeSchema, { accountId: 'acct-a', datasourceId: 'ds-files', connectorId: 'crawler', allowCreate: true })],
      }),
    )
    expect(wrapper.get('[data-testid="account-acct-a"]').text()).toContain('Writes as ds-files via crawler (create and update)')
    expect(wrapper.get('[data-testid="account-acct-b"]').text()).toContain('Read only')
    wrapper.unmount()
  })
})
