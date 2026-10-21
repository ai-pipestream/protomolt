// @vitest-environment jsdom
import { create } from '@bufbuild/protobuf'
import { Code, ConnectError } from '@connectrpc/connect'
import { describe, expect, it } from 'vitest'
import { AccountSchema, AccountStatus } from '@/gen/ai/protomolt/proto/account/v1/account_pb'
import { AccountService, ListAccountsResponseSchema } from '@/gen/ai/protomolt/proto/account/v1/account_service_pb'
import RepositoryAccountsView from '@/views/repository/RepositoryAccountsView.vue'
import { mountRepositoryView, settle } from '@/repository/repositoryViewKit'

describe('RepositoryAccountsView', () => {
  it('lists the accounts across pages and links each to its documents', async () => {
    const { wrapper } = await mountRepositoryView(RepositoryAccountsView, '/repository', (router) =>
      router.service(AccountService, {
        listAccounts(request) {
          return request.continuationToken === ''
            ? create(ListAccountsResponseSchema, {
                accounts: [create(AccountSchema, { accountId: 'acct-a', displayName: 'Alpha', status: AccountStatus.ACTIVE })],
                nextContinuationToken: 'p2',
              })
            : create(ListAccountsResponseSchema, { accounts: [create(AccountSchema, { accountId: 'acct-b', status: AccountStatus.DEACTIVATED })] })
        },
      }),
    )
    const list = wrapper.get('[data-testid="account-list"]')
    expect(list.text()).toContain('Alpha')
    expect(list.text()).toContain('acct-a · ACTIVE')
    expect(list.text()).toContain('acct-b · DEACTIVATED')
    expect(list.find('a[href="/repository/accounts/acct-a"]').exists()).toBe(true)
    wrapper.unmount()
  })

  it('shows the gateway status when AccountService is not served, and still opens an account by ID', async () => {
    const { wrapper, router } = await mountRepositoryView(RepositoryAccountsView, '/repository', () => {})
    const failure = wrapper.get('[data-testid="rpc-failure"]')
    expect(failure.text()).toContain('Listing accounts')
    expect(failure.text()).toContain('UNIMPLEMENTED')
    await wrapper.get('[data-testid="account-id-input"] input').setValue('acct-z')
    await wrapper.get('[data-testid="open-account"]').trigger('click')
    await settle()
    expect(router.currentRoute.value.fullPath).toBe('/repository/accounts/acct-z')
    wrapper.unmount()
  })

  it('shows a PERMISSION_DENIED with the server message, not an empty list', async () => {
    const { wrapper } = await mountRepositoryView(RepositoryAccountsView, '/repository', (router) =>
      router.service(AccountService, {
        listAccounts() {
          throw new ConnectError('no account binding for principal ada', Code.PermissionDenied)
        },
      }),
    )
    expect(wrapper.get('[data-testid="rpc-failure"]').text()).toContain('no account binding for principal ada')
    expect(wrapper.text()).not.toContain('No accounts are visible')
    wrapper.unmount()
  })
})
