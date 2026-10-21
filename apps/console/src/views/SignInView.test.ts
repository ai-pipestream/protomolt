// @vitest-environment jsdom
import { create } from '@bufbuild/protobuf'
import { flushPromises, mount } from '@vue/test-utils'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { ConsoleSessionStatusSchema } from '@/gen/ai/protomolt/proto/authz/v1/console_session_pb'
import { installDomStubs, routerForTests, vuetifyForTests } from '../componentTestKit'
import { SESSION_PATH, SessionApi, SessionApiError } from '../services/session'
import SignInView from './SignInView.vue'

installDomStubs()

function apiWith(status: () => Promise<ReturnType<typeof create<typeof ConsoleSessionStatusSchema>>>, logout = vi.fn()) {
  const api = new SessionApi()
  api.status = vi.fn(status)
  api.logout = logout
  return api
}

async function mountView(api: SessionApi, query = '') {
  const router = routerForTests()
  await router.push(`/sign-in${query}`)
  await router.isReady()
  const wrapper = mount(SignInView, {
    props: { api },
    global: { plugins: [vuetifyForTests(), router] },
  })
  await flushPromises()
  return wrapper
}

beforeEach(() => {
  vi.clearAllMocks()
})

describe('SignInView', () => {
  it('offers the host login as a full navigation carrying the return path', async () => {
    const api = apiWith(async () => create(ConsoleSessionStatusSchema, { authenticated: false }))
    const wrapper = await mountView(api, '?return=%2Fconsole%2Frepository%2Faccounts%2Fa')
    const button = wrapper.find('[data-test="sign-in"]')
    expect(button.exists()).toBe(true)
    expect(button.attributes('href')).toBe(
      `${SESSION_PATH}/login?return=${encodeURIComponent('/console/repository/accounts/a')}`,
    )
    expect(wrapper.find('[data-test="failure"]').exists()).toBe(false)
    expect(wrapper.text()).toContain('never holds a token')
  })

  it('shows the reason a login failed, as the host named it', async () => {
    const api = apiWith(async () => create(ConsoleSessionStatusSchema, { authenticated: false }))
    const wrapper = await mountView(api, '?error=SIGN_IN_FAILURE_LOGIN_EXPIRED')
    expect(wrapper.find('[data-test="failure"]').text()).toContain('took too long')
    expect(wrapper.find('[data-test="sign-in"]').exists()).toBe(true)
  })

  it('shows a live session with its accounts, a way on and a way out', async () => {
    const logout = vi.fn(async () => {})
    let authenticated = true
    const api = apiWith(
      async () =>
        create(ConsoleSessionStatusSchema, {
          authenticated,
          principal: authenticated ? 'alice' : '',
          accountIds: authenticated ? ['acct-a', 'acct-b'] : [],
        }),
      logout,
    )
    const wrapper = await mountView(api, '?return=%2Fconsole%2Frepository')
    expect(wrapper.find('[data-test="signed-in"]').text()).toContain('alice')
    expect(wrapper.find('[data-test="signed-in"]').text()).toContain('2 accounts (acct-a, acct-b)')
    expect(wrapper.find('[data-test="continue"]').attributes('href')).toBe('/repository')
    expect(wrapper.find('[data-test="sign-in"]').exists()).toBe(false)

    authenticated = false
    await wrapper.find('[data-test="sign-out"]').trigger('click')
    await flushPromises()
    expect(logout).toHaveBeenCalledTimes(1)
    expect(wrapper.find('[data-test="sign-in"]').exists()).toBe(true)
  })

  it('names a host that cannot answer for its session', async () => {
    const api = apiWith(async () => {
      throw new SessionApiError(503, `${SESSION_PATH} answered 503: console sign-in is not configured`)
    })
    const wrapper = await mountView(api)
    expect(wrapper.find('[data-test="host-error"]').text()).toContain('not configured')
    expect(wrapper.find('[data-test="sign-in"]').exists()).toBe(true)
  })
})
