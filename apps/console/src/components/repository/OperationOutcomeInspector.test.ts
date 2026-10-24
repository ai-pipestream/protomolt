// @vitest-environment jsdom
import { create } from '@bufbuild/protobuf'
import { mount } from '@vue/test-utils'
import { describe, expect, it } from 'vitest'
import { installDomStubs, vuetifyForTests } from '@/componentTestKit'
import {
  DocumentService,
  GetOperationOutcomeResponseSchema,
  NoRecordedOutcomeSchema,
  RecordedOutcomeSchema,
  type GetOperationOutcomeRequest,
} from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import { FieldViolationSchema, RejectionReason, SaveRejectionSchema } from '@/gen/ai/protomolt/proto/repo/v2/errors_pb'
import { AccessionSchema } from '@/gen/ai/protomolt/proto/repo/v2/manifest_pb'
import { REPOSITORY_CLIENTS } from '@/repository/clients'
import { at, routerClients, testAddress } from '@/repository/repositoryTestKit'
import { repositoryRouter, settle } from '@/repository/repositoryViewKit'
import OperationOutcomeInspector from './OperationOutcomeInspector.vue'

type Response = ReturnType<typeof create<typeof GetOperationOutcomeResponseSchema>>

async function mounted(
  answer: (request: GetOperationOutcomeRequest) => Response,
  props: { accountId: string; initialOperationId?: string },
) {
  installDomStubs()
  const seen: GetOperationOutcomeRequest[] = []
  const router = repositoryRouter()
  await router.push({ name: 'repository' })
  await router.isReady()
  const wrapper = mount(OperationOutcomeInspector, {
    props,
    global: {
      plugins: [vuetifyForTests(), router],
      provide: {
        [REPOSITORY_CLIENTS as symbol]: routerClients((routes) =>
          routes.service(DocumentService, {
            getOperationOutcome: (request) => {
              seen.push(request)
              return answer(request)
            },
          })),
      },
    },
    attachTo: document.body,
  })
  await settle()
  return { wrapper, seen }
}

const committed: Response = create(GetOperationOutcomeResponseSchema, {
  state: {
    case: 'committed',
    value: create(RecordedOutcomeSchema, {
      result: {
        case: 'accession',
        value: create(AccessionSchema, {
          address: testAddress({ accountId: 'acct-a', docId: 'doc-1', graphAddressId: 'ds-1' }),
          revision: 1n,
          previousRevision: 0n,
        }),
      },
      recordedAt: at('2026-10-01T00:00:00Z'),
    }),
  },
})

const none: Response = create(GetOperationOutcomeResponseSchema, {
  state: { case: 'none', value: create(NoRecordedOutcomeSchema) },
})

describe('OperationOutcomeInspector', () => {
  it('looks an initial operation ID up on mount and links the revision', async () => {
    const { wrapper, seen } = await mounted(() => committed,
        { accountId: 'acct-a', initialOperationId: 'op-1' })
    expect(seen).toHaveLength(1)
    expect(seen[0]).toMatchObject({ accountId: 'acct-a', operationId: 'op-1' })
    const panel = wrapper.get('[data-testid="outcome-committed"]')
    expect(panel.text()).toContain('Revision 1 of doc-1')
    expect(wrapper.get('[data-testid="outcome-new"]').text()).toContain('new document')
    expect(wrapper.get('[data-testid="outcome-open"]').attributes('href')).toContain('revision=1')
    wrapper.unmount()
  })

  it('renders a recorded unchanged outcome distinctly from a new or replaced revision and links the matched revision', async () => {
    const unchanged: Response = create(GetOperationOutcomeResponseSchema, {
      state: {
        case: 'committed',
        value: create(RecordedOutcomeSchema, {
          result: {
            case: 'unchanged',
            value: create(AccessionSchema, {
              address: testAddress({ accountId: 'acct-a', docId: 'doc-1', graphAddressId: 'ds-1' }),
              revision: 3n,
              previousRevision: 3n,
            }),
          },
          recordedAt: at('2026-10-01T00:00:00Z'),
        }),
      },
    })
    const { wrapper } = await mounted(() => unchanged, { accountId: 'acct-a', initialOperationId: 'op-unchanged' })
    const panel = wrapper.get('[data-testid="outcome-committed"]')
    expect(panel.text()).toContain('Unchanged — matched revision 3 of doc-1 (no new revision)')
    expect(wrapper.get('[data-testid="outcome-unchanged"]').text()).toContain('matched revision 3 (no new revision)')
    expect(wrapper.find('[data-testid="outcome-new"]').exists()).toBe(false)
    expect(wrapper.find('[data-testid="outcome-replaced"]').exists()).toBe(false)
    expect(wrapper.get('[data-testid="outcome-open"]').attributes('href')).toContain('revision=3')
    wrapper.unmount()
  })

  it('looks up a later initial operation ID and ignores a cleared one', async () => {
    const { wrapper, seen } = await mounted(() => committed,
        { accountId: 'acct-a', initialOperationId: 'op-1' })
    expect(seen).toHaveLength(1)
    await wrapper.setProps({ initialOperationId: 'op-9' })
    await settle()
    expect(seen).toHaveLength(2)
    expect(seen[1]).toMatchObject({ accountId: 'acct-a', operationId: 'op-9' })
    await wrapper.setProps({ initialOperationId: '' })
    await settle()
    expect(seen).toHaveLength(2)
    wrapper.unmount()
  })

  it('shows a recorded rejection with its reason and violations', async () => {
    const rejected: Response = create(GetOperationOutcomeResponseSchema, {
      state: {
        case: 'committed',
        value: create(RecordedOutcomeSchema, {
          result: {
            case: 'rejection',
            value: create(SaveRejectionSchema, {
              reason: RejectionReason.VALIDATION_FAILED,
              message: 'the title is blank',
              violations: [create(FieldViolationSchema, {
                fieldPath: 'search_metadata.title', ruleId: 'string.min_len', message: 'must not be blank',
              })],
            }),
          },
          recordedAt: at('2026-10-02T00:00:00Z'),
        }),
      },
    })
    const { wrapper } = await mounted(() => rejected, { accountId: 'acct-a', initialOperationId: 'op-2' })
    const panel = wrapper.get('[data-testid="outcome-rejected"]')
    expect(panel.text()).toContain('VALIDATION_FAILED')
    expect(panel.text()).toContain('the title is blank')
    expect(panel.text()).toContain('string.min_len search_metadata.title must not be blank')
    wrapper.unmount()
  })

  it('explains the no-outcome state after a manual lookup', async () => {
    const { wrapper, seen } = await mounted(() => none, { accountId: 'acct-a' })
    expect(seen).toHaveLength(0) // no initial ID, nothing asked on mount
    await wrapper.get('[data-testid="outcome-operation"] input').setValue('op-3')
    await wrapper.get('[data-testid="outcome-lookup"]').trigger('click')
    await settle()
    expect(seen).toHaveLength(1)
    expect(wrapper.get('[data-testid="outcome-none"]').text()).toContain('may still commit later')
    wrapper.unmount()
  })

  it('refuses a blank operation ID without a request', async () => {
    const { wrapper, seen } = await mounted(() => none, { accountId: 'acct-a' })
    await wrapper.get('[data-testid="outcome-lookup"]').trigger('click')
    await settle()
    expect(seen).toHaveLength(0)
    expect(wrapper.get('[data-testid="outcome-input-error"]').text()).toContain('Operation ID is blank')
    wrapper.unmount()
  })
})
