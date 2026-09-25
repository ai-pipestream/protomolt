// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import TaskConsoleView from './TaskConsoleView.vue'
import { taskApi } from '../services/tasks'
import { installDomStubs, routerForTests, vuetifyForTests } from '../componentTestKit'

vi.mock('../services/tasks', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../services/tasks')>()
  return {
    ...actual,
    taskApi: {
      sessionStatus: vi.fn(),
      login: vi.fn(),
      logout: vi.fn(),
      listTasks: vi.fn(),
      listWorkers: vi.fn(),
      task: vi.fn(),
      watchEvents: vi.fn(),
      sendMessage: vi.fn(),
      reviewAccept: vi.fn(),
      reviewRevise: vi.fn(),
      offerTask: vi.fn(),
      exportRecord: vi.fn(),
      cancelTask: vi.fn(),
    },
  }
})

const api = taskApi as unknown as Record<string, ReturnType<typeof vi.fn>>

installDomStubs()

const task = {
  taskId: 'task-1',
  phase: 'candidate',
  attempt: 1,
  workerId: 'worker-a',
  objective: 'Wire the console',
  candidateRevision: 1,
  lastProgressSeq: 0,
  lastCheckpointSeq: 3,
  lastCursor: 2,
}

const offerEvent = {
  cursor: 1,
  workerId: 'worker-a',
  taskId: 'task-1',
  lane: 'LANE_COORDINATOR',
  entry: {
    coordinatorFrame: {
      offer: {
        spec: {
          objective: 'Wire the console',
          requiredChecks: [
            { name: 'tests-green', description: 'vitest run passes' },
            { name: 'lint-clean', description: 'eslint reports nothing' },
          ],
        },
      },
    },
  },
}

const completionEvent = {
  cursor: 2,
  workerId: 'worker-a',
  taskId: 'task-1',
  lane: 'LANE_WORKER',
  entry: {
    workerFrame: {
      completion: {
        revision: 1,
        attempt: 1,
        summary: 'Console wired and verified',
        evidence: [
          { checkName: 'tests-green', verdict: 'CHECK_VERDICT_PASSED', detail: 'all pass' },
        ],
      },
    },
  },
}

async function mountView() {
  const router = routerForTests()
  await router.push('/tasks')
  await router.isReady()
  const wrapper = mount(TaskConsoleView, {
    global: { plugins: [vuetifyForTests(), router] },
  })
  await flushPromises()
  return wrapper
}

beforeEach(() => {
  vi.clearAllMocks()
  api.sessionStatus.mockResolvedValue({ authenticated: true, loginRequired: true })
  api.listTasks.mockResolvedValue({ tasks: [task], cursor: 2, findings: [] })
  api.listWorkers.mockResolvedValue([])
  api.task.mockResolvedValue({
    task,
    events: [offerEvent, completionEvent],
    cursor: 2,
    findings: [],
  })
  // The long-poll loop must not spin: the first watch call simply never settles.
  api.watchEvents.mockImplementation(() => new Promise(() => {}))
  api.reviewAccept.mockResolvedValue({ decision: 'accept', phase: 'accepted' })
  api.reviewRevise.mockResolvedValue({ decision: 'revise', phase: 'working' })
})

describe('TaskConsoleView', () => {
  it('renders the contract of done and the candidate review panel', async () => {
    const wrapper = await mountView()
    const text = wrapper.text()
    expect(text).toContain('Contract of done')
    expect(text).toContain('tests-green')
    expect(text).toContain('lint-clean')
    expect(text).toContain('Candidate revision 1 awaits your judgement')
    expect(text).toContain('Console wired and verified')
    expect(wrapper.find('.review-panel').exists()).toBe(true)
  })

  it('accepts the candidate only once a verdict is typed', async () => {
    const wrapper = await mountView()
    const accept = wrapper.findAll('button').find((b) => b.text().includes('Accept the work'))!
    expect(accept.attributes('disabled')).toBeDefined()

    await wrapper.find('.review-panel input').setValue('done to my satisfaction')
    expect(accept.attributes('disabled')).toBeUndefined()
    await accept.trigger('click')
    await flushPromises()
    expect(api.reviewAccept).toHaveBeenCalledWith('task-1', 1, 1, 'done to my satisfaction')
  })

  it('requests a revision with feedback and the toggled failed checks', async () => {
    const wrapper = await mountView()
    await wrapper.find('.review-panel textarea').setValue('lint still complains')

    const failedChip = wrapper
      .findAll('.review-panel .v-chip')
      .find((chip) => chip.text() === 'lint-clean')!
    await failedChip.trigger('click')

    await wrapper.findAll('button').find((b) => b.text().includes('Request revision'))!
      .trigger('click')
    await flushPromises()
    expect(api.reviewRevise).toHaveBeenCalledWith('task-1', 1, 1, 'lint still complains', ['lint-clean'])
  })

  it('clears a review draft when a newer candidate arrives', async () => {
    let resolveWatch!: ((value: { events: typeof completionEvent[]; cursor: number; truncated: boolean }) => void)
    api.watchEvents.mockImplementationOnce(() => new Promise((resolve) => {
      resolveWatch = resolve
    }))
    const wrapper = await mountView()

    await wrapper.find('.review-panel input').setValue('approve revision one')
    await wrapper.find('.review-panel textarea').setValue('fix the edge case')
    await wrapper.findAll('.review-panel .v-chip').find((chip) => chip.text() === 'lint-clean')!
      .trigger('click')

    resolveWatch({
      events: [{
        ...completionEvent,
        cursor: 3,
        entry: { workerFrame: { completion: {
          ...completionEvent.entry.workerFrame.completion,
          revision: 2,
          summary: 'New evidence for the corrected candidate',
        } } },
      }],
      cursor: 3,
      truncated: false,
    })
    await flushPromises()

    expect(wrapper.text()).toContain('Candidate revision 2 awaits your judgement')
    expect((wrapper.find('.review-panel input').element as HTMLInputElement).value).toBe('')
    expect((wrapper.find('.review-panel textarea').element as HTMLTextAreaElement).value).toBe('')
    expect(wrapper.findAll('.review-panel .v-chip').find((chip) => chip.text() === 'lint-clean')!
      .classes()).not.toContain('v-chip--variant-flat')
  })

  it('offers a task through the dialog with its contract of done', async () => {
    api.sessionStatus.mockResolvedValue({ authenticated: true, loginRequired: true })
    api.listTasks.mockResolvedValue({ tasks: [], cursor: 0, findings: [] })
    api.listWorkers.mockResolvedValue([
      { workerId: 'worker-a', admitted: true, connected: true,
        provider: 'scripted', model: 'm', capabilities: [] },
    ])
    api.offerTask.mockResolvedValue({ taskId: 'task-new', workerId: 'worker-a' })
    const wrapper = await mountView()

    await wrapper.findAll('button').find((b) => b.text().includes('Offer a task'))!
      .trigger('click')
    await flushPromises()
    const dialog = wrapper.getComponent({ name: 'VDialog' })
    await dialog.findComponent({ name: 'VSelect' }).setValue('worker-a')
    await dialog.findComponent({ name: 'VTextarea' }).setValue('Prove the offer lane')
    // VSelect renders an internal VTextField, so the row is: select, scopes,
    // check name, check description, lease minutes.
    const checkFields = dialog.findAllComponents({ name: 'VTextField' })
    await checkFields[2].setValue('unit-tests')
    await checkFields[3].setValue('focused tests pass')
    // The dialog teleports its DOM, so buttons are found through the
    // component tree rather than the wrapper's subtree.
    await dialog.findAllComponents({ name: 'VBtn' })
      .find((b) => b.text() === 'Offer')!.trigger('click')
    await flushPromises()

    expect(api.offerTask).toHaveBeenCalledWith('worker-a', 'Prove the offer lane',
      [{ name: 'unit-tests', description: 'focused tests pass' }], [], 30, {})
  })

  it('shows the signed-record export only for a terminal task', async () => {
    api.sessionStatus.mockResolvedValue({ authenticated: true, loginRequired: true })
    api.listWorkers.mockResolvedValue([])
    api.listTasks.mockResolvedValue({
      tasks: [{ ...task, phase: 'accepted' }], cursor: 0, findings: [] })
    api.task.mockResolvedValue({
      task: { ...task, phase: 'accepted' }, events: [offerEvent], cursor: 1, findings: [] })
    api.watchEvents.mockReturnValue(new Promise(() => {}))
    const wrapper = await mountView()
    expect(wrapper.findAll('button').some((b) => b.text().includes('Signed record')))
      .toBe(true)

    // An in-flight task offers no receipt.
    api.listTasks.mockResolvedValue({ tasks: [task], cursor: 0, findings: [] })
    api.task.mockResolvedValue({ task, events: [offerEvent], cursor: 1, findings: [] })
    const inFlight = await mountView()
    expect(inFlight.findAll('button').some((b) => b.text().includes('Signed record')))
      .toBe(false)
  })
})

describe('coordination starter controls', () => {
  it('shows typed results alongside the exact candidate identity', async () => {
    const typed = { ...completionEvent, entry: { workerFrame: { completion: {
      ...completionEvent.entry.workerFrame.completion,
      result: { '@type': 'type.googleapis.com/caller.Report', headline: 'Caller-defined result' },
    } } } }
    api.task.mockResolvedValue({ task, events: [offerEvent, typed], cursor: 2, findings: [] })
    const wrapper = await mountView()
    expect(wrapper.find('.typed-result').text()).toContain('Caller-defined result')
    expect(wrapper.text()).toContain('Responsible: Reviewer')
    wrapper.unmount()
  })

  it('shows a useful next action for a leased task and cancels with a reason', async () => {
    const leased = { ...task, phase: 'leased' }
    api.listTasks.mockResolvedValue({ tasks: [leased], cursor: 1, findings: [] })
    api.task.mockResolvedValue({ task: leased, events: [offerEvent], cursor: 1, findings: [] })
    api.cancelTask.mockResolvedValue({ taskId: task.taskId })
    const wrapper = await mountView()
    expect(wrapper.text()).toContain('has not submitted a current candidate')
    expect(wrapper.text()).toContain('Responsible: worker-a')
    await wrapper.findAllComponents({ name: 'VTextField' })
      .find((field) => field.props('label') === 'Reason to cancel')!.setValue('Change scope before retrying')
    await wrapper.findAll('button').find((button) => button.text() === 'Cancel attempt')!.trigger('click')
    await flushPromises()
    expect(api.cancelTask).toHaveBeenCalledWith('task-1', 'Change scope before retrying')
    wrapper.unmount()
  })

  it('loads the sample descriptor and sends its contract with the example offer', async () => {
    api.listTasks.mockResolvedValue({ tasks: [], cursor: 0, findings: [] })
    api.listWorkers.mockResolvedValue([{ workerId: 'fixture-worker', admitted: true,
      connected: true, provider: 'fixture', model: '', capabilities: [] }])
    api.offerTask.mockResolvedValue({ taskId: 'new-task', workerId: 'fixture-worker' })
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: true,
      arrayBuffer: async () => new Uint8Array([1, 2, 3]).buffer }))
    const wrapper = await mountView()
    try {
      await wrapper.findAll('button').find((button) => button.text() === 'Offer a task')!.trigger('click')
      await flushPromises()
      const dialog = wrapper.getComponent({ name: 'VDialog' })
      await dialog.findAllComponents({ name: 'VBtn' })
        .find((button) => button.text() === 'Use coordination example')!.trigger('click')
      await flushPromises()
      expect(dialog.findComponent({ name: 'VCardText' }).text()).toContain('no model calls or code execution')
      await dialog.findAllComponents({ name: 'VBtn' })
        .find((button) => button.text() === 'Offer')!.trigger('click')
      await flushPromises()
      expect(api.offerTask).toHaveBeenCalledWith('fixture-worker',
        'Produce a coordination report about this starter task.',
        expect.arrayContaining([expect.objectContaining({ name: 'fixture-report-valid' })]), [], 30,
        { contract: { descriptorSet: 'AQID', typeName: 'ai.protomolt.proto.samples.starter.v1.CoordinationReport' } })
    } finally {
      wrapper.unmount()
      vi.unstubAllGlobals()
    }
  })
})

describe('task selection isolation', () => {
  const other = { ...task, taskId: 'task-2', objective: 'A different task', lastCursor: 1 }
  const otherEvent = { ...completionEvent, taskId: other.taskId, entry: { workerFrame: {
    completion: { ...completionEvent.entry.workerFrame.completion, summary: 'Candidate belonging to task two' },
  } } }

  it('discards a detail response after the user has selected another task', async () => {
    let resolveOther!: (value: unknown) => void
    api.listTasks.mockResolvedValue({ tasks: [task, other], cursor: 2, findings: [] })
    api.task.mockImplementation((id: string) => id === other.taskId
      ? new Promise((resolve) => { resolveOther = resolve })
      : Promise.resolve({ task, events: [offerEvent, completionEvent], cursor: 2, findings: [] }))
    const wrapper = await mountView()
    await wrapper.findAllComponents({ name: 'VListItem' })
      .find((item) => item.text().includes(other.objective))!.trigger('click')
    await flushPromises()
    await wrapper.findAllComponents({ name: 'VListItem' })
      .find((item) => item.text().includes(task.objective))!.trigger('click')
    await flushPromises()
    resolveOther({ task: other, events: [otherEvent], cursor: 2, findings: [] })
    await flushPromises()
    expect(wrapper.text()).not.toContain('Candidate belonging to task two')
    expect(wrapper.find('.review-panel').text()).toContain('Console wired and verified')
    wrapper.unmount()
  })

  it('discards a long-poll reply for the task that was left', async () => {
    let resolveWatch!: (value: unknown) => void
    api.listTasks.mockResolvedValue({ tasks: [task, other], cursor: 2, findings: [] })
    api.watchEvents.mockImplementationOnce(() => new Promise((resolve) => { resolveWatch = resolve }))
    const wrapper = await mountView()
    api.task.mockResolvedValue({ task: other, events: [otherEvent], cursor: 2, findings: [] })
    await wrapper.findAllComponents({ name: 'VListItem' })
      .find((item) => item.text().includes(other.objective))!.trigger('click')
    await flushPromises()
    resolveWatch({ events: [{ ...completionEvent, cursor: 99 }], cursor: 99, truncated: false })
    await flushPromises()
    expect(wrapper.find('.review-panel').text()).toContain('Candidate belonging to task two')
    expect(wrapper.find('.review-panel').text()).not.toContain('Console wired and verified')
    wrapper.unmount()
  })
})
