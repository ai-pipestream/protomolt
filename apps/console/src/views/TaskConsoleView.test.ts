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
    expect(api.reviewAccept).toHaveBeenCalledWith('task-1', 'done to my satisfaction')
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
    expect(api.reviewRevise).toHaveBeenCalledWith('task-1', 'lint still complains', ['lint-clean'])
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
      [{ name: 'unit-tests', description: 'focused tests pass' }], [], 30)
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
