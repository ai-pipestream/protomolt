import { describe, expect, it } from 'vitest'

import {
  checkStatuses,
  frameFacts,
  frameKind,
  frameText,
  latestCandidate,
  transcriptText,
  type TaskEvent,
  type TaskSummary,
} from './tasks'

function coordinatorEvent(cursor: number, frame: Record<string, unknown>): TaskEvent {
  return {
    cursor,
    workerId: 'worker-1',
    taskId: 'task-1',
    lane: 'LANE_COORDINATOR',
    entry: { coordinatorFrame: frame },
  }
}

function workerEvent(cursor: number, frame: Record<string, unknown>): TaskEvent {
  return {
    cursor,
    workerId: 'worker-1',
    taskId: 'task-1',
    lane: 'LANE_WORKER',
    entry: { workerFrame: frame },
  }
}

function offerEvent(cursor: number, requiredChecks: Array<Record<string, string>>): TaskEvent {
  return coordinatorEvent(cursor, {
    offer: { spec: { objective: 'Prove the derivations', requiredChecks } },
  })
}

function completionEvent(
  cursor: number,
  completion: Record<string, unknown>,
): TaskEvent {
  return workerEvent(cursor, { completion })
}

const task: TaskSummary = {
  taskId: 'task-1',
  phase: 'PHASE_REVIEW',
  attempt: 2,
  workerId: 'worker-1',
  objective: 'Prove the derivations',
  candidateRevision: 2,
  lastProgressSeq: 0,
  lastCheckpointSeq: 0,
  lastCursor: 4,
}

describe('checkStatuses', () => {
  it('joins the latest candidate evidence to the offer checks', () => {
    const events = [
      offerEvent(1, [
        { name: 'tests-green', description: 'vitest suite passes' },
        { name: 'lint-clean', description: 'no lint findings' },
      ]),
      // An older candidate whose evidence must NOT win the join.
      completionEvent(2, {
        revision: 1,
        attempt: 1,
        summary: 'first try',
        evidence: [
          { checkName: 'tests-green', verdict: 'CHECK_VERDICT_FAILED', detail: '2 failing' },
          { checkName: 'lint-clean', verdict: 'CHECK_VERDICT_PASSED', detail: 'clean' },
        ],
      }),
      completionEvent(3, {
        revision: 2,
        attempt: 2,
        summary: 'second try',
        evidence: [
          { checkName: 'tests-green', verdict: 'CHECK_VERDICT_PASSED', detail: 'all green' },
          { checkName: 'lint-clean', verdict: 'CHECK_VERDICT_FAILED', detail: '1 finding' },
        ],
      }),
    ]

    expect(checkStatuses(events)).toEqual([
      {
        name: 'tests-green',
        description: 'vitest suite passes',
        status: 'passed',
        detail: 'all green',
      },
      {
        name: 'lint-clean',
        description: 'no lint findings',
        status: 'failed',
        detail: '1 finding',
      },
    ])
  })

  it("marks a check with no evidence 'unproven', never 'passed'", () => {
    const events = [
      offerEvent(1, [
        { name: 'tests-green', description: 'vitest suite passes' },
        { name: 'docs-updated', description: 'docs mention the change' },
      ]),
      completionEvent(2, {
        revision: 1,
        attempt: 1,
        summary: 'partial proof',
        evidence: [
          { checkName: 'tests-green', verdict: 'CHECK_VERDICT_PASSED', detail: 'green' },
        ],
      }),
    ]

    const statuses = checkStatuses(events)
    const docs = statuses.find((status) => status.name === 'docs-updated')
    expect(docs).toEqual({
      name: 'docs-updated',
      description: 'docs mention the change',
      status: 'unproven',
      detail: '',
    })
    expect(docs?.status).not.toBe('passed')
  })

  it('leaves every check unproven before any candidate exists', () => {
    const events = [offerEvent(1, [{ name: 'tests-green', description: 'vitest suite passes' }])]
    expect(checkStatuses(events).map((status) => status.status)).toEqual(['unproven'])
  })
})

describe('latestCandidate', () => {
  it('returns the newest completion by event order', () => {
    const events = [
      completionEvent(2, { revision: 1, attempt: 1, summary: 'first try' }),
      workerEvent(3, { progress: { message: 'still going' } }),
      completionEvent(5, { revision: 2, attempt: 2, summary: 'second try' }),
    ]

    expect(latestCandidate(events)).toEqual({
      revision: 2,
      attempt: 2,
      summary: 'second try',
      cursor: 5,
    })
  })

  it('returns null when the transcript has no completion', () => {
    const events = [
      offerEvent(1, [{ name: 'tests-green', description: 'vitest suite passes' }]),
      workerEvent(2, { progress: { message: 'working' } }),
    ]
    expect(latestCandidate(events)).toBeNull()
  })

  it('returns null for an empty transcript', () => {
    expect(latestCandidate([])).toBeNull()
  })
})

describe('frameKind', () => {
  it("recognizes a coordinator offer as 'offer'", () => {
    expect(frameKind(offerEvent(1, []))).toBe('offer')
  })

  it("recognizes a worker completion as 'completion'", () => {
    expect(frameKind(completionEvent(2, { revision: 1 }))).toBe('completion')
  })

  it("recognizes a coordinator revision request as 'revisionRequested'", () => {
    expect(frameKind(coordinatorEvent(3, { revisionRequested: { feedback: 'redo it' } }))).toBe(
      'revisionRequested',
    )
  })

  it("recognizes a task message as 'taskMessage'", () => {
    expect(frameKind(coordinatorEvent(4, { taskMessage: { text: 'hello' } }))).toBe('taskMessage')
  })

  it("answers 'frame' for an unrecognized arm", () => {
    expect(frameKind(coordinatorEvent(5, { somethingNew: { text: 'mystery' } }))).toBe('frame')
    expect(frameKind({ cursor: 6, workerId: 'w', taskId: 't', lane: 'LANE_WORKER', entry: {} }))
      .toBe('frame')
  })
})

describe('transcriptText', () => {
  const events = [
    offerEvent(1, [{ name: 'tests-green', description: 'vitest suite passes' }]),
    completionEvent(3, {
      revision: 1,
      attempt: 1,
      summary: 'did the work',
      evidence: [
        { checkName: 'tests-green', verdict: 'CHECK_VERDICT_PASSED', detail: 'all green' },
      ],
    }),
  ]

  it('includes cursor-numbered lines for every recorded frame', () => {
    const text = transcriptText(task, events)
    expect(text).toContain('[1] coordinator · offer')
    expect(text).toContain('[3] worker worker-1 · completion')
  })

  it('includes each frame text', () => {
    const text = transcriptText(task, events)
    expect(text).toContain(`  ${frameText(events[0])}`)
    expect(text).toContain('  did the work')
  })

  it("renders facts as '- ' lines", () => {
    const text = transcriptText(task, events)
    for (const event of events) {
      for (const fact of frameFacts(event)) {
        expect(text).toContain(`  - ${fact}`)
      }
    }
    expect(text).toContain('  - tests-green · CHECK_VERDICT_PASSED · all green')
  })

  it('carries the honesty sentence about being a projection with no provider reasoning', () => {
    const text = transcriptText(task, events)
    expect(text).toContain(
      'This is a projection of the recorded protocol frames, in cursor order.',
    )
    expect(text).toContain(
      'It carries no provider reasoning and no claim beyond what was recorded.',
    )
  })
})
