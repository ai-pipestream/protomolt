<template>
  <div class="task-console py-4">
    <v-card v-if="initializing" class="mx-auto pa-8" max-width="520" rounded="lg">
      <v-progress-linear indeterminate color="primary" />
      <div class="text-medium-emphasis mt-4">Connecting to the coordinator…</div>
    </v-card>

    <v-card v-else-if="!authenticated" class="mx-auto pa-8" max-width="520" rounded="lg">
      <v-icon icon="mdi-account-network" color="primary" size="44" />
      <h1 class="text-h4 mt-4">Agent task console</h1>
      <p class="text-medium-emphasis mt-2 mb-6">
        Sign in to inspect durable work, follow worker updates, and guide active tasks.
      </p>
      <v-form @submit.prevent="login">
        <v-text-field
          v-model="loginToken"
          label="Task console token"
          type="password"
          autocomplete="current-password"
          :error-messages="loginError"
          autofocus
        />
        <v-btn type="submit" color="primary" block :loading="loggingIn">Connect</v-btn>
      </v-form>
    </v-card>

    <template v-else>
      <div class="d-flex flex-wrap align-center ga-3 mb-5">
        <div>
          <div class="text-overline text-primary">Coordinator workspace</div>
          <h1 class="text-h4">Agent tasks</h1>
          <div class="text-medium-emphasis">
            Durable task context and structured updates from every connected worker.
          </div>
        </div>
        <v-spacer />
        <v-btn icon="mdi-refresh" variant="text" aria-label="Refresh tasks" @click="refresh" />
        <v-btn prepend-icon="mdi-logout" variant="outlined" @click="logout">Sign out</v-btn>
      </div>

      <v-alert v-if="error" type="error" variant="tonal" closable class="mb-4" @click:close="error = ''">
        {{ error }}
      </v-alert>

      <div class="worker-strip mb-5">
        <v-card
          v-for="worker in workers"
          :key="worker.workerId"
          class="worker-card pa-4"
          variant="tonal"
          rounded="lg"
        >
          <div class="d-flex align-center ga-2">
            <span class="presence-dot" :class="{ online: worker.connected && worker.admitted }" />
            <strong>{{ worker.workerId }}</strong>
            <v-chip size="x-small" :color="worker.connected ? 'success' : 'warning'" variant="flat">
              {{ worker.connected ? 'online' : 'away' }}
            </v-chip>
          </div>
          <div class="text-caption text-medium-emphasis mt-2">
            {{ [worker.provider, worker.model].filter(Boolean).join(' · ') || 'deterministic worker' }}
          </div>
          <div class="d-flex flex-wrap ga-1 mt-2">
            <v-chip v-for="capability in worker.capabilities" :key="capability" size="x-small">
              {{ capability }}
            </v-chip>
          </div>
        </v-card>
        <v-card v-if="workers.length === 0" class="pa-4 text-medium-emphasis" variant="outlined">
          No workers are registered yet.
        </v-card>
      </div>

      <v-row>
        <v-col cols="12" md="4">
          <v-card rounded="lg" height="100%">
            <v-card-title class="d-flex align-center">
              Tasks
              <v-chip size="small" class="ml-2">{{ tasks.length }}</v-chip>
            </v-card-title>
            <v-divider />
            <v-list lines="three" class="task-list">
              <v-list-item
                v-for="task in tasks"
                :key="task.taskId"
                :active="selected?.taskId === task.taskId"
                color="primary"
                @click="selectTask(task)"
              >
                <template #prepend>
                  <v-avatar :color="phaseColor(task.phase)" size="32">
                    <v-icon :icon="phaseIcon(task.phase)" size="18" />
                  </v-avatar>
                </template>
                <v-list-item-title>{{ task.objective || task.taskId }}</v-list-item-title>
                <v-list-item-subtitle>
                  {{ task.workerId || 'unassigned' }} · {{ task.phase }} · attempt {{ task.attempt }}
                </v-list-item-subtitle>
                <v-list-item-subtitle class="text-mono">{{ shortId(task.taskId) }}</v-list-item-subtitle>
              </v-list-item>
              <v-list-item v-if="tasks.length === 0" title="No durable tasks yet" subtitle="New offers will appear here." />
            </v-list>
          </v-card>
        </v-col>

        <v-col cols="12" md="8">
          <v-card v-if="!selected" rounded="lg" class="pa-8 text-center text-medium-emphasis">
            <v-icon icon="mdi-timeline-clock-outline" size="48" class="mb-3" />
            <div>Select a task to follow its durable timeline.</div>
          </v-card>

          <v-card v-else rounded="lg">
            <v-card-title class="d-flex flex-wrap align-center ga-2">
              <span class="text-truncate">{{ selected.objective }}</span>
              <v-chip :color="phaseColor(selected.phase)" size="small">{{ selected.phase }}</v-chip>
              <v-spacer />
              <v-chip size="small" variant="outlined">cursor {{ cursor }}</v-chip>
            </v-card-title>
            <v-card-subtitle class="pb-3">
              {{ selected.workerId }} · attempt {{ selected.attempt }} · checkpoint {{ selected.lastCheckpointSeq }}
            </v-card-subtitle>
            <v-divider />

            <div class="timeline pa-5">
              <v-alert
                v-for="finding in findings"
                :key="`${finding.frameId}:${finding.kind}`"
                type="warning"
                variant="tonal"
                density="compact"
                class="mb-3"
              >
                {{ finding.kind }}: {{ finding.error }}
              </v-alert>
              <div v-for="event in events" :key="event.cursor" class="timeline-row">
                <div class="timeline-rail">
                  <span class="timeline-dot" :class="event.lane === 'LANE_WORKER' ? 'worker' : 'coordinator'" />
                </div>
                <div class="timeline-content pb-5">
                  <div class="d-flex flex-wrap align-center ga-2">
                    <v-chip size="x-small" variant="tonal">{{ eventTitle(event) }}</v-chip>
                    <strong class="text-body-2">{{ eventActor(event) }}</strong>
                    <span class="text-caption text-medium-emphasis">cursor {{ event.cursor }}</span>
                  </div>
                  <div class="mt-2 text-body-2 preserve-lines">{{ eventText(event) }}</div>
                  <div v-if="eventFacts(event).length" class="mt-2 d-flex flex-column ga-1">
                    <div
                      v-for="fact in eventFacts(event)"
                      :key="fact"
                      class="event-fact text-caption text-mono"
                    >
                      {{ fact }}
                    </div>
                  </div>
                </div>
              </div>
              <div v-if="events.length === 0" class="text-medium-emphasis">No task frames recorded.</div>
            </div>

            <v-divider />
            <v-form class="pa-4" @submit.prevent="sendGuidance">
              <div class="d-flex flex-wrap ga-3">
                <v-select
                  v-model="messageKind"
                  :items="messageKinds"
                  label="Message kind"
                  density="compact"
                  style="max-width: 180px"
                />
                <v-select
                  v-model="recipient"
                  :items="workers.map((worker) => worker.workerId)"
                  label="Recipient"
                  density="compact"
                  style="min-width: 220px"
                />
              </div>
              <v-textarea
                v-model="messageText"
                label="Guide the worker"
                rows="2"
                auto-grow
                counter="16384"
                :maxlength="16384"
              />
              <div class="d-flex justify-end">
                <v-btn
                  type="submit"
                  color="primary"
                  prepend-icon="mdi-send"
                  :loading="sending"
                  :disabled="!recipient || !messageText.trim()"
                >
                  Send durable message
                </v-btn>
              </div>
            </v-form>
          </v-card>
        </v-col>
      </v-row>
    </template>
  </div>
</template>

<script setup lang="ts">
import { onBeforeUnmount, onMounted, ref } from 'vue'
import {
  taskApi,
  TaskApiError,
  type TaskEvent,
  type TaskFinding,
  type TaskMessageKind,
  type TaskSummary,
  type WorkerSummary,
} from '../services/tasks'

const initializing = ref(true)
const authenticated = ref(false)
const loginToken = ref('')
const loginError = ref('')
const loggingIn = ref(false)
const error = ref('')
const workers = ref<WorkerSummary[]>([])
const tasks = ref<TaskSummary[]>([])
const selected = ref<TaskSummary | null>(null)
const events = ref<TaskEvent[]>([])
const findings = ref<TaskFinding[]>([])
const cursor = ref(0)
const recipient = ref('')
const messageKind = ref<TaskMessageKind>('guidance')
const messageText = ref('')
const sending = ref(false)
const messageKinds: TaskMessageKind[] = ['guidance', 'question', 'answer', 'note']
let watchController: AbortController | null = null

onMounted(async () => {
  try {
    const status = await taskApi.sessionStatus()
    authenticated.value = status.authenticated
    if (authenticated.value) await refresh()
  } catch (failure) {
    error.value = message(failure)
  } finally {
    initializing.value = false
  }
})

onBeforeUnmount(() => watchController?.abort())

async function login() {
  loginError.value = ''
  loggingIn.value = true
  try {
    const status = await taskApi.login(loginToken.value)
    authenticated.value = status.authenticated
    loginToken.value = ''
    await refresh()
  } catch (failure) {
    loginError.value = message(failure)
  } finally {
    loggingIn.value = false
  }
}

async function logout() {
  watchController?.abort()
  await taskApi.logout()
  authenticated.value = false
  workers.value = []
  tasks.value = []
  selected.value = null
  events.value = []
  findings.value = []
}

async function refresh() {
  try {
    const [taskList, workerList] = await Promise.all([taskApi.listTasks(), taskApi.listWorkers()])
    tasks.value = taskList.tasks.sort((left, right) => right.lastCursor - left.lastCursor)
    workers.value = workerList
    if (selected.value) {
      const updated = tasks.value.find((task) => task.taskId === selected.value?.taskId)
      if (updated) selected.value = updated
    } else if (tasks.value[0]) {
      await selectTask(tasks.value[0])
    }
  } catch (failure) {
    if (failure instanceof TaskApiError && failure.status === 401) authenticated.value = false
    else error.value = message(failure)
  }
}

async function selectTask(task: TaskSummary) {
  watchController?.abort()
  selected.value = task
  recipient.value = task.workerId
  events.value = []
  findings.value = []
  cursor.value = 0
  try {
    const detail = await taskApi.task(task.taskId)
    selected.value = detail.task
    events.value = uniqueEvents(detail.events)
    findings.value = detail.findings
    cursor.value = Math.max(0, ...events.value.map((event) => event.cursor))
    watchTask(task.taskId)
  } catch (failure) {
    error.value = message(failure)
  }
}

async function watchTask(taskId: string) {
  const controller = new AbortController()
  watchController = controller
  while (!controller.signal.aborted && selected.value?.taskId === taskId) {
    try {
      const update = await taskApi.watchEvents(cursor.value, taskId, 25_000, 128, controller.signal)
      events.value = uniqueEvents([...events.value, ...update.events])
      cursor.value = Math.max(cursor.value, update.cursor)
      if (update.events.length) await refreshSummaries()
    } catch (failure) {
      if (controller.signal.aborted) return
      if (failure instanceof TaskApiError && failure.status === 401) {
        authenticated.value = false
        return
      }
      error.value = message(failure)
      return
    }
  }
}

async function refreshSummaries() {
  const list = await taskApi.listTasks()
  tasks.value = list.tasks.sort((left, right) => right.lastCursor - left.lastCursor)
  const updated = tasks.value.find((task) => task.taskId === selected.value?.taskId)
  if (updated) selected.value = updated
}

async function sendGuidance() {
  if (!selected.value || !recipient.value || !messageText.value.trim()) return
  sending.value = true
  try {
    await taskApi.sendMessage(
      selected.value.taskId,
      recipient.value,
      messageKind.value,
      messageText.value.trim(),
    )
    messageText.value = ''
  } catch (failure) {
    error.value = message(failure)
  } finally {
    sending.value = false
  }
}

function uniqueEvents(input: TaskEvent[]): TaskEvent[] {
  return [...new Map(input.map((event) => [event.cursor, event])).values()].sort(
    (left, right) => left.cursor - right.cursor,
  )
}

function frame(event: TaskEvent): Record<string, any> {
  return (event.entry.coordinatorFrame ?? event.entry.workerFrame ?? {}) as Record<string, any>
}

function eventKind(event: TaskEvent): string {
  const value = frame(event)
  return (
    [
      'hello',
      'admission',
      'offer',
      'accept',
      'reject',
      'progress',
      'checkpoint',
      'renewal',
      'completion',
      'revision',
      'acceptance',
      'blocked',
      'failure',
      'cancellation',
      'taskMessage',
    ].find((key) => value[key] !== undefined) ?? 'frame'
  )
}

function eventTitle(event: TaskEvent): string {
  return eventKind(event).replace(/([A-Z])/g, ' $1').toLowerCase()
}

function eventActor(event: TaskEvent): string {
  return event.lane === 'LANE_WORKER' ? event.workerId : 'coordinator'
}

function eventText(event: TaskEvent): string {
  const value = frame(event)[eventKind(event)] as Record<string, any> | undefined
  if (!value) return 'Recorded protocol frame'
  return (
    value.text ??
    value.message ??
    value.summary ??
    value.reason ??
    value.feedback ??
    value.note ??
    value.spec?.objective ??
    value.resumeToken ??
    'Recorded protocol frame'
  )
}

function eventFacts(event: TaskEvent): string[] {
  const value = frame(event)[eventKind(event)] as Record<string, any> | undefined
  if (!value) return []
  const facts: string[] = []
  for (const check of value.evidence ?? value.spec?.requiredChecks ?? []) {
    facts.push(
      [check.checkName ?? check.name, check.verdict, check.detail ?? check.description]
        .filter(Boolean)
        .join(' · '),
    )
  }
  for (const commit of value.commits ?? []) {
    facts.push(
      [commit.repository, commit.commit?.slice(0, 12), commit.subject].filter(Boolean).join(' · '),
    )
  }
  for (const artifact of value.artifacts ?? []) {
    facts.push(`artifact · ${artifact.uri ?? artifact.digest ?? artifact.objectKey ?? 'recorded'}`)
  }
  if (value.state) {
    facts.push(`checkpoint state · ${value.state.uri ?? value.state.digest ?? 'recorded'}`)
  }
  return facts.filter(Boolean)
}

function phaseColor(phase: string): string {
  if (phase === 'accepted') return 'success'
  if (['failed', 'blocked', 'cancelled', 'expired'].includes(phase)) return 'error'
  if (phase === 'candidate') return 'secondary'
  return 'primary'
}

function phaseIcon(phase: string): string {
  if (phase === 'accepted') return 'mdi-check'
  if (['failed', 'blocked', 'cancelled', 'expired'].includes(phase)) return 'mdi-alert'
  if (phase === 'candidate') return 'mdi-file-check-outline'
  return 'mdi-progress-clock'
}

function shortId(id: string): string {
  return id.length > 12 ? `${id.slice(0, 8)}…` : id
}

function message(failure: unknown): string {
  return (failure as { message?: string })?.message ?? String(failure)
}
</script>

<style scoped>
.worker-strip {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: 12px;
}

.worker-card {
  min-height: 112px;
}

.presence-dot {
  width: 9px;
  height: 9px;
  border-radius: 50%;
  background: rgb(var(--v-theme-warning));
  box-shadow: 0 0 0 4px rgba(var(--v-theme-warning), 0.14);
}

.presence-dot.online {
  background: rgb(var(--v-theme-success));
  box-shadow: 0 0 0 4px rgba(var(--v-theme-success), 0.14);
}

.task-list {
  max-height: 670px;
  overflow-y: auto;
}

.timeline {
  max-height: 540px;
  overflow-y: auto;
}

.timeline-row {
  display: grid;
  grid-template-columns: 26px minmax(0, 1fr);
}

.timeline-rail {
  position: relative;
}

.timeline-rail::after {
  position: absolute;
  top: 15px;
  bottom: -4px;
  left: 6px;
  width: 1px;
  content: '';
  background: rgba(var(--v-theme-on-surface), 0.16);
}

.timeline-row:last-child .timeline-rail::after {
  display: none;
}

.timeline-dot {
  position: absolute;
  top: 5px;
  left: 2px;
  width: 9px;
  height: 9px;
  border-radius: 50%;
  background: rgb(var(--v-theme-secondary));
}

.timeline-dot.worker {
  background: rgb(var(--v-theme-primary));
}

.preserve-lines {
  white-space: pre-wrap;
  overflow-wrap: anywhere;
}

.event-fact {
  padding: 6px 8px;
  border-radius: 6px;
  background: rgba(var(--v-theme-on-surface), 0.05);
  overflow-wrap: anywhere;
}
</style>
