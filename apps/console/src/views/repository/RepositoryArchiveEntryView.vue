<template>
  <div>
    <v-card class="mb-4">
      <v-card-title class="text-subtitle-1 d-flex align-center flex-wrap ga-2">
        <v-btn icon="mdi-arrow-left" size="small" variant="text" aria-label="Archive entries"
               :to="{ name: 'repository-archive', query: { account: queryText('account'), archive: queryText('archive') } }" />
        <v-icon size="small">mdi-file-cabinet</v-icon>
        <span class="text-mono" data-testid="entry-id">{{ queryText('entry') }}</span>
        <v-chip v-if="pinned !== null" size="small" variant="tonal" class="text-mono" data-testid="version-chip">
          version {{ pinned }}{{ requested === null ? ' (current)' : '' }}
        </v-chip>
        <v-spacer />
        <v-btn v-if="requested !== null" size="small" variant="text" data-testid="open-current-version" @click="openVersion(null)">
          Open current version
        </v-btn>
      </v-card-title>
      <v-card-text v-if="info" class="text-body-2 pt-0" data-testid="entry-info">
        <div>{{ info.title || info.filename }}</div>
        <div class="text-caption text-medium-emphasis">
          {{ info.contentType }} · {{ classification }} · updated {{ instant(info.updatedAt) ?? 'not recorded' }}
          <span v-if="info.sourceUri"> · {{ info.sourceUri }}</span>
        </div>
      </v-card-text>
    </v-card>

    <v-alert v-if="routeError" type="warning" variant="tonal" density="compact" class="mb-3" data-testid="route-error">{{ routeError }}</v-alert>
    <RpcFailureAlert v-if="manifestFailure" :failure="manifestFailure" title="Reading the entry manifest" />

    <v-card v-if="manifest" class="mb-4">
      <v-card-title class="text-subtitle-2">Renditions</v-card-title>
      <v-card-text>
        <div class="text-caption text-medium-emphasis mb-2">
          {{ byteSize(manifest.totalBytes) }} in this version<span v-if="manifest.rootChecksum"> · root checksum
          <span class="text-mono">{{ manifest.rootChecksum }}</span></span>
        </div>
        <v-table v-if="renditions.length > 0" density="compact" class="mb-3" data-testid="rendition-table">
          <thead><tr><th>Rendition</th><th>Media type</th><th>State</th><th>Size</th><th>SHA-256</th><th>Written</th><th /></tr></thead>
          <tbody>
            <tr v-for="r in renditions" :key="r.key" :data-testid="`rendition-${r.label}`">
              <td class="text-mono">{{ r.label }}</td>
              <td class="text-mono text-caption">{{ r.mediaType }}</td>
              <td>
                {{ r.state }}
                <div v-if="r.deletedReason" class="text-caption text-medium-emphasis">{{ r.deletedReason }}</div>
              </td>
              <td class="text-mono">{{ byteSize(r.sizeBytes) }}</td>
              <td class="text-mono text-caption">{{ r.sha256 }}</td>
              <td class="text-caption">{{ r.writtenAt ?? '' }}<div v-if="r.writtenBy">{{ r.writtenBy }}</div></td>
              <td>
                <v-btn v-if="r.readable" size="x-small" variant="tonal" :data-testid="`select-rendition-${r.label}`" @click="selected = r">Select</v-btn>
              </td>
            </tr>
          </tbody>
        </v-table>
        <div v-else class="text-body-2 text-medium-emphasis">This version records no renditions.</div>
        <ByteReader
          v-if="selected"
          :key="selected.key + String(pinned)"
          :title="selected.label"
          :filename="filenameFor(selected)"
          :media-type="selected.mediaType || null"
          :read="readSelected"
        />
      </v-card-text>
    </v-card>

    <v-card v-if="manifest" class="mb-4" data-testid="versions">
      <v-card-title class="text-subtitle-2">Versions</v-card-title>
      <v-card-text>
        <RpcFailureAlert v-if="versionsFailure" :failure="versionsFailure" title="Listing versions" />
        <v-table v-if="versions.length > 0" density="compact">
          <thead><tr><th>Version</th><th>Created</th><th>Renditions</th><th>Bytes</th><th /></tr></thead>
          <tbody>
            <tr v-for="v in versions" :key="v.version.toString()" :data-testid="`version-${v.version}`">
              <td class="text-mono">{{ v.version }}</td>
              <td class="text-mono text-caption">{{ v.createdAt ?? '' }}</td>
              <td>{{ v.renditions }}</td>
              <td>{{ byteSize(v.totalBytes) }}</td>
              <td>
                <v-btn v-if="v.version !== pinned" size="x-small" variant="tonal" :data-testid="`open-version-${v.version}`"
                       @click="openVersion(v.version)">Open</v-btn>
                <span v-else class="text-caption text-medium-emphasis">shown</span>
              </td>
            </tr>
          </tbody>
        </v-table>
        <v-btn v-if="versionsNext" size="small" variant="text" class="mt-2" data-testid="versions-more" @click="loadVersions(false)">
          Older versions
        </v-btn>
      </v-card-text>
    </v-card>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, shallowRef, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { create } from '@bufbuild/protobuf'
import ByteReader from '@/components/repository/ByteReader.vue'
import RpcFailureAlert from '@/components/repository/RpcFailureAlert.vue'
import {
  GetEntryManifestRequestSchema,
  ListVersionsRequestSchema,
} from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_service_pb'
import type { EntryAddress, EntryInfo, VersionManifest } from '@/gen/ai/protomolt/proto/repo/archive/v1/entry_pb'
import {
  classificationLabel,
  entryAddress,
  parseVersion,
  readRendition,
  renditionRows,
  versionRow,
  type RenditionRow,
  type VersionRow,
} from '@/repository/archiveCore'
import type { ByteRead } from '@/repository/byteReadCore'
import { useRepositoryClients } from '@/repository/clients'
import { byteSize, instant } from '@/repository/formatCore'
import { rpcFailure, type RpcFailure } from '@/repository/rpcErrorCore'
import { useCallScope } from '@/repository/useCallScope'

const VERSIONS_PAGE_SIZE = 25

const clients = useRepositoryClients()
const route = useRoute()
const router = useRouter()
const readScope = useCallScope()
const versionsScope = useCallScope()

function queryText(name: string): string {
  const value = route.query[name]
  return typeof value === 'string' ? value : ''
}

const routeError = ref<string | null>(null)
const address = shallowRef<EntryAddress | null>(null)
const requested = ref<bigint | null>(null)
const pinned = ref<bigint | null>(null)
const info = shallowRef<EntryInfo | null>(null)
const manifest = shallowRef<VersionManifest | null>(null)
const manifestFailure = ref<RpcFailure | null>(null)
const selected = ref<RenditionRow | null>(null)
const versions = shallowRef<VersionRow[]>([])
const versionsNext = ref('')
const versionsFailure = ref<RpcFailure | null>(null)

const renditions = computed(() => (manifest.value ? renditionRows(manifest.value) : []))
const classification = computed(() => classificationLabel(info.value?.classification))

// The suggested save name: the rendition's name and sub-key ahead of the
// entry's filename; the save dialog can change it.
function filenameFor(r: RenditionRow): string {
  const base = info.value?.filename || queryText('entry')
  return `${r.name}${r.subKey ? `-${r.subKey}` : ''}-${base}`
}

const readSelected: ByteRead = async (range, sink, signal, onProgress, stopAfter) => {
  const r = selected.value
  if (r === null || address.value === null || pinned.value === null) throw new Error('No rendition is selected.')
  const result = await readRendition(
    clients.archives,
    { address: address.value, version: pinned.value, name: r.name, subKey: r.subKey, range },
    sink,
    signal,
    onProgress,
    stopAfter,
  )
  return { receivedBytes: result.receivedBytes, verification: result.verification, mediaType: result.header.rendition?.mediaType ?? '' }
}

async function loadVersions(first: boolean) {
  const target = address.value
  if (target === null) return
  const signal = versionsScope.next()
  versionsFailure.value = null
  try {
    const page = await clients.archives.listVersions(
      create(ListVersionsRequestSchema, { address: target, limit: VERSIONS_PAGE_SIZE, continuationToken: first ? '' : versionsNext.value }),
      { signal },
    )
    if (signal.aborted) return
    const rows = page.versions.map(versionRow)
    versions.value = first ? rows : [...versions.value, ...rows]
    versionsNext.value = page.nextContinuationToken
  } catch (error) {
    if (!signal.aborted) versionsFailure.value = rpcFailure(error)
  }
}

async function load() {
  const signal = readScope.next()
  versionsScope.next()
  routeError.value = null
  manifestFailure.value = null
  info.value = null
  manifest.value = null
  pinned.value = null
  selected.value = null
  versions.value = []
  versionsNext.value = ''
  address.value = null
  let target: EntryAddress
  try {
    target = entryAddress(queryText('account'), queryText('archive'), queryText('entry'))
    requested.value = parseVersion(queryText('version'))
  } catch (error) {
    routeError.value = (error as Error).message
    return
  }
  address.value = target
  try {
    const answer = await clients.archives.getEntryManifest(
      create(GetEntryManifestRequestSchema, { address: target, version: requested.value ?? 0n }),
      { signal },
    )
    if (signal.aborted) return
    if (answer.manifest === undefined) throw new Error('GetEntryManifest answered without a manifest.')
    if (requested.value !== null && answer.manifest.version !== requested.value) {
      throw new Error(`GetEntryManifest answered for version ${answer.manifest.version}; version ${requested.value} was requested.`)
    }
    info.value = answer.info ?? null
    manifest.value = answer.manifest
    // Rendition reads pin the version this manifest describes.
    pinned.value = answer.manifest.version
  } catch (error) {
    if (!signal.aborted) manifestFailure.value = rpcFailure(error)
    return
  }
  void loadVersions(true)
}

function openVersion(version: bigint | null) {
  const query = { ...route.query }
  if (version === null) delete query.version
  else query.version = version.toString()
  void router.push({ name: 'repository-archive-entry', query })
}

watch(() => route.fullPath, () => void load(), { immediate: true })
</script>
